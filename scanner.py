import os
import re
import json
import time
import math
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import requests
import pandas as pd
import yfinance as yf

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT")

# ====== KAS v1 (agresyvesnis mokymuisi, bet su digest) ======
MIN_USD = 150_000
MAX_ITEMS = 8

# Struktūra (agresyvus režimas)
RANGE_PCTL_MAX = 0.65
DRAWDOWN_3M_MIN = 0.15

# Rating booster kol kas NE (SEC-only fazėje) — paliekam v2, kad nepridėt triukšmo.
# (Jei reikės, vėliau pridėsim kaip boosterį tik insider kandidatams.)

# SEC: current filings Atom for Form 4
SEC_ATOM_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom"

@dataclass
class BuyTx:
    ticker: str
    filing_time_utc: dt.datetime
    cik: str
    accession: str
    insider_title: str
    is_director: bool
    is_officer: bool
    is_ten_percent: bool
    shares: float
    price: float
    value_usd: float
    sec_doc_url: str  # filing folder (Archives)
    sec_xml_url: str  # actual form4 xml

@dataclass
class Enriched:
    tx: BuyTx
    range_pctl_52w: Optional[float]
    drawdown_3m: Optional[float]
    day_pct: Optional[float]
    vol_ratio: Optional[float]
    score: float

def http_get(url: str, timeout: int = 45) -> requests.Response:
    headers = {"User-Agent": SEC_USER_AGENT or "KASInsiderRadar/1.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r

def send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()

def parse_atom_entries(atom_xml: str) -> List[dict]:
    # Minimal atom parse via pandas read_xml is flaky; do simple regex-based split + light parsing.
    # Works fine for SEC atom.
    entries = atom_xml.split("<entry>")
    out = []
    for chunk in entries[1:]:
        def tag_value(tag: str) -> Optional[str]:
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", chunk, re.DOTALL)
            return m.group(1).strip() if m else None

        title = tag_value("title")
        updated = tag_value("updated")
        link = None
        m = re.search(r'<link[^>]+href="([^"]+)"', chunk)
        if m:
            link = m.group(1)

        # We prefer the Archives link inside <link> sometimes points to sec.gov/cgi-bin...
        out.append({"title": title, "updated": updated, "link": link, "raw": chunk})
    return out

def extract_cik_accession(entry: dict) -> Optional[Tuple[str, str, str]]:
    """
    Returns (cik, accession_nodashes, accession_with_dashes)
    We try to find an Archives URL with /data/{cik}/{accession_nodashes}/...
    """
    raw = entry.get("raw", "") or ""
    # find any Archives link in the raw entry
    m = re.search(r"(https://www\.sec\.gov/Archives/edgar/data/(\d+)/(\d{18})/)", raw)
    if m:
        folder = m.group(1)
        cik = m.group(2).lstrip("0") or m.group(2)
        acc_nodash = m.group(3)
        # reconstruct dashed accession (10-2-6)
        acc_dash = f"{acc_nodash[0:10]}-{acc_nodash[10:12]}-{acc_nodash[12:18]}"
        return cik, acc_nodash, acc_dash

    # fallback: sometimes we only have the filing index page link; try to pull accession from id tag
    filing_id = None
    mm = re.search(r"<id>(.*?)</id>", raw)
    if mm:
        filing_id = mm.group(1)
    if filing_id:
        mm2 = re.search(r"accession_number=([\d-]+)", filing_id)
        mm3 = re.search(r"(\d{10}-\d{2}-\d{6})", filing_id)
        if mm2 or mm3:
            acc_dash = (mm2.group(1) if mm2 else mm3.group(1))
            acc_nodash = acc_dash.replace("-", "")
            # cik might appear in title like "4 - ... (0000xxxxx)"
            mm4 = re.search(r"\((\d{10})\)", entry.get("title") or "")
            cik = (mm4.group(1).lstrip("0") if mm4 else "")
            if cik:
                return cik, acc_nodash, acc_dash
    return None

def pick_form4_xml_from_index(cik: str, acc_nodash: str) -> Optional[str]:
    """
    Uses SEC folder index.json to find likely Form4 XML.
    """
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/index.json"
    try:
        r = http_get(index_url)
        data = r.json()
        items = data.get("directory", {}).get("item", [])
        # prefer *.xml not xsl
        xmls = [it["name"] for it in items if it.get("name", "").lower().endswith(".xml")]
        if not xmls:
            return None
        # Heuristics: choose the smallest "primary" form4 xml (often endswith .xml and contains 'form4' or 'xslF345')
        priority = []
        for name in xmls:
            ln = name.lower()
            score = 0
            if "form4" in ln:
                score += 5
            if "f345" in ln:
                score += 3
            if ln.startswith("primary"):
                score += 2
            priority.append((score, name))
        priority.sort(reverse=True)
        best = priority[0][1]
        return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{best}"
    except Exception:
        return None

def parse_form4_xml(xml_text: str) -> List[dict]:
    """
    Parses Form4 XML into list of non-derivative transactions dicts.
    """
    # Use pandas.read_xml for simple extraction is not reliable across namespaces; do regex-ish parsing blocks.
    # We'll extract nonDerivativeTransaction blocks.
    txs = []
    blocks = re.findall(r"<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>", xml_text, re.DOTALL)
    for b in blocks:
        def get(path_tag: str) -> Optional[str]:
            m = re.search(rf"<{path_tag}[^>]*>(.*?)</{path_tag}>", b, re.DOTALL)
            return m.group(1).strip() if m else None

        code = get("transactionCode")
        acq_disp = get("transactionAcquiredDisposedCode")
        shares = get("transactionShares")
        price = get("transactionPricePerShare")
        # These tags often wrap <value>...</value>
        def inner_value(tag: str) -> Optional[str]:
            m = re.search(rf"<{tag}[^>]*>.*?<value>(.*?)</value>.*?</{tag}>", b, re.DOTALL)
            return m.group(1).strip() if m else None

        code = inner_value("transactionCode") or code
        acq_disp = inner_value("transactionAcquiredDisposedCode") or acq_disp
        shares_v = inner_value("transactionShares")
        price_v = inner_value("transactionPricePerShare")

        txs.append({
            "code": code,
            "acq_disp": acq_disp,
            "shares": shares_v,
            "price": price_v,
        })
    return txs

def parse_header_fields(xml_text: str) -> dict:
    def val(tag: str) -> Optional[str]:
        m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", xml_text, re.DOTALL)
        return m.group(1).strip() if m else None

    ticker = val("issuerTradingSymbol")
    officer_title = val("officerTitle") or ""
    is_director = (val("isDirector") or "").lower() == "true"
    is_officer = (val("isOfficer") or "").lower() == "true"
    is_ten = (val("isTenPercentOwner") or "").lower() == "true"

    return {
        "ticker": (ticker or "").strip().upper(),
        "officer_title": officer_title.strip(),
        "is_director": is_director,
        "is_officer": is_officer,
        "is_ten": is_ten,
    }

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def yf_enrich(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    range_pctl_52w, drawdown_3m, day_pct, vol_ratio
    """
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1y", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            return None, None, None, None

        closes = hist["Close"].dropna()
        vols = hist["Volume"].dropna()

        last_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else last_close
        day_pct = (last_close / prev_close - 1.0) if prev_close else None

        low_52 = float(closes.min())
        high_52 = float(closes.max())
        range_pctl = ((last_close - low_52) / (high_52 - low_52)) if high_52 > low_52 else None

        tail_90 = closes.tail(90)
        drawdown_3m = None
        if len(tail_90) >= 20:
            high_90 = float(tail_90.max())
            drawdown_3m = (1.0 - (last_close / high_90)) if high_90 else None

        tail_vol_20 = vols.tail(20)
        vol_ratio = None
        if len(tail_vol_20) >= 10:
            avg20 = float(tail_vol_20.mean())
            vol_ratio = (float(vols.iloc[-1]) / avg20) if avg20 else None

        return range_pctl, drawdown_3m, day_pct, vol_ratio
    except Exception:
        return None, None, None, None

def passes_structure(range_pctl: Optional[float], drawdown_3m: Optional[float]) -> bool:
    ok_range = (range_pctl is not None and range_pctl <= RANGE_PCTL_MAX)
    ok_dd = (drawdown_3m is not None and drawdown_3m >= DRAWDOWN_3M_MIN)
    return ok_range or ok_dd

def weight_role(officer_title: str, is_director: bool, is_officer: bool, is_ten: bool) -> float:
    t = (officer_title or "").lower()
    w = 1.0
    if any(k in t for k in ["ceo", "chief executive", "cfo", "chief financial", "president"]):
        w += 1.2
    elif is_director:
        w += 0.8
    elif is_officer:
        w += 0.3
    if is_ten:
        w += 0.2
    return w

def score_tx(value_usd: float, role_w: float, range_pctl: Optional[float], drawdown_3m: Optional[float]) -> float:
    sc = role_w
    if value_usd >= 300_000:
        sc += 0.8
    if value_usd >= 500_000:
        sc += 0.6

    if range_pctl is not None:
        if range_pctl <= 0.50:
            sc += 1.6
        elif range_pctl <= 0.65:
            sc += 1.0
    if drawdown_3m is not None and drawdown_3m >= DRAWDOWN_3M_MIN:
        sc += 0.8
    return sc

def fetch_sec_buys_last_24h() -> List[BuyTx]:
    atom = http_get(SEC_ATOM_URL).text
    entries = parse_atom_entries(atom)

    now = dt.datetime.utcnow()
    cutoff = now - dt.timedelta(hours=26)  # little buffer

    out: List[BuyTx] = []
    for e in entries:
        upd = e.get("updated")
        if not upd:
            continue
        try:
            # Atom updated format: 2026-02-15T08:34:12-05:00 etc. We'll normalize by parsing offset-ish.
            # Quick parse: take first 19 chars as naive, ignore tz (good enough for cutoff buffer)
            naive = upd[:19]
            filing_time = dt.datetime.strptime(naive, "%Y-%m-%dT%H:%M:%S")
            filing_time = filing_time  # treat as approx UTC
        except Exception:
            continue
        if filing_time < cutoff:
            continue

        info = extract_cik_accession(e)
        if not info:
            continue
        cik, acc_nodash, _acc_dash = info

        folder_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/"
        xml_url = pick_form4_xml_from_index(cik, acc_nodash)
        if not xml_url:
            continue

        try:
            xml_text = http_get(xml_url).text
        except Exception:
            continue

        header = parse_header_fields(xml_text)
        ticker = header.get("ticker", "")
        if not ticker:
            continue

        txs = parse_form4_xml(xml_text)
        for tx in txs:
            code = (tx.get("code") or "").strip().upper()
            acq = (tx.get("acq_disp") or "").strip().upper()
            if code != "P":
                continue
            if acq and acq != "A":
                continue

            shares = to_float(tx.get("shares"))
            price = to_float(tx.get("price"))
            if shares is None or price is None:
                continue

            value_usd = float(shares) * float(price)
            if value_usd < MIN_USD:
                continue

            out.append(
                BuyTx(
                    ticker=ticker,
                    filing_time_utc=filing_time,
                    cik=cik,
                    accession=acc_nodash,
                    insider_title=header.get("officer_title", "") or ("Director" if header.get("is_director") else ""),
                    is_director=bool(header.get("is_director")),
                    is_officer=bool(header.get("is_officer")),
                    is_ten_percent=bool(header.get("is_ten")),
                    shares=float(shares),
                    price=float(price),
                    value_usd=value_usd,
                    sec_doc_url=folder_url,
                    sec_xml_url=xml_url,
                )
            )

    return out

def build_digest(items: List[Enriched]) -> str:
    if not items:
        return "KAS RADAR 06:00\n\n0 signalų šiandien."

    items = sorted(items, key=lambda x: x.score, reverse=True)[:MAX_ITEMS]

    lines = ["KAS RADAR 06:00", ""]
    for i, it in enumerate(items, start=1):
        tx = it.tx
        rp = f"{it.range_pctl_52w*100:.0f}%" if it.range_pctl_52w is not None else "n/a"
        dd = f"{it.drawdown_3m*100:.0f}%" if it.drawdown_3m is not None else "n/a"
        day = f"{it.day_pct*100:.1f}%" if it.day_pct is not None else "n/a"
        vr = f"{it.vol_ratio:.1f}x" if it.vol_ratio is not None else "n/a"

        role = tx.insider_title if tx.insider_title else ("Director" if tx.is_director else "Officer")
        lines.append(f"{i}) {tx.ticker} — Score {it.score:.1f}")
        lines.append(f"   Insider: {role} | ${tx.value_usd:,.0f} (P)")
        lines.append(f"   Struktūra: Range {rp} | 3m DD {dd} | Reakcija {day} | Vol {vr}")
        lines.append(f"   SEC: {tx.sec_xml_url}")
        lines.append("")
    return "\n".join(lines).strip()

def main():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and SEC_USER_AGENT):
        raise SystemExit("Trūksta TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / SEC_USER_AGENT")

    txs = fetch_sec_buys_last_24h()

    enriched: List[Enriched] = []
    for tx in txs:
        range_pctl, drawdown_3m, day_pct, vol_ratio = yf_enrich(tx.ticker)

        if not passes_structure(range_pctl, drawdown_3m):
            continue

        role_w = weight_role(tx.insider_title, tx.is_director, tx.is_officer, tx.is_ten_percent)
        sc = score_tx(tx.value_usd, role_w, range_pctl, drawdown_3m)

        enriched.append(
            Enriched(
                tx=tx,
                range_pctl_52w=range_pctl,
                drawdown_3m=drawdown_3m,
                day_pct=day_pct,
                vol_ratio=vol_ratio,
                score=sc,
            )
        )

    msg = build_digest(enriched)
    # Jei nenori net "0 signalų", gali pakeisti: jei 0 -> nieko nesiųsti
    send_telegram(msg)

if __name__ == "__main__":
    main()
