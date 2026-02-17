import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


SEC_DATA_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{accession_nodashes}/{primary_doc}"
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"

DB_PATH = "insider_db.json"


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if v else default


def load_db() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {"seen": {}}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "seen" not in data or not isinstance(data["seen"], dict):
            return {"seen": {}}
        return data
    except Exception:
        return {"seen": {}}


def save_db(db: Dict[str, Any]) -> None:
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2, sort_keys=True)


def requests_session(sec_user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": sec_user_agent,
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


def get_company_tickers(session: requests.Session) -> Dict[str, Dict[str, Any]]:
    r = session.get(SEC_DATA_TICKERS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    out: Dict[str, Dict[str, Any]] = {}
    for _, row in data.items():
        try:
            ticker = str(row.get("ticker", "")).upper().strip()
            cik = int(row.get("cik_str"))
            title = str(row.get("title", "")).strip()
            if ticker and cik:
                out[ticker] = {"cik": cik, "title": title}
        except Exception:
            continue
    return out


def cik_to_10(cik: int) -> str:
    return str(cik).zfill(10)


def cik_to_nolead(cik: int) -> str:
    return str(int(cik))


def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def fetch_submissions(session: requests.Session, cik: int) -> Dict[str, Any]:
    url = SEC_SUBMISSIONS_URL.format(cik10=cik_to_10(cik))
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_recent_form4(submissions: Dict[str, Any], max_items: int) -> List[Dict[str, Any]]:
    recent = safe_get(submissions, ["filings", "recent"], {}) or {}
    forms = recent.get("form", []) or []
    accession = recent.get("accessionNumber", []) or []
    primary_doc = recent.get("primaryDocument", []) or []
    filing_date = recent.get("filingDate", []) or []

    out: List[Dict[str, Any]] = []
    for i in range(min(len(forms), len(accession), len(primary_doc), len(filing_date), max_items)):
        if str(forms[i]).strip().upper() != "4":
            continue
        out.append(
            {
                "accession": str(accession[i]).strip(),
                "primary_doc": str(primary_doc[i]).strip(),
                "filing_date": str(filing_date[i]).strip(),
            }
        )
    return out


def fetch_form4_primary(session: requests.Session, cik: int, accession: str, primary_doc: str) -> str:
    url = SEC_ARCHIVES_BASE.format(
        cik_nolead=cik_to_nolead(cik),
        accession_nodashes=accession.replace("-", ""),
        primary_doc=primary_doc,
    )
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def extract_transactions_minimal(xml_text: str) -> List[Dict[str, Any]]:
    """
    Minimalus Form 4 XML parsas be sunkių bibliotekų.
    Ieškom nonDerivativeTransaction blokų ir pasiimam:
    ticker (issuerTradingSymbol), transactionCode, shares, price, date, ownerName.
    Jei kažko nėra, paliekam None.
    """
    def find_between(s: str, a: str, b: str) -> Optional[str]:
        i = s.find(a)
        if i == -1:
            return None
        i += len(a)
        j = s.find(b, i)
        if j == -1:
            return None
        return s[i:j].strip()

    def all_blocks(s: str, start_tag: str, end_tag: str) -> List[str]:
        blocks = []
        idx = 0
        while True:
            i = s.find(start_tag, idx)
            if i == -1:
                break
            j = s.find(end_tag, i)
            if j == -1:
                break
            blocks.append(s[i : j + len(end_tag)])
            idx = j + len(end_tag)
        return blocks

    xml = xml_text

    issuer_symbol = find_between(xml, "<issuerTradingSymbol>", "</issuerTradingSymbol>")
    issuer_name = find_between(xml, "<issuerName>", "</issuerName>")
    reporting_owner = find_between(xml, "<rptOwnerName>", "</rptOwnerName>")

    tx_blocks = all_blocks(xml, "<nonDerivativeTransaction>", "</nonDerivativeTransaction>")
    out: List[Dict[str, Any]] = []

    for blk in tx_blocks:
        code = find_between(blk, "<transactionCode>", "</transactionCode>")
        shares = find_between(blk, "<transactionShares>", "</transactionShares>")
        shares_val = None
        if shares:
            shares_val = find_between(shares, "<value>", "</value>")
        price = find_between(blk, "<transactionPricePerShare>", "</transactionPricePerShare>")
        price_val = None
        if price:
            price_val = find_between(price, "<value>", "</value>")
        date = find_between(blk, "<transactionDate>", "</transactionDate>")
        date_val = None
        if date:
            date_val = find_between(date, "<value>", "</value>")

        out.append(
            {
                "ticker": (issuer_symbol or "").upper().strip(),
                "issuer": (issuer_name or "").strip(),
                "owner": (reporting_owner or "").strip(),
                "code": (code or "").strip(),
                "shares": shares_val,
                "price": price_val,
                "date": date_val,
            }
        )

    return out


def to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def send_telegram(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        raise RuntimeError("Trūksta TELEGRAM_BOT_TOKEN arba TELEGRAM_CHAT_ID")
    url = TELEGRAM_SEND_URL.format(token=token)
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()


def main() -> None:
    telegram_token = env_str("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = env_str("TELEGRAM_CHAT_ID")
    sec_user_agent = env_str("SEC_USER_AGENT")

    if not sec_user_agent or "@" not in sec_user_agent:
        raise RuntimeError("SEC_USER_AGENT turi būti kaip: Vardas Pavarde email@domenas")

    watchlist_raw = env_str("WATCHLIST", "")
    watchlist = [t.strip().upper() for t in watchlist_raw.split(",") if t.strip()]
    if not watchlist:
        watchlist = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]

    max_form4_per_ticker = env_int("MAX_FORM4_PER_TICKER", 6)
    max_total_candidates = env_int("MAX_TOTAL_CANDIDATES", 25)

    min_value_usd = env_float("MIN_VALUE_USD", 25000.0)
    allow_only_code_p = env_str("ONLY_PURCHASES", "1").strip() != "0"

    db = load_db()
    seen: Dict[str, Any] = db.get("seen", {})

    session = requests_session(sec_user_agent)

    tickers_map = get_company_tickers(session)

    found_alerts: List[str] = []
    total_checked = 0

    for ticker in watchlist:
        meta = tickers_map.get(ticker)
        if not meta:
            continue

        cik = int(meta["cik"])
        total_checked += 1

        try:
            subs = fetch_submissions(session, cik)
            recent_form4 = parse_recent_form4(subs, max_form4_per_ticker)
        except Exception as e:
            print(f"SEC klaida ticker {ticker}: {e}")
            continue

        for item in recent_form4:
            accession = item["accession"]
            filing_date = item["filing_date"]
            primary_doc = item["primary_doc"]

            unique_key = f"{ticker}:{accession}:{primary_doc}"
            if unique_key in seen:
                continue

            try:
                xml_text = fetch_form4_primary(session, cik, accession, primary_doc)
                txs = extract_transactions_minimal(xml_text)
            except Exception as e:
                print(f"Form4 parse klaida {ticker} {accession}: {e}")
                continue

            matched_any = False

            for tx in txs:
                code = (tx.get("code") or "").upper().strip()
                if allow_only_code_p and code != "P":
                    continue

                shares = to_float(tx.get("shares"))
                price = to_float(tx.get("price"))
                if shares is None or price is None:
                    continue

                value = shares * price
                if value < min_value_usd:
                    continue

                owner = tx.get("owner") or ""
                issuer = tx.get("issuer") or ""
                tx_date = tx.get("date") or filing_date

                line = (
                    f"Insider radaras\n"
                    f"Ticker {ticker}\n"
                    f"Issuer {issuer}\n"
                    f"Owner {owner}\n"
                    f"Veiksmas Pirkimas\n"
                    f"Data {tx_date}\n"
                    f"Kiekis {shares:.0f}\n"
                    f"Kaina {price:.2f}\n"
                    f"Vertė {value:,.0f} USD\n"
                    f"Šaltinis SEC Form 4 {filing_date}"
                )
                found_alerts.append(line)
                matched_any = True

                if len(found_alerts) >= max_total_candidates:
                    break

            seen[unique_key] = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "filing_date": filing_date,
                "matched": matched_any,
            }

            if len(found_alerts) >= max_total_candidates:
                break

        if len(found_alerts) >= max_total_candidates:
            break

        time.sleep(0.2)

    db["seen"] = seen
    save_db(db)

    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    if found_alerts:
        text = f"Radaras patikrintas {now_local}\nRasta {len(found_alerts)} kandidatai\n\n" + "\n\n".join(found_alerts)
    else:
        text = f"Radaras patikrintas {now_local}\nNaujų insider pirkimų pagal filtrą nerasta"

    send_telegram(telegram_token, telegram_chat_id, text)

    print(f"Watchlist: {watchlist}")
    print(f"Checked tickers: {total_checked}")
    print(f"Alerts: {len(found_alerts)}")
    print(f"MIN_VALUE_USD: {min_value_usd}")
    print(f"ONLY_PURCHASES: {allow_only_code_p}")


if __name__ == "__main__":
    main()

  
