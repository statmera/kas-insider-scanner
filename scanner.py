import os
import json
import time
import random
import datetime as dt
import requests
import xml.etree.ElementTree as ET

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "").strip()

DB_PATH = "insider_db.json"

# Greičio kontrolė
MIN_SLEEP_BETWEEN_REQUESTS = 0.35  # sekundės
MAX_SLEEP_BETWEEN_REQUESTS = 0.75  # sekundės

# Retry kontrolė
MAX_RETRIES = 6
BASE_BACKOFF = 1.2  # sekundės
MAX_BACKOFF = 45.0  # sekundės

# Saugikliai, kad vienas run nebūtų per sunkus SEC
MAX_NEW_FILINGS_TO_PROCESS = 30  # mažink/didink pagal poreikį

# Paprastas filtras
ONLY_PURCHASES = True


def load_db():
    if not os.path.exists(DB_PATH):
        return {"seen_accessions": [], "last_run_utc": None}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen_accessions": [], "last_run_utc": None}


def save_db(db):
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def jitter_sleep(min_s=MIN_SLEEP_BETWEEN_REQUESTS, max_s=MAX_SLEEP_BETWEEN_REQUESTS):
    time.sleep(random.uniform(min_s, max_s))


def sec_headers():
    # SEC labai svarbu User-Agent.
    # Rekomenduojamas formatas: "Vardas Pavardė (projektas) email@domenas.com"
    ua = SEC_USER_AGENT if SEC_USER_AGENT else "KASInsiderScanner (contact: email@example.com)"
    return {
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    }


def sec_get(url, timeout=30):
    """
    Patikimas GET su retry/backoff ant 429/5xx.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            jitter_sleep()
            r = requests.get(url, headers=sec_headers(), timeout=timeout)

            if r.status_code == 200:
                return r

            # Jei 429 arba laikini 5xx, darom backoff
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait_s = float(ra)
                    except Exception:
                        wait_s = BASE_BACKOFF * (2 ** (attempt - 1))
                else:
                    wait_s = BASE_BACKOFF * (2 ** (attempt - 1))

                wait_s = min(wait_s, MAX_BACKOFF)
                time.sleep(wait_s + random.uniform(0.0, 0.8))
                continue

            r.raise_for_status()

        except Exception as e:
            last_err = e
            wait_s = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
            time.sleep(wait_s + random.uniform(0.0, 0.8))
            continue

    if last_err:
        raise last_err
    raise RuntimeError("SEC request failed without exception")


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Trūksta TELEGRAM_BOT_TOKEN arba TELEGRAM_CHAT_ID secrets")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def get_latest_form4_feed(limit=MAX_NEW_FILINGS_TO_PROCESS):
    """
    Paimam naujausius Form 4 filingus iš SEC Atom feed.
    """
    feed_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom"
    r = sec_get(feed_url, timeout=30)
    text = r.text

    links = []
    needle = 'href="'
    idx = 0
    while True:
        idx = text.find(needle, idx)
        if idx == -1:
            break
        idx += len(needle)
        end = text.find('"', idx)
        if end == -1:
            break
        href = text[idx:end]
        idx = end + 1

        if "/Archives/edgar/data/" in href and href.endswith("-index.html"):
            links.append(href)

    # unikalūs, išlaikom eilę
    seen = set()
    uniq = []
    for x in links:
        if x not in seen:
            uniq.append(x)
            seen.add(x)

    return uniq[:limit]


def index_json_from_index_html(index_html_url):
    if not index_html_url.endswith("-index.html"):
        return None
    base = index_html_url.rsplit("/", 1)[0]
    return base + "/index.json"


def pick_primary_xml(index_json):
    """
    Iš index.json paimam tikėtiną Form 4 XML failą.
    """
    files = index_json.get("directory", {}).get("item", [])
    xml_candidates = [f["name"] for f in files if f.get("name", "").lower().endswith(".xml")]
    if not xml_candidates:
        return None
    return xml_candidates[0]


def _txt(node):
    return node.text.strip() if node is not None and node.text else None


def parse_form4_xml_purchases(xml_text):
    """
    Tvirtas Form 4 XML parsingas:
    - paima issuerTradingSymbol
    - paima rptOwnerName
    - surenka VISAS nonDerivativeTransaction su transactionCode == "P"
    Grąžina: list[purchase], kur purchase turi symbol/owner/date/shares/price/code
    """
    root = ET.fromstring(xml_text)

    # issuerTradingSymbol
    symbol = None
    for el in root.iter():
        if el.tag.endswith("issuerTradingSymbol"):
            symbol = _txt(el)
            break

    # owner name (rptOwnerName)
    owner = None
    for el in root.iter():
        if el.tag.endswith("rptOwnerName"):
            owner = _txt(el)
            break

    purchases = []

    # non-derivative transactions
    for txn in root.iter():
        if not txn.tag.endswith("nonDerivativeTransaction"):
            continue

        code = shares = price = tdate = None

        for el in txn.iter():
            if el.tag.endswith("transactionCode"):
                code = _txt(el)
            elif el.tag.endswith("transactionShares"):
                shares = _txt(el)
            elif el.tag.endswith("transactionPricePerShare"):
                price = _txt(el)
            elif el.tag.endswith("transactionDate"):
                tdate = _txt(el)

        if code == "P":
            purchases.append({
                "symbol": symbol,
                "owner": owner,
                "code": code,
                "shares": shares,
                "price": price,
                "date": tdate
            })

    return purchases


def main():
    db = load_db()
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    db["last_run_utc"] = now

    new_items = []
    failed = 0
    last_err = None

    index_links = get_latest_form4_feed(limit=MAX_NEW_FILINGS_TO_PROCESS)

    for index_html in index_links:
        index_json_url = index_json_from_index_html(index_html)
        if not index_json_url:
            continue

        accession = index_html.split("/")[-2] if len(index_html.split("/")) >= 2 else index_html

        if accession in db.get("seen_accessions", []):
            continue

        try:
            j = sec_get(index_json_url, timeout=30).json()
            xml_name = pick_primary_xml(j)
            if not xml_name:
                db["seen_accessions"].append(accession)
                continue

            base = index_json_url.rsplit("/", 1)[0]
            xml_url = base + "/" + xml_name

            xml_text = sec_get(xml_url, timeout=30).text
            purchases = parse_form4_xml_purchases(xml_text)

            # Filtras: tik jei nėra nė vieno P, atmetam
            if ONLY_PURCHASES and not purchases:
                db["seen_accessions"].append(accession)
                continue

            sym = (purchases[0].get("symbol") or "").strip()
            if not sym:
                db["seen_accessions"].append(accession)
                continue

            # Įdedam visas P transakcijas iš šito filing'o
            for p in purchases:
                new_items.append({
                    "symbol": sym,
                    "owner": p.get("owner"),
                    "date": p.get("date"),
                    "shares": p.get("shares"),
                    "price": p.get("price"),
                    "code": p.get("code"),
                    "link": index_html
                })

            db["seen_accessions"].append(accession)

        except Exception as e:
            failed += 1
            last_err = str(e)
            # nekrentam iš viso run, tęsiam
            continue

    # apribojam db dydį
    db["seen_accessions"] = db.get("seen_accessions", [])[-2500:]
    save_db(db)

    if not new_items:
        msg = f"Radaras patikrintas {now}\nNaujų insider pirkimų nerasta"
        if failed and last_err:
            msg += f"\nPastaba: buvo klaidų (pvz. {last_err[:120]})"
        send_telegram(msg)
        return

    # grupuojam pagal tickerį
    by_sym = {}
    for it in new_items:
        by_sym.setdefault(it["symbol"], []).append(it)

    lines = [f"Radaras patikrintas {now}", "Nauji insider pirkimai"]
    for sym in sorted(by_sym.keys()):
        items = by_sym[sym]
        last = items[0]

        owners = sorted({(x.get('owner') or '').strip() for x in items if x.get('owner')})
        owners_txt = ", ".join([o for o in owners if o])[:140]

        lines.append("")
        lines.append(f"{sym} | įrašų {len(items)}")
        if owners_txt:
            lines.append(f"Insider: {owners_txt}")
        if last.get("date"):
            lines.append(f"Data: {last.get('date')}")
        if last.get("shares") or last.get("price"):
            s = last.get("shares") or "-"
            p = last.get("price") or "-"
            lines.append(f"Kiekis: {s} | Kaina: {p}")
        lines.append(f"Nuoroda: {last.get('link')}")

    if failed:
        lines.append("")
        lines.append("Pastaba: dalis užklausų buvo apribotos arba nepavyko, bet radaras veikė")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()
