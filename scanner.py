import os
import json
import time
import random
import datetime as dt
import requests

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

# Paprastas filtras, kad būtų mažiau triukšmo, bet ir ne per griežta
# Jei nori dar paprasčiau, palik tik "transactionCode == P" ir tiek
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
    # SEC labai svarbu User-Agent. Be jo arba su "default" dažnai riboja.
    # Formatas: "TavoVardas Email" arba "Projektas kontaktas"
    ua = SEC_USER_AGENT if SEC_USER_AGENT else "KASInsiderScanner contact@example.com"
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
                # jei SEC atsiunčia Retry-After, gerbiam
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

            # Kiti statusai - laikom klaida, bet nereiškia retry
            r.raise_for_status()

        except Exception as e:
            last_err = e
            # backoff ir ant network klaidų
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
    Paimam naujausius Form 4 filingus iš SEC RSS/Atom feed.
    Tai yra lengviausias kelias, mažiau spaudžia limitus nei ėjimas per daug folderių.
    """
    # SEC Atom feed Form 4:
    # https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom
    feed_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom"
    r = sec_get(feed_url, timeout=30)

    # Atom yra XML, bet jame yra linkai su accession ir filing url.
    # Paprastai ištraukiam "href=" filing detalių puslapius.
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

        # mus domina filing detail puslapiai
        # pvz: https://www.sec.gov/Archives/edgar/data/.../...-index.html
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
    # index.html -> index.json tame pačiame folderyje
    # .../0001193125-26-xxxxxx-index.html -> .../index.json
    if not index_html_url.endswith("-index.html"):
        return None
    base = index_html_url.rsplit("/", 1)[0]
    return base + "/index.json"


def pick_primary_xml(index_json):
    """
    Iš index.json paimam tikėtiną Form 4 XML failą.
    """
    files = index_json.get("directory", {}).get("item", [])
    # dazniausiai xml yra "primaryDocument" arba tiesiog vienas .xml
    xml_candidates = [f["name"] for f in files if f.get("name", "").lower().endswith(".xml")]
    if not xml_candidates:
        return None
    # dažniausiai pirmas xml veikia
    return xml_candidates[0]


def parse_form4_xml(xml_text):
    """
    Minimalus XML parsingas be extra bibliotekų.
    Ištraukiam:
    issuerTradingSymbol
    reportingOwnerName
    transactionCode
    transactionShares
    transactionPricePerShare
    transactionDate
    """
    def find_tag(tag):
        start = xml_text.find(f"<{tag}>")
        if start == -1:
            return None
        start += len(tag) + 2
        end = xml_text.find(f"</{tag}>", start)
        if end == -1:
            return None
        return xml_text[start:end].strip()

    symbol = find_tag("issuerTradingSymbol")
    owner = find_tag("reportingOwnerName")
    code = find_tag("transactionCode")
    shares = find_tag("transactionShares")
    price = find_tag("transactionPricePerShare")
    tdate = find_tag("transactionDate")

    return {
        "symbol": symbol,
        "owner": owner,
        "code": code,
        "shares": shares,
        "price": price,
        "date": tdate
    }


def main():
    db = load_db()
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    db["last_run_utc"] = now

    new_items = []
    failed = 0

    index_links = get_latest_form4_feed(limit=MAX_NEW_FILINGS_TO_PROCESS)

    for index_html in index_links:
        index_json_url = index_json_from_index_html(index_html)
        if not index_json_url:
            continue

        # accession paprastai yra folderio pavadinime
        accession = index_html.split("/")[-2] if len(index_html.split("/")) >= 2 else index_html

        if accession in db.get("seen_accessions", []):
            continue

        try:
            j = sec_get(index_json_url, timeout=30).json()
            xml_name = pick_primary_xml(j)
            if not xml_name:
                # pažymim kaip matytą, kad nesikartotų
                db["seen_accessions"].append(accession)
                continue

            base = index_json_url.rsplit("/", 1)[0]
            xml_url = base + "/" + xml_name

            xml_text = sec_get(xml_url, timeout=30).text
            parsed = parse_form4_xml(xml_text)

            # Filtras
            if ONLY_PURCHASES and parsed.get("code") != "P":
                db["seen_accessions"].append(accession)
                continue

            sym = (parsed.get("symbol") or "").strip()
            if not sym:
                db["seen_accessions"].append(accession)
                continue

            new_items.append({
                "symbol": sym,
                "owner": parsed.get("owner"),
                "date": parsed.get("date"),
                "shares": parsed.get("shares"),
                "price": parsed.get("price"),
                "code": parsed.get("code"),
                "link": index_html
            })

            db["seen_accessions"].append(accession)

        except Exception:
            failed += 1
            # nekrentam iš viso run, tęsiam
            continue

    # apribojam db dydį, kad neaugtų be galo
    db["seen_accessions"] = db.get("seen_accessions", [])[-2500:]
    save_db(db)

    # Telegram žinutė be simbolių ir be triukšmo
    if not new_items:
        msg = f"Radaras patikrintas {now}\nNaujų insider pirkimų nerasta"
        send_telegram(msg)
        return

    # grupuojam pagal tickerį
    by_sym = {}
    for it in new_items:
        by_sym.setdefault(it["symbol"], []).append(it)

    lines = [f"Radaras patikrintas {now}", "Nauji insider pirkimai"]
    for sym in sorted(by_sym.keys()):
        items = by_sym[sym]
        # sutraukiam į vieną eilutę: ticker, kiek įrašų, paskutinis owner, link
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

    # jei kažkas failino, pridedam tyliai
    if failed:
        lines.append("")
        lines.append(f"Pastaba: dalis užklausų buvo apribotos arba nepavyko, bet radaras veikė")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()

   
