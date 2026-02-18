import os
import json
import datetime as dt
import requests

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "").strip()

DB_FILE = "insider_db.json"

# Grąžinti griežtesnį filtrą (keisk čia jei reikia)
MIN_USD = 100_000  # 100k USD

# Kiek naujausių Form 4 tikrinti iš SEC "current filings"
MAX_FEED_ITEMS = 120

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count={count}&output=atom"

def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Trūksta TELEGRAM_BOT_TOKEN arba TELEGRAM_CHAT_ID")
    r = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=30,
    )
    r.raise_for_status()

def load_db():
    if not os.path.exists(DB_FILE):
        return {"seen": {}}
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "seen" not in data or not isinstance(data["seen"], dict):
            return {"seen": {}}
        return data
    except Exception:
        return {"seen": {}}

def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def sec_get(url: str) -> requests.Response:
    if not SEC_USER_AGENT or "@" not in SEC_USER_AGENT:
        raise RuntimeError("SEC_USER_AGENT turi būti reali eilutė su el. paštu (pvz. Vardas Pavarde email@domenas)")
    headers = {"User-Agent": SEC_USER_AGENT, "Accept": "*/*"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r

def parse_atom_for_folder_urls(atom_xml: str):
    # Iš atom feed'o ištraukiam nuorodas į filing folderį (Archives/edgar/data/...)
    urls = []
    parts = atom_xml.split("<entry>")
    for e in parts[1:]:
        if "Archives/edgar/data" not in e:
            continue
        start = e.find("https://www.sec.gov/Archives/edgar/data")
        if start == -1:
            start = e.find("http://www.sec.gov/Archives/edgar/data")
        if start == -1:
            continue
        end = e.find("</link>", start)
        # saugiau: ieškom pirmos kabutės po start
        q = e.find('"', start)
        if q != -1:
            url = e[start:q]
            # folder url paprastai baigiasi .txt arba index, bet mums reikia direktorijos:
            # paimam iki paskutinio '/'
            if "/" in url:
                folder = url.rsplit("/", 1)[0] + "/"
                urls.append(folder)
    # dedup
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def folder_to_xml_url(folder_url: str):
    # folder_url + "index.json" -> randa pirmą .xml
    j = sec_get(folder_url + "index.json").json()
    items = j.get("directory", {}).get("item", [])
    for it in items:
        name = it.get("name", "")
        if isinstance(name, str) and name.lower().endswith(".xml"):
            return folder_url + name
    return None

def find_between(s: str, a: str, b: str):
    i = s.find(a)
    if i == -1:
        return None
    i += len(a)
    j = s.find(b, i)
    if j == -1:
        return None
    return s[i:j].strip()

def parse_form4_purchases(xml: str):
    # Grąžina listą: (ticker, owner, date, shares, price, value)
    ticker = find_between(xml, "<issuerTradingSymbol>", "</issuerTradingSymbol>")
    owner = find_between(xml, "<rptOwnerName>", "</rptOwnerName>")
    if not ticker:
        return []
    ticker = ticker.upper().strip()
    owner = (owner or "").strip()

    results = []
    blocks = xml.split("<nonDerivativeTransaction>")
    for blk in blocks[1:]:
        if "<transactionCode>P</transactionCode>" not in blk:
            continue

        date_block = find_between(blk, "<transactionDate>", "</transactionDate>")
        tx_date = find_between(date_block, "<value>", "</value>") if date_block else None

        shares_block = find_between(blk, "<transactionShares>", "</transactionShares>")
        shares_str = find_between(shares_block, "<value>", "</value>") if shares_block else None

        price_block = find_between(blk, "<transactionPricePerShare>", "</transactionPricePerShare>")
        price_str = find_between(price_block, "<value>", "</value>") if price_block else None

        try:
            shares = float(str(shares_str).replace(",", ""))
            price = float(str(price_str).replace(",", ""))
            value = shares * price
        except Exception:
            continue

        results.append((ticker, owner, tx_date or "", shares, price, value))

    return results

def main():
    db = load_db()
    seen = db.get("seen", {})

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    atom = sec_get(SEC_ATOM.format(count=MAX_FEED_ITEMS)).text
    folders = parse_atom_for_folder_urls(atom)

    found = []
    total_folders = 0
    total_xml = 0

    for folder in folders:
        total_folders += 1
        xml_url = folder_to_xml_url(folder)
        if not xml_url:
            continue
        # unique key pagal xml url (vienam filing’ui pakanka)
        if xml_url in seen:
            continue

        total_xml += 1
        xml = sec_get(xml_url).text
        purchases = parse_form4_purchases(xml)

        # pažymim kaip matytą, net jei nepraeis filtro (kad nekartotų)
        seen[xml_url] = {"ts": now, "p_found": len(purchases)}

        for ticker, owner, tx_date, shares, price, value in purchases:
            if value < MIN_USD:
                continue
            found.append((ticker, owner, tx_date, shares, price, value, xml_url))

    db["seen"] = seen
    save_db(db)

    if found:
        # trumpa, aiški suvestinė
        lines = [f"Insider radaras {now}", f"Rasta {len(found)} pirkimai virš {MIN_USD:,} USD", ""]
        for ticker, owner, tx_date, shares, price, value, xml_url in found[:25]:
            lines.append(
                f"{ticker} | {owner if owner else 'owner n/a'} | {tx_date} | {value:,.0f} USD | {shares:.0f} @ {price:.2f}"
            )
        if len(found) > 25:
            lines.append("")
            lines.append(f"Papildomai dar {len(found) - 25} įrašai (nesiųsti, kad nebūtų triukšmo)")
        send_telegram("\n".join(lines))
    else:
        # heartbeat
        send_telegram(f"Radaras patikrintas {now}\nNaujų insider pirkimų pagal filtrą nerasta")

if __name__ == "__main__":
    main()
