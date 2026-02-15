import os
import json
import datetime as dt
import requests
import yfinance as yf

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT")

DB_FILE = "insider_db.json"
MIN_USD = 150000

SEC_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=40&output=atom"

def send(msg):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
        timeout=30,
    )

def load_db():
    with open(DB_FILE, "r") as f:
        return json.load(f)

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)

def fetch_sec():
    headers = {"User-Agent": SEC_USER_AGENT}
    r = requests.get(SEC_URL, headers=headers, timeout=30)
    return r.text

def parse_atom(xml):
    entries = xml.split("<entry>")
    results = []
    for e in entries[1:]:
        if "Form 4" not in e:
            continue
        if "Archives/edgar/data" not in e:
            continue
        start = e.find("Archives/edgar/data")
        link = "https://www.sec.gov/" + e[start:e.find('"', start)]
        results.append(link)
    return results

def get_xml_url(folder_url):
    headers = {"User-Agent": SEC_USER_AGENT}
    r = requests.get(folder_url + "index.json", headers=headers, timeout=30)
    data = r.json()
    for item in data["directory"]["item"]:
        if item["name"].endswith(".xml"):
            return folder_url + item["name"]
    return None

def parse_form4(xml):
    results = []
    if "<transactionCode>P</transactionCode>" not in xml:
        return results
    try:
        ticker = xml.split("<issuerTradingSymbol>")[1].split("</issuerTradingSymbol>")[0]
    except:
        return results
    blocks = xml.split("<nonDerivativeTransaction>")
    for b in blocks[1:]:
        if "<transactionCode>P</transactionCode>" not in b:
            continue
        try:
            shares = float(b.split("<value>")[1].split("</value>")[0])
            price = float(b.split("<transactionPricePerShare>")[1].split("<value>")[1].split("</value>")[0])
            value = shares * price
            if value >= MIN_USD:
                results.append((ticker, value))
        except:
            continue
    return results

def structure_filter(ticker):
    try:
        data = yf.Ticker(ticker).history(period="6mo")
        if data.empty:
            return False
        low = data["Close"].min()
        high = data["Close"].max()
        current = data["Close"].iloc[-1]
        drawdown = (high - current) / high
        range_pos = (current - low) / (high - low) if high > low else 1
        return drawdown >= 0.30 or range_pos <= 0.65
    except:
        return False

def main():
    db = load_db()
    atom = fetch_sec()
    folders = parse_atom(atom)

    today = str(dt.date.today())

    for folder in folders:
        xml_url = get_xml_url(folder)
        if not xml_url:
            continue
        headers = {"User-Agent": SEC_USER_AGENT}
        r = requests.get(xml_url, headers=headers, timeout=30)
        buys = parse_form4(r.text)
        for ticker, value in buys:
            if ticker not in db["tickers"]:
                db["tickers"][ticker] = []
            db["tickers"][ticker].append({"date": today, "value": value})

    save_db(db)

    msg_lines = ["KAS INSIDER KAUPIMO RADAR\n"]
    now = dt.date.today()

    for ticker, records in db["tickers"].items():
        last30 = [r for r in records if (now - dt.date.fromisoformat(r["date"])).days <= 30]
        last14 = [r for r in records if (now - dt.date.fromisoformat(r["date"])).days <= 14]

        if len(last30) >= 2 or len(last14) >= 2:
            if structure_filter(ticker):
                total = sum(r["value"] for r in last30)
                msg_lines.append(
                    f"{ticker} | {len(last30)} buys / 30d | ${int(total):,}"
                )

    if len(msg_lines) > 1:
        send("\n".join(msg_lines))

if __name__ == "__main__":
    main()

