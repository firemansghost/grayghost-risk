# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# ---------- helpers ----------
def clamp(x, lo=0.0, hi=1.0): return max(lo, min(hi, x))

def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent":"gh-actions/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def fetch_btc_price_usd():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        txt = http_get(url, timeout=10)
        payload = json.loads(txt)
        return round(float(payload["data"]["amount"]), 2)
    except Exception as e:
        print(f"[run_daily] WARN price fetch failed: {e}", file=sys.stderr)
        return None

def parse_number_token(tok: str):
    tok = tok.strip().replace(",", "")
    if tok in {"-", "–", "—", ""}:
        return None
    neg = tok.startswith("(") and tok.endswith(")")
    tok = tok.strip("()")
    try:
        val = float(tok)
        return -val if neg else val
    except:
        return None

def fetch_etf_trailing(n=7):
    """
    Scrape Farside 'Bitcoin ETF Flow – All Data' and return the latest N
    daily totals (USD) *most recent first*, with their dates.
    """
    url = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    try:
        html = http_get(url, timeout=20)
        # strip tags → plain text
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"[ \t]+", " ", text)

        # rows like "11 Jan 2024 ..."
        rows = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})(.*?)(?=\d{1,2}\s+[A-Za-z]{3}\s+\d{4}|$)", text, flags=re.S)

        vals = []  # [(date_str, total_usd)]
        for date_str, block in reversed(rows):      # iterate from oldest → newest
            toks = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", block)
            if not toks:
                continue
            total_musd = parse_number_token(toks[-1])   # last number in row == "Total"
            if total_musd is None:
                continue
            vals.append((date_str, round(float(total_musd) * 1_000_000.0, 2)))

        # keep the most recent n
        vals = vals[-n:]
        # return most recent first
        vals = list(reversed(vals))
        return vals
    except Exception as e:
        print(f"[run_daily] WARN fetch_etf_trailing failed: {e}", file=sys.stderr)
        return []

def sigmoid(x):
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

# ---------- dummy base risk (placeholder) ----------
risk_prev = 0.35
if latest_path.exists():
    try:
        risk_prev = float(json.loads(latest_path.read_text()).get("risk", 0.35))
    except Exception:
        pass
risk = clamp(risk_prev + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

# ---------- real driver: ETF flows (with instant 7d backfill) ----------
trail = fetch_etf_trailing(n=7)              # [(date, usd), most recent first]
etf_usd = trail[0][1] if trail else None
etf_date = trail[0][0] if trail else None
if trail:
    sma7_usd = round(sum(v for _, v in trail) / len(trail), 2)
else:
    sma7_usd = None

# normalize: big inflow → lower risk score
if sma7_usd is not None:
    etf_score = clamp(sigmoid(-sma7_usd / 2e8), 0.0, 1.0)
else:
    base = etf_usd if etf_usd is not None else 0.0
    etf_score = clamp(sigmoid(-base / 2e8), 0.0, 1.0)

etf_contrib = round((etf_score - 0.5) * 0.2, 2)

# ---------- other drivers still dummy for now ----------
drivers = {
    "etf_flows": {
        "score": round(etf_score, 2),
        "contribution": etf_contrib,
        "raw_usd": etf_usd,
        "sma7_usd": sma7_usd,
        "asof": etf_date,
        "trailing": [{"date": d, "usd": v} for d, v in trail],
        "source": "Farside 'Bitcoin ETF Flow – All Data'"
    },
    "net_liquidity":  {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": None},
    "stablecoins":    {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": None},
    "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": None},
    "onchain":        {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": None}
}

# ---------- BTC price ----------
btc_price = fetch_btc_price_usd() or (json.loads(latest_path.read_text()).get("btc_price_usd") if latest_path.exists() else None)

# ---------- assemble & write ----------
doc = {
    "as_of": as_of,
    "risk": round(risk, 2),
    "band": band,
    "regime": regime,
    "btc_price_usd": btc_price,
    "etf_flow_usd": etf_usd,
    "etf_flow_sma7_usd": sma7_usd,
    "drivers": drivers
}

latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

print(f"[run_daily] OK risk={risk:.2f} band={band} btc={btc_price} etf_usd={etf_usd} sma7={sma7_usd} asof={etf_date}")
