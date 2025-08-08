# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# ---------- helpers ----------
def get_prev_doc():
    try:
        if latest_path.exists():
            return json.loads(latest_path.read_text())
    except Exception as e:
        print(f"[run_daily] WARN could not read prev latest.json: {e}", file=sys.stderr)
    return {}

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
    """Parses numbers like '111.7' or '(52.7)' → float (negative if parens). Returns None if not a number."""
    tok = tok.strip().replace(",", "")
    if tok == "-" or tok == "–" or tok == "—" or tok == "":  # missing
        return None
    neg = tok.startswith("(") and tok.endswith(")")
    tok = tok.strip("()")
    try:
        val = float(tok)
        return -val if neg else val
    except:
        return None

def fetch_etf_flow_usd():
    """
    Scrape Farside 'Bitcoin ETF Flow – All Data' table and return:
      - today's total daily net flow in USD (float, not millions),
      - plus a 7-day SMA of that daily net flow (USD).
    Farside shows values in US$ millions per day. We'll grab the LAST date row with a numeric 'Total'.
    """
    url = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    try:
        html = http_get(url, timeout=20)
        # Strip tags to make row parsing easier
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"[ \t]+", " ", text)
        # Find all date-row chunks: date + the numbers that follow until the next date or end
        # Dates like "11 Jan 2024"
        rows = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})(.*?)(?=\d{1,2}\s+[A-Za-z]{3}\s+\d{4}|$)", text, flags=re.S)
        last_total_musd = None
        last_date = None
        # Iterate from the end to get the most recent with a Total number
        for date_str, block in reversed(rows):
            # Grab all number-like tokens in that row
            toks = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", block)
            if not toks:
                continue
            # The final number in the row should be the "Total"
            maybe_total = parse_number_token(toks[-1])
            if maybe_total is not None:
                last_total_musd = maybe_total
                last_date = date_str
                break
        if last_total_musd is None:
            raise RuntimeError("Could not find last Total value")

        # Convert millions USD → USD
        last_total_usd = round(float(last_total_musd) * 1_000_000.0, 2)

        # Build a short trailing window from history to compute a 7-day SMA
        # We'll store today's raw flow into the doc; SMA uses up to last 6 history days + today.
        trailing = []
        for i in range(30):  # look back up to 30 days
            d = as_of if i == 0 else (datetime.date.today() - datetime.timedelta(days=i)).isoformat()
            p = HIST / f"{d}.json"
            if not p.exists(): 
                continue
            try:
                rec = json.loads(p.read_text())
                v = rec.get("etf_flow_usd")
                if isinstance(v, (int, float)):
                    trailing.append(float(v))
            except Exception:
                continue
            if len(trailing) >= 6:  # we have up to 6 past days
                break
        # Include today's value at the front
        series = [last_total_usd] + trailing
        sma7 = sum(series[:7]) / min(len(series), 7)

        return last_total_usd, sma7, last_date
    except Exception as e:
        print(f"[run_daily] WARN fetch_etf_flow_usd failed: {e}", file=sys.stderr)
        return None, None, None

def sigmoid(x): 
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

# ---------- previous ----------
prev = get_prev_doc()
prev_risk = float(prev.get("risk", 0.35))

# ---------- dummy risk drift for MVP (we'll replace later) ----------
risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

# ---------- real driver: ETF flows ----------
etf_usd, etf_sma7_usd, etf_date = fetch_etf_flow_usd()
# Normalization: map big inflows → low score; big outflows → high score.
# Scale: ~$200m typical daily flow → tune as 2e8; we use the SMA to de-noise.
if etf_sma7_usd is not None:
    s = sigmoid(-etf_sma7_usd / 2e8)  # inflow positive → negative arg → score toward 0
    etf_score = clamp(s, 0.0, 1.0)
else:
    # Fallback: use today's raw, or neutral 0.5 if missing
    base = etf_usd if etf_usd is not None else 0.0
    etf_score = clamp(sigmoid(-base / 2e8), 0.0, 1.0)

# Contribution: center at 0.5 → positive means pushing risk up, negative reduces risk.
etf_contrib = round((etf_score - 0.5) * 0.2, 2)  # ±0.10 max for now (tune later)

# ---------- other drivers (still dummy placeholders) ----------
drivers = {
    "etf_flows": {
        "score": round(etf_score, 2),
        "contribution": etf_contrib,
        "raw_usd": etf_usd,
        "sma7_usd": etf_sma7_usd,
        "source": "Farside 'Bitcoin ETF Flow – All Data'"
    },
    "net_liquidity":  {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-80_000_000_000, 80_000_000_000)},
    "stablecoins":    {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-2_000_000_000, 3_000_000_000)},
    "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": round(random.uniform(-5,12),2)},
    "onchain":        {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": f"LTH_SOPR={round(random.uniform(0.92,1.08),2)}"}
}

# ---------- BTC price ----------
btc_price = fetch_btc_price_usd() or prev.get("btc_price_usd")

# ---------- assemble & write ----------
doc = {
    "as_of": as_of,
    "risk": round(risk, 2),
    "band": band,
    "regime": regime,
    "btc_price_usd": btc_price,
    "etf_flow_usd": etf_usd,          # expose raw for history/backtest
    "etf_flow_sma7_usd": etf_sma7_usd,
    "drivers": drivers
}

latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

print(f"[run_daily] OK risk={risk:.2f} band={band} btc={btc_price} etf_usd={etf_usd} sma7={etf_sma7_usd} date={etf_date}")
