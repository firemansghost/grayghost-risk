# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# ----- helpers -----
def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))

def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "gh-actions/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def http_json(url, timeout=20):
    return json.loads(http_get(url, timeout=timeout))

def fetch_btc_price_usd():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        payload = http_json(url, timeout=10)
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
    except Exception:
        return None

def fetch_etf_trailing(n=7):
    """
    Scrape Farside 'Bitcoin ETF Flow – All Data' and return latest n
    daily totals in USD as [(date_str, usd)], most recent first.
    """
    url = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    try:
        html = http_get(url, timeout=20)
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"[ \t]+", " ", text)
        rows = re.findall(
            r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})(.*?)(?=\d{1,2}\s+[A-Za-z]{3}\s+\d{4}|$)",
            text, flags=re.S
        )
        vals = []
        for date_str, block in reversed(rows):  # oldest -> newest
            toks = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", block)
            if not toks:
                continue
            total_musd = parse_number_token(toks[-1])  # last number = Total
            if total_musd is None:
                continue
            vals.append((date_str, round(float(total_musd) * 1_000_000.0, 2)))
        vals = vals[-n:]                 # keep last n
        vals = list(reversed(vals))      # most recent first
        return vals
    except Exception as e:
        print(f"[run_daily] WARN fetch_etf_trailing failed: {e}", file=sys.stderr)
        return []

def fetch_stablecoin_deltas(coin_id, days=8):
    """
    CoinGecko daily market caps for a stablecoin.
    Returns list of (timestamp_ms, market_cap_usd).
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={days}&interval=daily"
    try:
        payload = http_json(url, timeout=20)
        caps = payload.get("market_caps", [])
        return [(int(ts), float(val)) for ts, val in caps if val is not None]
    except Exception as e:
        print(f"[run_daily] WARN CG fetch {coin_id} failed: {e}", file=sys.stderr)
        return []

def combine_stablecoin_issuance(days=8):
    """
    Combine USDT + USDC daily market caps and compute daily deltas.
    Returns: (today_delta_usd, sma7_delta_usd, trailing_list)
    trailing_list is most recent first: [{date:'DD Mon YYYY', usd:delta}, ...]
    """
    teth = fetch_stablecoin_deltas("tether", days=days)
    usdc = fetch_stablecoin_deltas("usd-coin", days=days)
    if not teth or not usdc:
        return None, None, []

    total_caps = []
    L = min(len(teth), len(usdc))
    for i in range(L):
        ts = max(teth[i][0], usdc[i][0])
        total_caps.append((ts, teth[i][1] + usdc[i][1]))
    if len(total_caps) < 2:
        return None, None, []

    deltas = []
    for i in range(1, len(total_caps)):
        ts, cap = total_caps[i]
        prev_cap = total_caps[i-1][1]
        deltas.append((ts, cap - prev_cap))

    last7 = deltas[-7:]
    today_delta = last7[-1][1] if last7 else None
    sma7 = round(sum(v for _, v in last7) / len(last7), 2) if last7 else None

    trailing = []
    for ts, v in reversed(last7):  # most recent first
        d = datetime.datetime.utcfromtimestamp(ts/1000).strftime("%d %b %Y")
        trailing.append({"date": d, "usd": round(v, 2)})

    return (round(today_delta, 2) if today_delta is not None else None,
            sma7 if sma7 is not None else None,
            trailing)

def sigmoid(x):
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

# ----- base risk placeholder -----
prev_risk = 0.35
if latest_path.exists():
    try:
        prev_risk = float(json.loads(latest_path.read_text()).get("risk", 0.35))
    except Exception:
        pass
risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

# ----- real driver: ETF flows -----
trail = fetch_etf_trailing(n=7)           # [(date, usd)], most recent first
etf_usd = trail[0][1] if trail else None
etf_date = trail[0][0] if trail else None
sma7_etf = round(sum(v for _, v in trail) / len(trail), 2) if trail else None
etf_score_base = sma7_etf if sma7_etf is not None else (etf_usd or 0.0)
etf_score = clamp(sigmoid(-etf_score_base / 200_000_000.0), 0.0, 1.0)  # inflow lowers risk
etf_contrib = round((etf_score - 0.5) * 0.2, 2)

# ----- real driver: Stablecoin issuance (USDT+USDC) -----
sc_today, sc_sma7, sc_trailing = combine_stablecoin_issuance(days=8)
sc_base = sc_sma7 if sc_sma7 is not None else (sc_today or 0.0)
sc_score = clamp(sigmoid(-sc_base / 1_000_000_000.0), 0.0, 1.0)        # issuance lowers risk
sc_contrib = round((sc_score - 0.5) * 0.2, 2)

# ----- other drivers (still dummy) -----
drivers = {
    "etf_flows": {
        "score": round(etf_score, 2),
        "contribution": etf_contrib,
        "raw_usd": etf_usd,
        "sma7_usd": sma7_etf,
        "asof": etf_date,
        "trailing": [{"date": d, "usd": v} for d, v in trail],
        "source": "Farside Bitcoin ETF Flow – All Data"
    },
    "net_liquidity":  {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": None},
    "stablecoins": {
        "score": round(sc_score, 2),
        "contribution": sc_contrib,
        "raw_delta_usd": sc_today,
        "sma7_delta_usd": sc_sma7,
        "trailing": sc_trailing,
        "source": "CoinGecko USDT + USDC market_caps (daily)"
    },
    "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": None},
    "onchain":        {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": None}
}

# ----- BTC price -----
prev_doc = {}
if latest_path.exists():
    try:
        prev_doc = json.loads(latest_path.read_text())
    except Exception:
        pass
btc_price = fetch_btc_price_usd() or prev_doc.get("btc_price_usd")

# ----- assemble & write -----
doc = {
    "as_of": as_of,
    "risk": round(risk, 2),
    "band": band,
    "regime": regime,
    "btc_price_usd": btc_price,
    "etf_flow_usd": etf_usd,
    "etf_flow_sma7_usd": sma7_etf,
    "stablecoin_delta_usd": sc_today,
    "stablecoin_delta_sma7_usd": sc_sma7,
    "drivers": drivers
}

latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

print(f"[run_daily] OK risk={risk:.2f} band={band} btc={btc_price} "
      f"etf_usd={etf_usd} etf_sma7={sma7_etf} sc_today={sc_today} sc_sma7={sc_sma7}")
