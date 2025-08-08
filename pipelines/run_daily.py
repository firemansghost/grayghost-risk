# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# -------- helpers --------
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
    daily totals in USD, as a list of (date_str, usd). Most recent first.
    """
    url = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    try:
        html = http_get(url, timeout=20)
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"[ \t]+", " ", text)
        rows = re.findall(
            r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})(.*?)(?=\d{1,2}\s+[A-Za-z]{3}\s+\d{4}|$)",
            text,
            flags=re.S,
        )
        vals = []
        for date_str, block in reversed(rows):  # oldest -> newest
            toks = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", block)
            if not toks:
                continue
            total_musd = parse_number_token(toks[-1])  # last number = Total column
            if total_musd is None:
                continue
            vals.append((date_str, round(float(total_musd) * 1_000_000.0, 2)))
        vals = vals[-n:]                 # keep last n
        vals = list(reversed(vals))      # most recent first
        return vals
    except Exception as e:
        print(f"[run_daily] WARN fetch_etf_trailing failed: {e}", file=sys.stderr)
        return []

def sigmoid(x):
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

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

# -------- base risk placeholder --------
prev_risk = 0.35
if latest_path.exists():
    try:
        prev_risk = float(json.loads(latest_path.read_text()).get("risk", 0.35))
    except Exception:
        pass
risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

# -------- real driver: ETF flows --------
trail
