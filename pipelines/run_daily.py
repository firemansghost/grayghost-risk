# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math, os

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# ----- tiny utils -----
def clamp(x, lo=0.0, hi=1.0): return max(lo, min(hi, x))

def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "gh-actions/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def http_json(url, timeout=20):
    return json.loads(http_get(url, timeout=timeout))

def sigmoid(x):
    try: return 1.0 / (1.0 + math.exp(-x))
    except OverflowError: return 0.0 if x < 0 else 1.0

# ----- BTC price (Coinbase) -----
def fetch_btc_price_usd():
    try:
        j = http_json("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10)
        return round(float(j["data"]["amount"]), 2)
    except Exception as e:
        print(f"[run_daily] WARN price fetch failed: {e}", file=sys.stderr)
        return None

# ----- ETF flows (Farside) -----
def parse_number_token(tok: str):
    tok = tok.strip().replace(",", "")
    if tok in {"-", "–", "—", ""}: return None
    neg = tok.startswith("(") and tok.endswith(")")
    tok = tok.strip("()")
    try: v = float(tok); return -v if neg else v
    except: return None

def fetch_etf_trailing(n=7):
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
        for date_str, block in reversed(rows):  # oldest → newest
            toks = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", block)
            if not toks: continue
            total_musd = parse_number_token(toks[-1])  # 'Total' column
            if total_musd is None: continue
            vals.append((date_str, round(float(total_musd)*1_000_000, 2)))
        vals = vals[-n:]
        vals = list(reversed(vals))  # most recent first
        return vals
    except Exception as e:
        print(f"[run_daily] WARN fetch_etf_trailing failed: {e}", file=sys.stderr)
        return []

# ----- Stablecoin issuance (CoinGecko) -----
def fetch_stablecoin_deltas(coin_id, days=8):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={days}&interval=daily"
    try:
        caps = http_json(url, timeout=20).get("market_caps", [])
        return [(int(ts), float(val)) for ts, val in caps if val is not None]
    except Exception as e:
        print(f"[run_daily] WARN CG fetch {coin_id} failed: {e}", file=sys.stderr)
        return []

def combine_stablecoin_issuance(days=8):
    teth = fetch_stablecoin_deltas("tether", days)
    usdc = fetch_stablecoin_deltas("usd-coin", days)
    if not teth or not usdc: return None, None, []
    L = min(len(teth), len(usdc))
    total = []
    for i in range(L):
        ts = max(teth[i][0], usdc[i][0])
        total.append((ts, teth[i][1]+usdc[i][1]))
    if len(total) < 2: return None, None, []
    deltas = []
    for i in range(1, len(total)):
        ts, cap = total[i]
        prev = total[i-1][1]
        deltas.append((ts, cap - prev))
    last7 = deltas[-7:]
    today = last7[-1][1] if last7 else None
    sma7 = round(sum(v for _, v in last7)/len(last7), 2) if last7 else None
    trail = []
    for ts, v in reversed(last7):  # most recent first
        d = datetime.datetime.utcfromtimestamp(ts/1000).strftime("%d %b %Y")
        trail.append({"date": d, "usd": round(v,2)})
    return (round(today,2) if today is not None else None, sma7, trail)

# ----- FRED helpers (Net Liquidity) -----
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()

def fetch_fred_series(series_id, days=120):
    if not FRED_API_KEY:
        print("[run_daily] INFO no FRED_API_KEY, skipping Net Liquidity", file=sys.stderr)
        return []  # no key: skip cleanly
    start = (datetime.date.today() - datetime.timedelta(days=days+5)).isoformat()
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&observation_start={start}")
    try:
        obs = http_json(url, timeout=20).get("observations", [])
        out = []
        for o in obs:
            d = o.get("date"); v = o.get("value")
            if not d or v in (None, ".", ""): continue
            try: out.append((datetime.date.fromisoformat(d), float(v)))
            except: continue
        return out  # list of (date, value)
    except Exception as e:
        print(f"[run_daily] WARN FRED {series_id} failed: {e}", file=sys.stderr)
        return []

def forward_fill(daily_dates, pairs):
    """pairs: list[(date, value)] sparse; return list aligned to daily_dates with last-known-value fill."""
    mp = {d:v for d,v in pairs}
    filled = []
    last = None
    for d in daily_dates:
        if d in mp: last = mp[d]
        filled.append(last)
    return filled

def compute_net_liquidity():
    """Return dict with level, deltas, and trailing for UI; or None if no key/data."""
    # fetch
    walcl = fetch_fred_series("WALCL", days=180)          # weekly
    tga   = fetch_fred_series("WTREGEN", days=180)        # daily TGA balance
    rrp   = fetch_fred_series("RRPONTSYD", days=180)      # daily ON RRP outstanding
    if not walcl or not tga or not rrp:
        return None
    # build daily date index
    start = max(min(walcl[0][0], tga[0][0], rrp[0][0]), datetime.date.today() - datetime.timedelta(days=120))
    dates = [start + datetime.timedelta(days=i) for i in range((datetime.date.today()-start).days+1)]
    f_w = forward_fill(dates, walcl)
    f_t = forward_fill(dates, tga)
    f_r = forward_fill(dates, rrp)
    # compute net liquidity series
    net = []
    for i, d in enumerate(dates):
        if f_w[i] is None or f_t[i] is None or f_r[i] is None:
            net.append(None)
        else:
            net.append(f_w[i] - f_t[i] - f_r[i])
    # drop leading None
    first_idx = next((i for i,x in enumerate(net) if x is not None), None)
    if first_idx is None: return None
    dates = dates[first_idx:]; net = net[first_idx:]
    # latest
    level = net[-1]
    delta1d = level - net[-2] if len(net) >= 2 else 0.0
    delta7d = level - net[-7] if len(net) >= 7 else delta1d
    sma7 = delta7d / 7.0
    # trailing (last 7 daily deltas) for UI
    trailing = []
    for i in range(1, min(8, len(net))):
        d = dates[-i].strftime("%d %b %Y")
        v = net[-i] - net[-i-1]
        trailing.append({"date": d, "usd": round(v,2)})
    # normalize: rising liquidity lowers risk. Scale ~ $100B
    base = sma7
    score = clamp(sigmoid(-base / 100_000_000_000.0), 0.0, 1.0)
    contrib = round((score - 0.5) * 0.2, 2)
    return {
        "score": round(score, 2),
        "contribution": contrib,
        "level_usd": round(level, 2),
        "delta1d_usd": round(delta1d, 2),
        "sma7_delta_usd": round(sma7, 2),
        "trailing": trailing,
        "source": "FRED WALCL − WTREGEN − RRPONTSYD"
    }

# ----- base risk (placeholder until full model) -----
prev_risk = 0.35
if latest_path.exists():
    try: prev_risk = float(json.loads(latest_path.read_text()).get("risk", 0.35))
    except Exception: pass
risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

# ----- real ETF flows -----
trail = fetch_etf_trailing(n=7)
etf_usd = trail[0][1] if trail else None
etf_date = trail[0][0] if trail else None
sma7_etf = round(sum(v for _, v in trail)/len(trail), 2) if trail else None
etf_base = sma7_etf if sma7_etf is not None else (etf_usd or 0.0)
etf_score = clamp(sigmoid(-etf_base / 200_000_000.0), 0.0, 1.0)
etf_contrib = round((etf_score - 0.5) * 0.2, 2)

# ----- real Stablecoins -----
sc_today, sc_sma7, sc_trailing = combine_stablecoin_issuance(days=8)
sc_base = sc_sma7 if sc_sma7 is not None else (sc_today or 0.0)
sc_score = clamp(sigmoid(-sc_base / 1_000_000_000.0), 0.0, 1.0)
sc_contrib = round((sc_score - 0.5) * 0.2, 2)

# ----- real Net Liquidity (if key present) -----
netliq = compute_net_liquidity()

# ----- assemble drivers -----
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
    "net_liquidity": netliq or {
        "score": round(random.uniform(0.4,0.7),2),
        "contribution": round(random.uniform(-0.08,0.12),2),
        "level_usd": None, "delta1d_usd": None, "sma7_delta_usd": None,
        "trailing": [], "source": "FRED (pending key)"
    },
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
    try: prev_doc = json.loads(latest_path.read_text())
    except Exception: pass
btc_price = fetch_btc_price_usd() or prev_doc.get("btc_price_usd")

# ----- final doc -----
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
      f"etf_usd={etf_usd} etf_sma7={sma7_etf} sc_today={sc_today} sc_sma7={sc_sma7} "
      f"netliq_level={netliq.get('level_usd') if netliq else None}")
