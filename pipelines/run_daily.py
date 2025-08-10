# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request, urllib.error, sys, re, math, os

# ====== CONFIG ======
WEEKLY_MODE   = True          # A) daily runs, slower-moving risk
SMOOTH_DAYS   = 21 if WEEKLY_MODE else 7
EMA_KEEP      = 0.85 if WEEKLY_MODE else 0.60    # risk = KEEP*prev + (1-KEEP)*instant
# Driver weights (sum ~1)
WEIGHTS_WEEKLY = {
    "etf_flows":     0.22,
    "net_liquidity": 0.30,
    "stablecoins":   0.22,
    "term_structure":0.12,
    "onchain":       0.14,
}
WEIGHTS_DAILY = {
    "etf_flows":     0.24,
    "net_liquidity": 0.26,
    "stablecoins":   0.22,
    "term_structure":0.16,
    "onchain":       0.12,
}
WEIGHTS = WEIGHTS_WEEKLY if WEEKLY_MODE else WEIGHTS_DAILY

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
as_of_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
latest_path = DATA / "latest.json"

# ----- utils -----
def clamp(x, lo=0.0, hi=1.0): return max(lo, min(hi, x))

def http_get(url, timeout=20, headers=None):
    if headers is None:
        headers = {"User-Agent": "gh-actions/1.0"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def http_json(url, timeout=20, headers=None):
    return json.loads(http_get(url, timeout=timeout, headers=headers))

def sigmoid(x):
    try: return 1.0 / (1.0 + math.exp(-x))
    except OverflowError: return 0.0 if x < 0 else 1.0

# ----- BTC price -----
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
def fetch_stablecoin_caps(coin_id, days=8):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={days}&interval=daily"
    try:
        caps = http_json(url, timeout=20).get("market_caps", [])
        return [(int(ts), float(val)) for ts, val in caps if val is not None]
    except Exception as e:
        print(f"[run_daily] WARN CG fetch {coin_id} failed: {e}", file=sys.stderr)
        return []

def combine_stablecoin_issuance(window=7):
    need_days = window + 2
    teth = fetch_stablecoin_caps("tether", need_days)
    usdc = fetch_stablecoin_caps("usd-coin", need_days)
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
    lastW = deltas[-window:]
    today = lastW[-1][1] if lastW else None
    smaW = round(sum(v for _, v in lastW)/len(lastW), 2) if lastW else None
    trail = []
    for ts, v in reversed(lastW):  # most recent first
        d = datetime.datetime.utcfromtimestamp(ts/1000).strftime("%d %b %Y")
        trail.append({"date": d, "usd": round(v,2)})
    return (round(today,2) if today is not None else None, smaW, trail)

# ----- FRED (Net Liquidity) -----
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()

def fetch_fred_series(series_id, days=180):
    if not FRED_API_KEY:
        print("[run_daily] INFO no FRED_API_KEY, skipping Net Liquidity", file=sys.stderr)
        return []
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
        return out
    except Exception as e:
        print(f"[run_daily] WARN FRED {series_id} failed: {e}", file=sys.stderr)
        return []

def scale_series(series_id, pairs):
    if series_id in ("WALCL", "WTREGEN"):   # millions USD → dollars
        factor = 1_000_000.0
    elif series_id == "RRPONTSYD":          # billions USD → dollars
        factor = 1_000_000_000.0
    else:
        factor = 1.0
    return [(d, v*factor) for d, v in pairs]

def compute_net_liquidity(window=7):
    walcl_raw = fetch_fred_series("WALCL", days=180)
    tga_raw   = fetch_fred_series("WTREGEN", days=180)
    rrp_raw   = fetch_fred_series("RRPONTSYD", days=180)
    if not walcl_raw or not tga_raw or not rrp_raw:
        return None

    walcl = scale_series("WALCL", walcl_raw)
    tga   = scale_series("WTREGEN", tga_raw)
    rrp   = scale_series("RRPONTSYD", rrp_raw)

    start = max(min(walcl[0][0], tga[0][0], rrp[0][0]),
                datetime.date.today() - datetime.timedelta(days=120))
    dates = [start + datetime.timedelta(days=i)
             for i in range((datetime.date.today() - start).days + 1)]

    def ffill(pairs):
        mp = {d: v for d, v in pairs}
        out, last = [], None
        for d in dates:
            if d in mp:
                last = mp[d]
            out.append(last)
        return out

    f_w, f_t, f_r = ffill(walcl), ffill(tga), ffill(rrp)
    net = [None if (f_w[i] is None or f_t[i] is None or f_r[i] is None)
           else f_w[i] - f_t[i] - f_r[i] for i in range(len(dates))]

    first = next((i for i, x in enumerate(net) if x is not None), None)
    if first is None:
        return None
    dates, net = dates[first:], net[first:]

    level = net[-1]
    # window-safe average change
    N = max(2, min(int(window or 7), len(net) - 1))
    deltaN = level - net[-N]
    smaN = deltaN / N

    trailing = []
    for i in range(1, min(8, len(net))):
        d = dates[-i].strftime("%d %b %Y")
        trailing.append({"date": d, "usd": round(net[-i] - net[-i-1], 2)})

    score = clamp(sigmoid(-smaN / 100_000_000_000.0), 0.0, 1.0)  # more liq → lower risk
    contrib = round((score - 0.5) * 0.2, 2)

    asof_date = dates[-1]
    return {
        "score": round(score, 2),
        "contribution": contrib,
        "level_usd": round(level, 2),
        "delta1d_usd": round(level - net[-2], 2) if len(net) >= 2 else 0.0,
        "sma7_delta_usd": round(smaN, 2),  # name kept for UI compatibility
        "trailing": trailing,
        "asof": asof_date.strftime("%d %b %Y"),
        "asof_utc": f"{asof_date.isoformat()}T00:00:00Z",
        "source": "FRED WALCL − WTREGEN − RRPONTSYD (USD)"
    }

# ----- Term Structure & Leverage -----
def fetch_binance_funding_7d_annual_pct():
    try:
        j = http_json("https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1000", timeout=20)
        rates = [float(x.get("fundingRate", 0.0)) for x in j][-21:] if isinstance(j, list) else []
        rates = [r for r in rates if abs(r) > 1e-10]
        if rates:
            avg_8h = sum(rates)/len(rates)
            return avg_8h*100.0, avg_8h*3*365*100.0
    except Exception as e:
        print(f"[run_daily] WARN funding binance hist failed: {e}", file=sys.stderr)
    try:
        now = http_json("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT", timeout=20)
        last = float(now.get("lastFundingRate", 0.0))
        if abs(last) > 1e-10:
            return last*100.0, last*3*365*100.0
    except Exception as e:
        print(f"[run_daily] WARN funding binance fallback failed: {e}", file=sys.stderr)
    return None, None

def fetch_okx_funding_7d_annual_pct():
    try:
        hdr = {"User-Agent":"gh-actions/1.0"}
        j = http_json("https://www.okx.com/api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=100",
                      headers=hdr, timeout=20)
        arr = j.get("data", []) if isinstance(j, dict) else []
        rates = [float(x.get("fundingRate", 0.0)) for x in arr][-21:]
        rates = [r for r in rates if abs(r) > 1e-10]
        if rates:
            avg_8h = sum(rates)/len(rates)
            return avg_8h*100.0, avg_8h*3*365*100.0
    except Exception as e:
        print(f"[run_daily] WARN funding okx hist failed: {e}", file=sys.stderr)
    try:
        hdr = {"User-Agent":"gh-actions/1.0"}
        j = http_json("https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP",
                      headers=hdr, timeout=20)
        arr = j.get("data", []) if isinstance(j, dict) else []
        if arr:
            last = float(arr[0].get("fundingRate", 0.0))
            if abs(last) > 1e-10:
                return last*100.0, last*3*365*100.0
    except Exception as e:
        print(f"[run_daily] WARN funding okx fallback failed: {e}", file=sys.stderr)
    return None, None

def fetch_bitmex_funding_7d_annual_pct():
    try:
        j = http_json("https://www.bitmex.com/api/v1/funding?symbol=XBTUSD&count=100&reverse=true", timeout=20)
        rates = [float(x.get("fundingRate", 0.0)) for x in j][:21] if isinstance(j, list) else []
        rates = [r for r in rates if abs(r) > 1e-10]
        if rates:
            avg_8h = sum(rates)/len(rates)
            return avg_8h*100.0, avg_8h*3*365*100.0
    except Exception as e:
        print(f"[run_daily] WARN funding bitmex failed: {e}", file=sys.stderr)
    return None, None

def fetch_okx_premium_now_pct():
    try:
        hdr = {"User-Agent":"gh-actions/1.0"}
        j = http_json("https://www.okx.com/api/v5/public/mark-price?instId=BTC-USDT-SWAP",
                      headers=hdr, timeout=20)
        arr = j.get("data", []) if isinstance(j, dict) else []
        if arr:
            mark = float(arr[0].get("markPx"))
            index = float(arr[0].get("indexPx"))
            if index:
                return (mark - index) / index * 100.0
    except Exception as e:
        print(f"[run_daily] WARN okx premium now failed: {e}", file=sys.stderr)
    return None

def fetch_bybit_premium_now_pct():
    try:
        j = http_json("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT", timeout=20)
        root = (j.get("result") or j.get("data") or {})
        lst = root.get("list") or []
        if lst:
            it = lst[0]
            mark = float(it.get("markPrice"))
            index = float(it.get("indexPrice"))
            if index:
                return (mark - index) / index * 100.0
    except Exception as e:
        print(f"[run_daily] WARN bybit premium now failed: {e}", file=sys.stderr)
    return None

def fetch_deribit_premium_now_pct():
    try:
        j = http_json("https://deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL", timeout=20)
        res = j.get("result", {})
        mark = float(res.get("mark_price"))
        index = float(res.get("index_price"))
        if index:
            return (mark - index) / index * 100.0
    except Exception as e:
        print(f"[run_daily] WARN deribit premium now failed: {e}", file=sys.stderr)
    return None

def fetch_proxy_premium_now_pct():
    try:
        fut = float(http_json("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT", timeout=15).get("price"))
        spot = fetch_btc_price_usd()
        if spot:
            return (fut - spot) / spot * 100.0
    except Exception as e:
        print(f"[run_daily] WARN proxy premium failed: {e}", file=sys.stderr)
    return None

def get_premium_now_pct_multi():
    try:
        now = http_json("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT", timeout=20)
        mark = float(now.get("markPrice")); index = float(now.get("indexPrice"))
        if index: return (mark - index) / index * 100.0
    except Exception as e:
        print(f"[run_daily] WARN premium now binance failed: {e}", file=sys.stderr)
    for fn in (fetch_okx_premium_now_pct, fetch_bybit_premium_now_pct, fetch_deribit_premium_now_pct, fetch_proxy_premium_now_pct):
        v = fn()
        if v is not None: return v
    return None

def fetch_binance_premium_7d_avg_pct():
    try:
        arr = http_json("https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT&interval=1h&limit=168", timeout=20)
        closes = [float(x[4]) for x in arr] if isinstance(arr, list) else []
        closes = [c for c in closes if abs(c) > 1e-12]
        if closes:
            return (sum(closes)/len(closes)) * 100.0
    except Exception as e:
        print(f"[run_daily] WARN premium klines binance failed: {e}", file=sys.stderr)
    return None

def compute_term_structure_driver():
    f8, fann = fetch_binance_funding_7d_annual_pct()
    if fann is None:
        f8, fann = fetch_okx_funding_7d_annual_pct()
    if fann is None:
        f8, fann = fetch_bitmex_funding_7d_annual_pct()

    prem_now = get_premium_now_pct_multi()
    prem_7d  = fetch_binance_premium_7d_avg_pct()
    if prem_7d is None:
        prem_7d = prem_now

    parts = []
    if fann is not None:
        parts.append(sigmoid((fann - 10.0) / 10.0))     # 10% ann ~ neutral
    if prem_7d is not None:
        parts.append(sigmoid((prem_7d - 0.00) / 0.20))  # +0.20% premium ~ riskier
    if not parts:
        return {
            "score": round(random.uniform(0.3,0.7),2),
            "contribution": round(random.uniform(-0.08,0.12),2),
            "funding_ann_pct": None, "funding_8h_pct": None,
            "perp_premium_now_pct": None, "perp_premium_7d_pct": None,
            "source": "Binance/OKX/BitMEX/Bybit/Deribit/Proxy"
        }

    score = clamp(sum(parts)/len(parts), 0.0, 1.0)
    contrib = round((score - 0.5) * (0.18 if WEEKLY_MODE else 0.2), 2)

    return {
        "score": round(score, 2),
        "contribution": contrib,
        "funding_ann_pct": None if fann is None else round(fann, 2),
        "funding_8h_pct": None if f8 is None else round(f8, 4),
        "perp_premium_now_pct": None if prem_now is None else round(prem_now, 3),
        "perp_premium_7d_pct": None if prem_7d is None else round(prem_7d, 3),
        "source": "Binance/OKX/BitMEX/Bybit/Deribit/Proxy"
    }

# ----- On-chain (free: blockchain.com + mempool.space) -----
def fetch_blockchain_chart(name: str, days: int = 220):
    url = f"https://api.blockchain.info/charts/{name}?timespan={days}days&format=json"
    try:
        j = http_json(url, timeout=20)
        vals = j.get("values", [])
        out = []
        for it in vals:
            ts = it.get("x"); y = it.get("y")
            if ts is None or y is None: continue
            d = datetime.datetime.utcfromtimestamp(int(ts)).date()
            out.append((d, float(y)))
        return out
    except Exception as e:
        print(f"[run_daily] WARN blockchain.com {name} failed: {e}", file=sys.stderr)
        return []

def fetch_mempool_summary():
    size_mb = None; fee_30m = None
    try:
        j = http_json("https://mempool.space/api/mempool", timeout=15)
        vsize = float(j.get("vsize", 0.0))
        size_mb = vsize / 1_000_000.0
    except Exception as e:
        print(f"[run_daily] WARN mempool size failed: {e}", file=sys.stderr)
    try:
        f = http_json("https://mempool.space/api/v1/fees/recommended", timeout=15)
        fee_30m = float(f.get("halfHourFee", f.get("fastestFee", None)))
    except Exception as e:
        print(f"[run_daily] WARN fee rec failed: {e}", file=sys.stderr)
    return size_mb, fee_30m

def compute_onchain_driver(btc_price_usd: float, window: int = SMOOTH_DAYS):
    addrs = fetch_blockchain_chart("n-unique-addresses", 220)
    fees  = fetch_blockchain_chart("transaction-fees", 220)   # BTC/day
    txs   = fetch_blockchain_chart("n-transactions", 220)
    hrate = fetch_blockchain_chart("hash-rate", 220)

    if not addrs or not fees:
        return {
            "score": round(random.uniform(0.3,0.7),2),
            "contribution": round(random.uniform(-0.08,0.12),2),
            "trailing": [],
            "source": "blockchain.com charts (fallback)"
        }

    # map to dict by date; intersect where possible
    ad_map = {d:v for d,v in addrs}
    fe_map = {d:v for d,v in fees}
    tx_map = {d:v for d,v in txs} if txs else {}
    hr_map = {d:v for d,v in hrate} if hrate else {}
    common = sorted(set(ad_map.keys()) & set(fe_map.keys()))
    if len(common) < window + 10:
        common = sorted(ad_map.keys())

    ad_vals = [ad_map[d] for d in common]
    fe_btc  = [fe_map.get(d, fe_map[common[-1]]) for d in common]
    tx_vals = [tx_map.get(d, tx_map.get(common[-1], 0.0)) for d in common] if tx_map else [0.0]*len(common)
    hr_vals = [hr_map.get(d, hr_map.get(common[-1], 0.0)) for d in common] if hr_map else [0.0]*len(common)

    # baselines (≈180d) vs window avgs
    base_len = min(180, len(ad_vals)) or len(ad_vals)
    ad_base = sum(ad_vals[-base_len:]) / base_len
    fe_base = sum(fe_btc[-base_len:]) / base_len
    tx_base = (sum(tx_vals[-base_len:]) / base_len) if txs else 0.0

    ad_avgW = sum(ad_vals[-window:]) / window
    fe_avgW = sum(fe_btc[-window:]) / window
    tx_avgW = (sum(tx_vals[-window:]) / window) if txs else 0.0

    # hash momentum: (SMA_window - SMA_90)/SMA_90
    hr_mom = 0.0
    if any(hr_vals):
        look = min(90, len(hr_vals))
        sma_long = sum(hr_vals[-look:]) / look if look else 0.0
        sma_short = sum(hr_vals[-window:]) / window
        if sma_long:
            hr_mom = (sma_short - sma_long) / sma_long

    # deviations vs baseline
    dev_ad = 0.0 if ad_base == 0 else (ad_avgW - ad_base) / ad_base
    dev_fe = 0.0 if fe_base == 0 else (fe_avgW - fe_base) / fe_base
    dev_tx = 0.0 if tx_base == 0 else (tx_avgW - tx_base) / tx_base

    # blend: addr 40% + tx 20% + fees 20% + hash 20%  (higher → lower risk)
    dev = 0.4*dev_ad + 0.2*dev_tx + 0.2*dev_fe + 0.2*hr_mom
    score = clamp(sigmoid(-dev / 0.5), 0.0, 1.0)
    contrib = round((score - 0.5) * 0.2, 2)

    # trailing sparkline: fees converted to USD-ish (visual only)
    bp = btc_price_usd or 0.0
    trail = []
    lastW_dates = common[-window:]
    for d in reversed(lastW_dates):  # most recent first
        fee_btc = fe_map.get(d, 0.0)
        usd = round(fee_btc * bp, 2) if bp else round(fee_btc, 6)
        trail.append({"date": d.strftime("%d %b %Y"), "usd": usd})

    # mempool
    mem_mb, fee30 = fetch_mempool_summary()

    return {
        "score": round(score, 2),
        "contribution": contrib,
        "addr_today": round(ad_vals[-1], 0),
        "addr_avg_w": round(ad_avgW, 0),
        "tx_today": round(tx_vals[-1], 0) if txs else None,
        "tx_avg_w": round(tx_avgW, 0) if txs else None,
        "fee_usd_today": round((fe_btc[-1] * (btc_price_usd or 0.0)), 2) if btc_price_usd else round(fe_btc[-1], 6),
        "fee_usd_avg_w": round((fe_avgW * (btc_price_usd or 0.0)), 2) if btc_price_usd else round(fe_avgW, 6),
        "hash_mom_pct": round(hr_mom*100.0, 2) if any(hr_vals) else None,
        "mempool_vsize_mb": round(mem_mb, 2) if mem_mb is not None else None,
        "mempool_halfhour_satvb": round(fee30, 0) if fee30 is not None else None,
        "trailing": trail,
        "source": "blockchain.com (addr/tx/fees/hash) + mempool.space"
    }

# ===== compute drivers =====
trail = fetch_etf_trailing(n=SMOOTH_DAYS)
etf_usd  = trail[0][1] if trail else None
etf_date = trail[0][0] if trail else None
sma_etf  = round(sum(v for _, v in trail)/len(trail), 2) if trail else None
etf_base = sma_etf if sma_etf is not None else (etf_usd or 0.0)
etf_score = clamp(sigmoid(-etf_base / 200_000_000.0), 0.0, 1.0)
etf_contrib = round((etf_score - 0.5) * 0.2, 2)

sc_today, sc_smaW, sc_trailing = combine_stablecoin_issuance(window=SMOOTH_DAYS)
sc_base = sc_smaW if sc_smaW is not None else (sc_today or 0.0)
sc_score = clamp(sigmoid(-sc_base / 1_000_000_000.0), 0.0, 1.0)
sc_contrib = round((sc_score - 0.5) * 0.2, 2)

netliq = compute_net_liquidity(window=SMOOTH_DAYS)
term   = compute_term_structure_driver()

# BTC price before on-chain USD conversions
prev_doc = {}
if latest_path.exists():
    try: prev_doc = json.loads(latest_path.read_text())
    except Exception: pass
btc_price = fetch_btc_price_usd() or prev_doc.get("btc_price_usd")

onchain = compute_onchain_driver(btc_price, window=SMOOTH_DAYS)

drivers = {
    "etf_flows": {
        "score": round(etf_score, 2),
        "contribution": etf_contrib,
        "raw_usd": etf_usd,
        "sma7_usd": sma_etf,            # window avg
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
        "sma7_delta_usd": sc_smaW,
        "trailing": sc_trailing,
        "source": "CoinGecko USDT + USDC market_caps (daily)"
    },
    "term_structure": term,
    "onchain": onchain
}

# ---- per-driver freshness/health ----
def _parse_dmy(s):
    try:
        return datetime.datetime.strptime(s, "%d %b %Y").date()
    except Exception:
        return None

def add_health(d, kind, asof_str=None, asof_utc=None):
    """
    kind: 'daily' (expect <=72h fresh) or 'intraday' (<=6h fresh)
    adds: d['health'] = {status: ok|stale|down, age_hours: float}
          and normalizes asof/asof_utc if missing.
    """
    if not isinstance(d, dict):
        return

    # choose a timestamp
    dt_utc = None
    if asof_utc:
        try:
            dt_utc = datetime.datetime.fromisoformat(asof_utc.replace("Z", "+00:00"))
        except Exception:
            dt_utc = None
    elif asof_str:
        dt = _parse_dmy(asof_str)
        if dt:
            dt_utc = datetime.datetime(dt.year, dt.month, dt.day, tzinfo=datetime.timezone.utc)

    age_hours = None
    if dt_utc:
        age_hours = (datetime.datetime.now(datetime.timezone.utc) - dt_utc).total_seconds() / 3600.0

    status = "down"
    if age_hours is not None:
        thr = 6 if kind == "intraday" else 72
        status = "ok" if age_hours <= thr else "stale"

    if asof_str and not d.get("asof"):
        d["asof"] = asof_str
    if dt_utc and not d.get("asof_utc"):
        d["asof_utc"] = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    d["health"] = {"status": status, "age_hours": None if age_hours is None else round(age_hours, 1)}

# ETF (daily)
add_health(drivers.get("etf_flows"), "daily", asof_str=drivers["etf_flows"].get("asof"))

# Net liquidity (daily) — compute_net_liquidity() already returns asof/asof_utc
if drivers.get("net_liquidity"):
    add_health(
        drivers["net_liquidity"], "daily",
        asof_str=drivers["net_liquidity"].get("asof"),
        asof_utc=drivers["net_liquidity"].get("asof_utc")
    )

# Stablecoins (daily) — use most recent trailing date if present
_sc_asof = None
try:
    _sc_asof = drivers["stablecoins"]["trailing"][0]["date"]
except Exception:
    pass
add_health(drivers.get("stablecoins"), "daily", asof_str=_sc_asof)

# Term structure & On-chain (intraday)
add_health(drivers.get("term_structure"), "intraday", asof_utc=as_of_utc)
add_health(drivers.get("onchain"), "intraday", asof_utc=as_of_utc)

# ----- risk: weighted blend + EMA smoothing -----
prev_risk = None
if latest_path.exists():
    try:
        prev_risk = float(json.loads(latest_path.read_text()).get("risk", None))
    except Exception:
        pass

def get_score(key):
    d = drivers.get(key, {})
    return float(d.get("score", 0.5))

inst = (
    WEIGHTS["etf_flows"]     * get_score("etf_flows") +
    WEIGHTS["net_liquidity"] * get_score("net_liquidity") +
    WEIGHTS["stablecoins"]   * get_score("stablecoins") +
    WEIGHTS["term_structure"]* get_score("term_structure") +
    WEIGHTS["onchain"]       * get_score("onchain")
)
risk = inst if prev_risk is None else (EMA_KEEP * prev_risk + (1.0 - EMA_KEEP) * inst)
risk = clamp(risk)

band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if get_score("net_liquidity") < 0.5 else "liquidity_off"

doc = {
    "as_of": as_of,
    "as_of_utc": as_of_utc,
    "smooth_days": SMOOTH_DAYS,
    "risk": round(risk, 2),
    "band": band,
    "regime": regime,
    "btc_price_usd": btc_price,

    # convenience root fields for UI
    "etf_flow_usd": etf_usd,
    "etf_flow_sma7_usd": sma_etf,               # window avg
    "stablecoin_delta_usd": sc_today,
    "stablecoin_delta_sma7_usd": sc_smaW,       # window avg

    # full drivers
    "drivers": drivers
}

# write latest + dated snapshot
latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

# ----- build risk history files (last ~2 years) -----
def build_history(max_days=730):
    rows = []
    for p in sorted(HIST.glob("*.json")):
        try:
            j = json.loads(p.read_text())
            rows.append({
                "date": j.get("as_of") or p.stem,
                "as_of_utc": j.get("as_of_utc"),
                "risk": float(j.get("risk", "nan")),
                "band": j.get("band"),
                "btc_price_usd": j.get("btc_price_usd")
            })
        except Exception:
            continue
    # keep only valid + most recent
    rows = [r for r in rows if isinstance(r.get("risk"), float) and math.isfinite(r["risk"])]
    rows = rows[-max_days:]

    # JSON
    (DATA / "risk_history.json").write_text(json.dumps(rows, indent=2))

    # CSV
    lines = ["date,as_of_utc,risk,band,btc_price_usd"]
    for r in rows:
        bp = "" if r.get("btc_price_usd") is None else str(r["btc_price_usd"])
        lines.append(f'{r["date"]},{r.get("as_of_utc","")},{r["risk"]:.4f},{r.get("band","")},{bp}')
    (DATA / "risk_history.csv").write_text("\n".join(lines))

# write latest + dated snapshot
latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

# ----- build risk history files (last ~2 years) -----
def build_history(max_days=730):
    rows = []
    for p in sorted(HIST.glob("*.json")):
        try:
            j = json.loads(p.read_text())
            rows.append({
                "date": j.get("as_of") or p.stem,
                "as_of_utc": j.get("as_of_utc"),
                "risk": float(j.get("risk", "nan")),
                "band": j.get("band"),
                "btc_price_usd": j.get("btc_price_usd")
            })
        except Exception:
            continue
    # keep only valid + most recent
    rows = [r for r in rows if isinstance(r.get("risk"), float) and math.isfinite(r["risk"])]
    rows = rows[-max_days:]

    # JSON
    (DATA / "risk_history.json").write_text(json.dumps(rows, indent=2))

    # CSV
    lines = ["date,as_of_utc,risk,band,btc_price_usd"]
    for r in rows:
        bp = "" if r.get("btc_price_usd") is None else str(r["btc_price_usd"])
        lines.append(f'{r["date"]},{r.get("as_of_utc","")},{r["risk"]:.4f},{r.get("band","")},{bp}')
    (DATA / "risk_history.csv").write_text("\n".join(lines))

build_history()
print(f"[run_daily] history rows={sum(1 for _ in HIST.glob('*.json'))} -> risk_history.json/csv written")

# final log line
print(
    f"[run_daily] OK risk={risk:.3f} inst={inst:.3f} band={band} "
    f"smooth_days={SMOOTH_DAYS} ema_keep={EMA_KEEP} "
    f"term_fund_ann={drivers['term_structure'].get('funding_ann_pct')} "
    f"term_prem_7d={drivers['term_structure'].get('perp_premium_7d_pct')} "
    f"asof_utc={as_of_utc}"
)
