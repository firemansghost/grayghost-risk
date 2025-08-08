# pipelines/run_daily.py
import json, random, datetime, pathlib, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"
HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

# --- helpers ---------------------------------------------------------------
def get_prev_doc():
    if latest_path.exists():
        try:
            return json.loads(latest_path.read_text())
        except Exception:
            pass
    return {}

def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))

def fetch_btc_price_usd():
    """
    Fetch BTC-USD spot price from Coinbase (no API key).
    Uses stdlib urllib to avoid pip installs.
    """
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            payload = json.load(resp)
            amt = float(payload["data"]["amount"])
            return round(amt, 2)
    except Exception:
        return None

# --- previous state --------------------------------------------------------
prev = get_prev_doc()
prev_risk = float(prev.get("risk", 0.35))

# --- dummy risk drift (MVP placeholder) ------------------------------------
risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

drivers = {
    "etf_flows":      {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-300_000_000, 600_000_000)},
    "net_liquidity":  {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-80_000_000_000, 80_000_000_000)},
    "stablecoins":    {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-2_000_000_000, 3_000_000_000)},
    "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": round(random.uniform(-5, 12),2)},
    "onchain":        {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": f"LTH_SOPR={round(random.uniform(0.92,1.08),2)}"}
}

# --- BTC price -------------------------------------------------------------
btc_price = fetch_btc_price_usd()
if btc_price is None:
    # fall back to yesterday's value if available
    btc_price = prev.get("btc_price_usd")

# --- assemble document -----------------------------------------------------
doc = {
    "as_of": as_of,
    "risk": round(risk, 2),
    "band": band,
    "regime": regime,
    "btc_price_usd": btc_price,
    "drivers": drivers
}

# --- write outputs ---------------------------------------------------------
latest_path.write_text(json.dumps(doc, indent=2))
(HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

print(f"[run_daily] Wrote {latest_path} with risk={risk:.2f}, band={band}, btc=${btc_price}")
