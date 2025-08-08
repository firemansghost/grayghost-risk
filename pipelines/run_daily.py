import json, random, datetime, pathlib, urllib.request, urllib.error, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
HIST = DATA / "history"; HIST.mkdir(parents=True, exist_ok=True)

as_of = datetime.date.today().isoformat()
latest_path = DATA / "latest.json"

def get_prev_doc():
    try:
        if latest_path.exists():
            return json.loads(latest_path.read_text())
    except Exception as e:
        print(f"[run_daily] WARN could not read prev latest.json: {e}", file=sys.stderr)
    return {}

def clamp(x, lo=0.0, hi=1.0): return max(lo, min(hi, x))

def fetch_btc_price_usd():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"gh-actions/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.load(resp)
            return round(float(payload["data"]["amount"]), 2)
    except urllib.error.HTTPError as e:
        print(f"[run_daily] WARN HTTP {e.code} from Coinbase", file=sys.stderr)
    except Exception as e:
        print(f"[run_daily] WARN price fetch failed: {e}", file=sys.stderr)
    return None

try:
    prev = get_prev_doc()
    prev_risk = float(prev.get("risk", 0.35))

    risk = clamp(prev_risk + random.uniform(-0.03, 0.03))
    band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
    regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

    drivers = {
        "etf_flows":      {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-300_000_000, 600_000_000)},
        "net_liquidity":  {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-80_000_000_000, 80_000_000_000)},
        "stablecoins":    {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-2_000_000_000, 3_000_000_000)},
        "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": round(random.uniform(-5,12),2)},
        "onchain":        {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": f"LTH_SOPR={round(random.uniform(0.92,1.08),2)}"}
    }

    btc_price = fetch_btc_price_usd() or prev.get("btc_price_usd")

    doc = {
        "as_of": as_of,
        "risk": round(risk, 2),
        "band": band,
        "regime": regime,
        "btc_price_usd": btc_price,
        "drivers": drivers
    }

    latest_path.write_text(json.dumps(doc, indent=2))
    (HIST / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

    print(f"[run_daily] OK risk={risk:.2f} band={band} btc={btc_price}")
except Exception as e:
    print(f"[run_daily] FATAL {e}", file=sys.stderr)
    fallback = {"as_of": as_of, "risk": 0.35, "band":"yellow","regime":"liquidity_on","btc_price_usd": None, "drivers": {}}
    latest_path.write_text(json.dumps(fallback, indent=2))
    (HIST / f"{as_of}.json").write_text(json.dumps(fallback, indent=2))
