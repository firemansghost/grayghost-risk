import json, os, random, datetime, math, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

latest_path = DATA / "latest.json"
as_of = datetime.date.today().isoformat()

# Load yesterday if exists to create smoother dummy
prev_risk = 0.35
if latest_path.exists():
    try:
        prev = json.loads(latest_path.read_text())
        prev_risk = float(prev.get("risk", prev_risk))
    except Exception:
        pass

# drift risk gently
risk = max(0.0, min(1.0, prev_risk + random.uniform(-0.03, 0.03)))

band = "green" if risk < 0.25 else ("red" if risk > 0.60 else "yellow")
regime = "liquidity_on" if random.random() > 0.4 else "liquidity_off"

drivers = {
    "etf_flows": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-300_000_000, 600_000_000)},
    "net_liquidity": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-80_000_000_000, 80_000_000_000)},
    "stablecoins": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": random.randint(-2_000_000_000, 3_000_000_000)},
    "term_structure": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw_basis_pct": round(random.uniform(-5, 12),2)},
    "onchain": {"score": round(random.uniform(0.2,0.8),2), "contribution": round(random.uniform(-0.08,0.12),2), "raw": f"LTH_SOPR={round(random.uniform(0.92,1.08),2)}"}
}

doc = {"as_of": as_of, "risk": round(risk,2), "band": band, "regime": regime, "drivers": drivers}
(DATA / "latest.json").write_text(json.dumps(doc, indent=2))

# Append to history
hist_dir = DATA / "history"
hist_dir.mkdir(exist_ok=True)
(hist_dir / f"{as_of}.json").write_text(json.dumps(doc, indent=2))

# Write band_state for alerts
state_file = DATA / "band_state.txt"
state_file.write_text(band)
print(f"Wrote {latest_path} with risk={risk:.2f}, band={band}")
