
---

## Quick start

1. **Fork/clone** the repo to your GitHub account.
2. **Vercel** → New Project → Import this repo → set framework = “Other” (static).
3. **GitHub Actions** → ensure `.github/workflows/daily.yml` is present.
4. (Optional) **Secrets**:
   - `FRED_API_KEY` (for Net Liquidity). Free key from https://fred.stlouisfed.org.
5. Manually run: **GitHub → Actions → daily-risk → Run workflow**.
6. Hard refresh the site (Ctrl/Cmd+Shift+R).

---

## Configuration

- **Smoothing window**: backend and UI honor `smooth_days` (default **21**).  
  Edit the constant in `run_daily.py` (look for where `smooth_days` is set/printed), commit, and re-run.

- **Data sources**: all are free/public endpoints. Funding/premium uses exchange fallbacks.

- **Schedule**: tweak cron in `.github/workflows/daily.yml`.

---

## JSON endpoints (public)

- Latest:  
  `https://raw.githubusercontent.com/<your-username>/grayghost-risk/main/data/latest.json`
- Risk history (JSON/CSV):  
  `https://raw.githubusercontent.com/<your-username>/grayghost-risk/main/data/risk_history.json`  
  `https://raw.githubusercontent.com/<your-username>/grayghost-risk/main/data/risk_history.csv`

---

## JSON shape (contract)

```jsonc
{
  "as_of": "YYYY-MM-DD",
  "as_of_utc": "2025-08-10T14:36:21Z",
  "smooth_days": 21,
  "risk": 0.45,
  "band": "green|yellow|red",
  "regime": "liquidity_on|liquidity_off",
  "btc_price_usd": 118354.37,

  // convenience (duplicates from drivers)
  "etf_flow_usd": 655300000.0,
  "etf_flow_sma7_usd": 106228571.43,
  "stablecoin_delta_usd": -69721822.15,
  "stablecoin_delta_sma7_usd": 171180218.89,

  "drivers": {
    "etf_flows": {
      "score": 0.37,
      "contribution": -0.03,
      "raw_usd": 655300000.0,
      "sma7_usd": 106228571.43,
      "trailing": [ { "date": "11 Jan 2024", "usd": 655300000.0 }, ... ],
      "asof": "11 Jan 2024",
      "asof_utc": "optional",
      "source": "Farside Bitcoin ETF Flow – All Data",
      "health": { "status": "ok|stale|down", "age_hours": 12.3 }
    },

    "net_liquidity": {
      "score": 0.49,
      "contribution": 0.00,
      "level_usd": 6.56e12,
      "delta1d_usd": -2.34e9,
      "sma7_delta_usd": 2.19e9,           // name retained even if window != 7
      "trailing": [ { "date": "08 Aug 2025", "usd": -2342000000.0 }, ... ],
      "asof": "08 Aug 2025",
      "asof_utc": "2025-08-08T00:00:00Z",
      "source": "FRED WALCL − WTREGEN − RRPONTSYD (USD)",
      "health": { "status": "ok|stale|down", "age_hours": 48.0 }
    },

    "stablecoins": {
      "score": 0.45,
      "contribution": -0.01,
      "raw_delta_usd": 570365983.53,
      "sma7_delta_usd": 188200480.81,
      "trailing": [ { "date": "08 Aug 2025", "usd": 570365983.53 }, ... ],
      "source": "CoinGecko USDT + USDC market_caps (daily)",
      "health": { "status": "ok|stale|down", "age_hours": 6.0 }
    },

    "term_structure": {
      "score": 0.41,
      "contribution": -0.02,
      "funding_ann_pct": 6.53,
      "funding_8h_pct": 0.006,
      "perp_premium_now_pct": 0.021,      // may be tiny; UI shows bp
      "perp_premium_7d_pct": 0.018,
      "source": "Binance/OKX/BitMEX/Bybit (fallback)",
      "health": { "status": "ok|stale|down", "age_hours": 0.2 }
    },

    "onchain": {
      "score": 0.67,
      "contribution": 0.08,
      "trailing": [ ...optional... ],
      "source": "free proxies",
      "health": { "status": "ok|stale|down", "age_hours": 0.2 }
    }
  }
}
