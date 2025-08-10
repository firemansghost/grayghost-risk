# GrayGhost Risk (MVP)

A clean, fast, browser-based dashboard for **multi-month Bitcoin risk** using free/cheap data.  
Daily Python job builds JSON; a static site on Vercel renders pretty gauges, sparklines, source health, and a banded risk history.

**Live:** https://grayghost-risk.vercel.app/  
**Repo:** https://github.com/firemansghost/grayghost-risk

---

## Features

- **Today’s Risk** with green/yellow/red band
- **BTC Price** (spot)
- **Driver cards** (score + contribution) with:
  - Color-coded **Today** and **Avg (window)** numbers
  - **Tiny sparklines** with hover tooltips
  - **Source + timestamp + colored health dot** (ok/stale/down)
- **Risk History** strip (with green/yellow/red bands) + **Download CSV**
- **Daily JSON build** with history snapshots committed to `/data/`

### Drivers (v1)
- **ETF Net Flows** (Farside “All data”)
- **Global Net Liquidity** (FRED: WALCL − WTREGEN − RRPONTSYD)
- **Stablecoin Issuance** (USDT + USDC market cap deltas, CoinGecko)
- **Term Structure & Leverage** (funding + perp premium across Binance/OKX/BitMEX/Bybit)
- **On-chain (free proxies)**: activity/fees/mempool (placeholder-friendly)

> Smoothing window: currently **21d** (multi-week swings). Exposed as `smooth_days` in the JSON.

---

## How it works

- **Backend**: `pipelines/run_daily.py` (Python stdlib) fetches sources, computes driver scores & contributions, writes:
  - `data/latest.json`
  - `data/history/YYYY-MM-DD.json`
  - `data/risk_history.json` and `data/risk_history.csv`
- **Frontend**: `/app` is a static site (vanilla HTML/CSS/JS) deployed to Vercel.
  - `app/assets/app.js` fetches `latest.json` from GitHub raw and renders the UI.
  - `app/assets/style.css` holds the theme, gauges, sparklines, and history styles.

---

## Repository layout


© 2025 GrayGhost
