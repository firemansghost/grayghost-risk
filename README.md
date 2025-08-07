# GrayGhost Risk (MVP)

Bold, browser-based BTC risk meter with daily updates at **07:00 CT**. Static front-end + GitHub Actions pipeline. No servers.

## Structure
- `app/` — static site (HTML/CSS/JS) pulling `data/latest.json`
- `data/` — generated JSON (latest + history)
- `pipelines/` — Python scripts for daily computation (dummy now)
- `.github/workflows/daily.yml` — cron to run the pipeline and commit data

## Quick Start
1. **Create repo** on GitHub and push these files.
2. Enable **GitHub Pages** (serve `/app` if using Pages) *or* deploy `/app` to **Vercel**.
3. The Action runs daily and updates `data/latest.json` with a dummy risk (until you wire real data).

## Email Alerts (optional)
Set these repository **Secrets** if you want email when the band flips:
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
- `SMTP_FROM` (e.g., no-reply@yourdomain.com)
- `ALERT_EMAILS` (comma-separated, e.g., `bobbyedwards@live.com,firemansghost@gmail.com`)

## TODO (next sprints)
- Replace dummy generator with real pipeline:
  - ETF flows, liquidity proxy (FRED), stablecoin issuance, term-structure, on-chain slow metrics.
- Regime rules + weight blending, backtest report.
- Pretty charts (heatmap, driver gauges) — currently minimal but styled.
- PWA install + CSV/PNG export.

---
© 2025 GrayGhost
