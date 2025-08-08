// app/assets/app.js
async function main() {
  try {
    const DATA_URL =
      'https://raw.githubusercontent.com/firemansghost/grayghost-risk/main/data/latest.json?ts=' + Date.now();

    const res = await fetch(DATA_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error('Fetch failed: ' + res.status + ' ' + res.statusText);
    const data = await res.json();

    // ===== Top card =====
    const asOfEl = document.getElementById('asOf');
    if (asOfEl) {
      if (data.as_of_utc) {
        const d = new Date(data.as_of_utc);
        asOfEl.textContent = 'Updated ' + d.toUTCString().replace('GMT', 'UTC');
      } else {
        asOfEl.textContent = 'As of ' + (data.as_of ?? '—');
      }
    }

    const riskEl = document.getElementById('riskScore');
    if (riskEl) riskEl.textContent = Number(data.risk ?? NaN).toFixed(2);

    const bandEl = document.getElementById('riskBand');
    if (bandEl) {
      const band = String(data.band ?? 'yellow');
      bandEl.textContent = band.toUpperCase();
      bandEl.classList.add(band);
    }

    // BTC price
    const priceEl = document.getElementById('btcPrice');
    if (priceEl) {
      const p = data.btc_price_usd;
      priceEl.textContent = p ? `BTC $${Number(p).toLocaleString()}` : 'BTC $—';
    }

    // ===== helpers (human-readable USD & %) =====
    const humanUSD = (n) => {
      const x = Number(n);
      if (!isFinite(x)) return '—';
      const ax = Math.abs(x);
      if (ax >= 1e12) return `$${(x / 1e12).toFixed(2)}T`;
      if (ax >= 1e9)  return `$${(x / 1e9).toFixed(2)}B`;
      if (ax >= 1e6)  return `$${(x / 1e6).toFixed(2)}M`;
      return `$${x.toLocaleString()}`;
    };

    const fmtSignedUSD = (v) => {
      const x = Number(v);
      if (!isFinite(x) || Math.abs(x) < 1) return { text: '—', cls: 'neu' };
      const sign = x >= 0 ? '+' : '−';
      return { text: `${sign}${humanUSD(Math.abs(x))}`, cls: x >= 0 ? 'pos' : 'neg' };
    };

    const fmtLevelUSD = (v) => humanUSD(v);
    const fmtPct = (v) => (isFinite(Number(v)) ? `${Number(v).toFixed(2)}%` : '—');
    const fmtPctOrBp = (v) => {
      const n = Number(v);
      if (!isFinite(n)) return '—';
      // show basis points if magnitude < 0.10%
      if (Math.abs(n) < 0.10) return `${(n * 100).toFixed(1)} bp`;
      return `${n.toFixed(2)}%`;
    };

    // ===== Driver gauges =====
    const gauges = document.getElementById('gauges');
    const map = {
      etf_flows: 'ETF Net Flows',
      net_liquidity: 'Global Net Liquidity',
      stablecoins: 'Stablecoin Issuance',
      term_structure: 'Term Structure & Leverage',
      onchain: 'On-chain Value'
    };

    if (gauges && data.drivers) {
      for (const k of Object.keys(map)) {
        const g = data.drivers[k];
        if (!g) continue;

        let extra = '';
        if (k === 'etf_flows') {
          const raw = (typeof data.etf_flow_usd === 'number') ? data.etf_flow_usd : g.raw_usd;
          const sma = (typeof data.etf_flow_sma7_usd === 'number') ? data.etf_flow_sma7_usd : g.sma7_usd;
          const r = fmtSignedUSD(raw), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Today: <span class="${r.cls}">${r.text}</span></div>
            <div class="title">7d Avg: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'stablecoins') {
          const raw = (typeof data.stablecoin_delta_usd === 'number') ? data.stablecoin_delta_usd : g.raw_delta_usd;
          const sma = (typeof data.stablecoin_delta_sma7_usd === 'number') ? data.stablecoin_delta_sma7_usd : g.sma7_delta_usd;
          const r = fmtSignedUSD(raw), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Today: <span class="${r.cls}">${r.text}</span></div>
            <div class="title">7d Avg: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'net_liquidity') {
          const lvl = g.level_usd;
          const d1  = g.delta1d_usd;
          const sma = g.sma7_delta_usd;
          const d1f = fmtSignedUSD(d1), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Level: ${fmtLevelUSD(lvl)}</div>
            <div class="title">Today: <span class="${d1f.cls}">${d1f.text}</span></div>
            <div class="title">7d Avg: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'term_structure') {
          const fAnn = g.funding_ann_pct;
          const prem = g.perp_premium_7d_pct;
          extra = `
            <div class="title">Funding (ann.): ${fmtPct(fAnn)}</div>
            <div class="title">Perp Premium (7d): ${fmtPctOrBp(prem)}</div>
          `;
        }

        const div = document.createElement('div');
        div.className = 'gauge';
        div.innerHTML = `
          <div class="title">${map[k]}</div>
          <div class="value">${(g.score * 100).toFixed(0)}<span style="font-size:12px;"> /100</span></div>
          <div class="title">Contribution: ${(g.contribution >= 0 ? '+' : '') + (g.contribution * 100).toFixed(0)} bp</div>
          ${extra}
        `;
        gauges.appendChild(div);
      }
    }

    // ===== "Why it moved" bars =====
    const contribs = document.getElementById('contribs');
    if (contribs && data.drivers) {
      for (const k of Object.keys(map)) {
        const g = data.drivers[k];
        if (!g) continue;
        const div = document.createElement('div');
        div.className = 'contrib';
        const width = Math.min(100, Math.abs(g.contribution * 1000)); // simple demo scale
        const color = g.contribution >= 0 ? 'var(--orange)' : 'var(--green)';
        div.innerHTML = `
          <div class="title">${map[k]}</div>
          <div class="bar"><span style="width:${width}%; background:${color};"></span></div>
        `;
        contribs.appendChild(div);
      }
    }
  } catch (e) {
    console.error(e);
    const riskEl = document.getElementById('riskScore');
    if (riskEl) riskEl.textContent = 'N/A';
  }
}
main();
