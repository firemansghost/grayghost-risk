// app/assets/app.js  (only the relevant bits shown changed)
async function main() {
  try {
    const DATA_URL =
      'https://raw.githubusercontent.com/firemansghost/grayghost-risk/main/data/latest.json?ts=' + Date.now();

    const res = await fetch(DATA_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error('Fetch failed: ' + res.status + ' ' + res.statusText);
    const data = await res.json();

    // ...top card & helpers are same as your latest...
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
        if (k === 'term_structure') {
          const fAnn = g.funding_ann_pct;
          const prem = g.perp_premium_7d_pct;
          extra = `
            <div class="title">Funding (ann.): ${fmtPct(fAnn)}</div>
            <div class="title">Perp Premium (7d): ${fmtPct(prem)}</div>
          `;
        }
        // (existing extras for ETF / Stablecoins / Net Liquidity unchanged...)

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

    // ...rest unchanged...
  } catch (e) {
    console.error(e);
    const riskEl = document.getElementById('riskScore'); if (riskEl) riskEl.textContent = 'N/A';
  }
}
main();
