// app/assets/app.js
async function main() {
  try {
    // Pull the latest risk JSON from your GitHub repo
    const DATA_URL =
      'https://raw.githubusercontent.com/firemansghost/grayghost-risk/main/data/latest.json?ts=' + Date.now();

    const res = await fetch(DATA_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error('Fetch failed: ' + res.status + ' ' + res.statusText);

    // <-- defines `data`
    const data = await res.json();

    // ===== Top card =====
    const asOfEl = document.getElementById('asOf');
    if (asOfEl) asOfEl.textContent = 'As of ' + (data.as_of ?? '—');

    const riskEl = document.getElementById('riskScore');
    if (riskEl) riskEl.textContent = Number(data.risk ?? NaN).toFixed(2);

    const bandEl = document.getElementById('riskBand');
    if (bandEl) {
      const band = String(data.band ?? 'yellow');
      bandEl.textContent = band.toUpperCase();
      bandEl.classList.add(band); // adds .green/.yellow/.red for styling
    }

    // BTC price (new)
    const priceEl = document.getElementById('btcPrice');
    if (priceEl) {
      const p = data.btc_price_usd;
      priceEl.textContent = p ? `BTC $${Number(p).toLocaleString()}` : 'BTC $—';
    }

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
        const div = document.createElement('div');
        div.className = 'gauge';
        div.innerHTML = `
          <div class="title">${map[k]}</div>
          <div class="value">${(g.score * 100).toFixed(0)}<span style="font-size:12px;"> /100</span></div>
          <div class="title">Contribution: ${(g.contribution >= 0 ? '+' : '') + (g.contribution * 100).toFixed(0)} bp</div>
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
        // Visual width scaled up a bit so tiny contributions still show
        const width = Math.min(100, Math.abs(g.contribution * 1000)); // demo scale
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
