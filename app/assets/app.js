
async function main(){
  try{
    const res = await fetch('../data/latest.json', {cache:'no-store'});
    const data = await res.json();
    document.getElementById('asOf').textContent = 'As of ' + data.as_of;
    document.getElementById('riskScore').textContent = data.risk.toFixed(2);
    const bandEl = document.getElementById('riskBand');
    bandEl.textContent = data.band.toUpperCase();
    bandEl.classList.add(data.band);

    // Gauges
    const gauges = document.getElementById('gauges');
    const map = {
      etf_flows: 'ETF Net Flows',
      net_liquidity: 'Global Net Liquidity',
      stablecoins: 'Stablecoin Issuance',
      term_structure: 'Term Structure & Leverage',
      onchain: 'On-chain Value'
    };
    for(const k of Object.keys(map)){
      const g = data.drivers[k];
      if(!g) continue;
      const div = document.createElement('div');
      div.className = 'gauge';
      div.innerHTML = `
        <div class="title">${map[k]}</div>
        <div class="value">${(g.score*100).toFixed(0)}<span style="font-size:12px;"> /100</span></div>
        <div class="title">Contribution: ${(g.contribution>=0?'+':'') + (g.contribution*100).toFixed(0)} bp</div>
      `;
      gauges.appendChild(div);
    }

    // Contributions bars
    const contribs = document.getElementById('contribs');
    for(const k of Object.keys(map)){
      const g = data.drivers[k];
      if(!g) continue;
      const div = document.createElement('div');
      div.className = 'contrib';
      const width = Math.min(100, Math.abs(g.contribution*1000)); // scale for demo
      const color = g.contribution >= 0 ? 'var(--orange)' : 'var(--green)';
      div.innerHTML = `
        <div class="title">${map[k]}</div>
        <div class="bar"><span style="width:${width}%; background:${color};"></span></div>
      `;
      contribs.appendChild(div);
    }
  }catch(e){
    console.error(e);
    document.getElementById('riskScore').textContent = 'N/A';
  }
}
main();
