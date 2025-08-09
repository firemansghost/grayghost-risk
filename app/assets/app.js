// app/assets/app.js
async function main() {
  try {
    const DATA_URL =
      'https://raw.githubusercontent.com/firemansghost/grayghost-risk/main/data/latest.json?ts=' + Date.now();

    const res = await fetch(DATA_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error('Fetch failed: ' + res.status + ' ' + res.statusText);
    const data = await res.json();

    const WINDOW_DAYS = data.smooth_days ?? 7;
    const avgLabel = `Avg (${WINDOW_DAYS}d)`;

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

    // ===== helpers =====
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
      if (Math.abs(n) < 0.10) return `${(n * 100).toFixed(2)} bp`;
      return `${n.toFixed(2)}%`;
    };

    // color classes for term-structure lines
    const clsFunding = (ann) => {
      const n = Number(ann);
      if (!isFinite(n)) return 'neu';
      if (n >= 12) return 'hot';
      if (n <= 8)  return 'cool';
      return 'neu';
    };
    const clsPremium = (pct) => {
      const n = Number(pct);
      if (!isFinite(n)) return 'neu';
      if (n >= 0.15) return 'hot';
      if (n <= -0.10) return 'cool';
      return 'neu';
    };

    // ===== sparkline helpers =====
    const isNum = (v) => Number.isFinite(Number(v));
    function makeSparkline(trailing) {
      const series = (trailing || []).slice().reverse(); // oldest -> newest for drawing & tooltip
      const vals = series.map(d => d && isNum(d.usd) ? Number(d.usd) : null).filter(v => v !== null);
      if (vals.length < 2) return '';

      const w = 100, h = 28, pad = 1;
      let min = Math.min(...vals), max = Math.max(...vals);
      if (min === max) { min -= 1; max += 1; }
      const sx = (i) => pad + (i * (w - 2*pad) / (vals.length - 1));
      const sy = (v) => {
        const t = (v - min) / (max - min);
        return (h - pad) - t * (h - 2*pad);
      };

      let d = `M ${sx(0)} ${sy(vals[0])}`;
      for (let i = 1; i < vals.length; i++) d += ` L ${sx(i)} ${sy(vals[i])}`;
      const cls = vals[vals.length - 1] >= vals[0] ? 'up' : 'down';

      const y0 = sy(min), y1 = sy(max);
      const bg = `<path class="spark-bg" d="M ${pad} ${y0} L ${w-pad} ${y0} M ${pad} ${y1} L ${w-pad} ${y1}" />`;
      const seriesAttr = encodeURIComponent(JSON.stringify(series));

      return `
        <div class="sparkline" data-series="${seriesAttr}">
          <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
            ${bg}
            <path class="spark ${cls}" d="${d}" />
            <rect x="0" y="0" width="${w}" height="${h}" fill="transparent"></rect>
          </svg>
        </div>
      `;
    }

    function attachSparklineTooltips() {
      const tip = document.createElement('div');
      tip.className = 'spark-tip';
      document.body.appendChild(tip);

      document.querySelectorAll('.sparkline').forEach(el => {
        const raw = el.getAttribute('data-series');
        if (!raw) return;
        let series;
        try { series = JSON.parse(decodeURIComponent(raw)); } catch { series = []; }
        if (!Array.isArray(series) || series.length < 2) return;

        el.addEventListener('mousemove', (e) => {
          const rect = el.getBoundingClientRect();
          const x = e.clientX - rect.left;
          const idx = Math.max(0, Math.min(series.length - 1, Math.round((x / rect.width) * (series.length - 1))));
          const pt = series[idx];
          if (!pt) return;

          const v = Number(pt.usd);
          const absFmt = humanUSD(Math.abs(v)).replace('$','');
          tip.textContent = `${pt.date}: ${v >= 0 ? '+' : '−'}${absFmt}`;

          tip.style.left = (e.pageX + 12) + 'px';
          tip.style.top  = (e.pageY + 12) + 'px';
          tip.style.opacity = '1';
        });

        el.addEventListener('mouseleave', () => { tip.style.opacity = '0'; });
      });
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

        let extra = '';
        if (k === 'etf_flows') {
          const raw = (typeof data.etf_flow_usd === 'number') ? data.etf_flow_usd : g.raw_usd;
          const sma = (typeof data.etf_flow_sma7_usd === 'number') ? data.etf_flow_sma7_usd : g.sma7_usd;
          const r = fmtSignedUSD(raw), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Today: <span class="${r.cls}">${r.text}</span></div>
            <div class="title">${avgLabel}: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'stablecoins') {
          const raw = (typeof data.stablecoin_delta_usd === 'number') ? data.stablecoin_delta_usd : g.raw_delta_usd;
          const sma = (typeof data.stablecoin_delta_sma7_usd === 'number') ? data.stablecoin_delta_sma7_usd : g.sma7_delta_usd;
          const r = fmtSignedUSD(raw), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Today: <span class="${r.cls}">${r.text}</span></div>
            <div class="title">${avgLabel}: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'net_liquidity') {
          const lvl = g.level_usd;
          const d1  = g.delta1d_usd;
          const sma = g.sma7_delta_usd;
          const d1f = fmtSignedUSD(d1), s = fmtSignedUSD(sma);
          extra = `
            <div class="title">Level: ${fmtLevelUSD(lvl)}</div>
            <div class="title">Today: <span class="${d1f.cls}">${d1f.text}</span></div>
            <div class="title">${avgLabel}: <span class="${s.cls}">${s.text}</span></div>
          `;
        } else if (k === 'term_structure') {
          const fAnn = g.funding_ann_pct;
          const prem = g.perp_premium_7d_pct;
          const fCls = clsFunding(fAnn);
          const pCls = clsPremium(prem);
          extra = `
            <div class="title">Funding (ann.): <span class="${fCls}">${fmtPct(fAnn)}</span></div>
            <div class="title">Perp Premium (7d): <span class="${pCls}">${fmtPctOrBp(prem)}</span></div>
          `;
        } else if (k === 'onchain') {
          const aT = g.addr_today, aA = g.addr_avg_w;
          const fT = g.fee_usd_today, fA = g.fee_usd_avg_w;
          const mMB = g.mempool_vsize_mb, mFee = g.mempool_halfhour_satvb;

          const fTfmt = fmtSignedUSD(fT), fAfmt = fmtSignedUSD(fA);
          const actToday = Number.isFinite(aT) ? aT.toLocaleString() : '—';
          const actAvg   = Number.isFinite(aA) ? aA.toLocaleString() : '—';
          const mText = (Number.isFinite(mMB) ? `${mMB.toFixed(1)} MB` : '—')
                      + ', 30m ~ ' + (Number.isFinite(mFee) ? `${mFee.toFixed(0)} sat/vB` : '—');

          extra = `
            <div class="title">Activity (addr): ${actToday} · ${avgLabel}: ${actAvg}</div>
            <div class="title">Fees: <span class="${fTfmt.cls}">${fTfmt.text}</span> · ${avgLabel}: <span class="${fAfmt.cls}">${fAfmt.text}</span></div>
            <div class="title">Mempool: ${mText}</div>
          `;
        }

        const spark = makeSparkline(g.trailing);

        const div = document.createElement('div');
        div.className = 'gauge';
        div.innerHTML = `
          <div class="title">${map[k]}</div>
          <div class="value">${(g.score * 100).toFixed(0)}<span style="font-size:12px;"> /100</span></div>
          <div class="title">Contribution: ${(g.contribution >= 0 ? '+' : '') + (g.contribution * 100).toFixed(0)} bp</div>
          ${extra}
          ${spark}
        `;
        gauges.appendChild(div);
      }
      // after the DOM exists, hook up tooltips
      attachSparklineTooltips();
    }

    // ===== "Why it moved" bars =====
    const contribs = document.getElementById('contribs');
    if (contribs && data.drivers) {
      for (const k of Object.keys(map)) {
        const g = data.drivers[k];
        if (!g) continue;
        const div = document.createElement('div');
        div.className = 'contrib';
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
