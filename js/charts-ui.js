// js/charts-ui.js — 비율/주가 캔버스 차트 그리기 + 기간 버튼 + dataZoom + 툴팁 (DOM 의존)
// classic script 전역 공유. 로드 순서 계약(index.html):
//   format.js → calc.js → dashboard-core.js → render.js → charts-ui.js → live-ui.js → app-boot.js
// app-boot.js(startDashboard)가 createDashboardCharts(app)를 호출해 반환 함수를 app에 붙인다.
// 계산부(buildRatioTrendLines/buildChartLegendItems/applyZoomToHistory/normalizeZoomState/
// filterHistoryByDays/getStackedSubsidiarySeries/MIN_ZOOM_SPAN)는 dashboard-core.js 전역,
// 시간축/보간(buildTimeScale/nearestIndexForX/drawTimeAxisLabels/withAlpha/clamp)은 calc.js 전역을 쓴다.

// --- 줌 상태 (app-boot에서도 초기 상태 생성에 사용) ---
function createZoomState() {
  return { start: 0, end: 1 };
}

function resetZoomState(zoom) {
  zoom.start = 0;
  zoom.end = 1;
}

// --- 테마/팔레트 ---
function getThemeColors() {
  const styles = getComputedStyle(document.documentElement);
  return {
    accent: styles.getPropertyValue('--accent').trim(),
    up: styles.getPropertyValue('--up').trim(),
    down: styles.getPropertyValue('--down').trim(),
    text: styles.getPropertyValue('--text').trim(),
    textDim: styles.getPropertyValue('--text-dim').trim(),
    grid: styles.getPropertyValue('--grid').trim(),
    warnLine: styles.getPropertyValue('--warn-line').trim(),
    warnText: styles.getPropertyValue('--warn-text').trim(),
    avgLine: styles.getPropertyValue('--avg-line').trim(),
    avgText: styles.getPropertyValue('--avg-text').trim(),
    smaLine: styles.getPropertyValue('--sma-line').trim(),
    emaLine: styles.getPropertyValue('--ema-line').trim(),
    ratioFillStart: styles.getPropertyValue('--ratio-fill-start').trim(),
    ratioFillEnd: styles.getPropertyValue('--ratio-fill-end').trim(),
    priceUpFillStart: styles.getPropertyValue('--price-up-fill-start').trim(),
    priceUpFillEnd: styles.getPropertyValue('--price-up-fill-end').trim(),
    priceDownFillStart: styles.getPropertyValue('--price-down-fill-start').trim(),
    priceDownFillEnd: styles.getPropertyValue('--price-down-fill-end').trim(),
  };
}

function getStackPalette() {
  if ((document.documentElement.dataset.theme || 'dark') === 'light') {
    return ['#2d66d6', '#1f9b57', '#d1433f', '#b7791f', '#7c4dff', '#118ab2'];
  }
  return ['#6c8cff', '#38c96a', '#f0615e', '#f5a623', '#8a63ff', '#4dc7ff'];
}

function renderChartLegend(items, el) {
  if (!el) return;
  if (!items || items.length === 0) {
    el.innerHTML = '';
    return;
  }
  el.innerHTML = items.map(function(item) {
    return `<span class="chart-legend-item"><span class="legend-swatch" style="background:${item.color}"></span>${item.label}</span>`;
  }).join('');
}

// --- 캔버스 드로잉 프리미티브 (상태 비의존 — 인자만 사용) ---
function drawSingleRatioChart(ctx, hist, xs, colors, pad, cW, cH, chartMin, rangeS) {
  const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  gradient.addColorStop(0, colors.ratioFillStart);
  gradient.addColorStop(1, colors.ratioFillEnd);

  ctx.beginPath();
  for (let i = 0; i < hist.length; i++) {
    const y = pad.top + cH - ((hist[i].ratio - chartMin) / rangeS * cH);
    if (i === 0) ctx.moveTo(xs[i], y);
    else ctx.lineTo(xs[i], y);
  }
  const lastX = pad.left + cW;
  ctx.lineTo(lastX, pad.top + cH);
  ctx.lineTo(pad.left, pad.top + cH);
  ctx.closePath();
  ctx.fillStyle = gradient;
  ctx.fill();

  ctx.beginPath();
  for (let i = 0; i < hist.length; i++) {
    const y = pad.top + cH - ((hist[i].ratio - chartMin) / rangeS * cH);
    if (i === 0) ctx.moveTo(xs[i], y);
    else ctx.lineTo(xs[i], y);
  }
  ctx.strokeStyle = colors.accent;
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';
  ctx.stroke();
}

function drawStackedRatioChart(ctx, hist, xs, series, palette, colors, pad, cW, cH, chartMin, rangeS) {
  let cumulative = new Array(hist.length).fill(0);
  series.forEach(function(sub, subIdx) {
    const color = palette[subIdx % palette.length];
    const nextCumulative = hist.map(function(entry, idx) {
      const contribution = entry.subsidiaries && entry.subsidiaries[subIdx] ? entry.subsidiaries[subIdx].ratio : 0;
      return cumulative[idx] + contribution;
    });

    ctx.beginPath();
    for (let i = 0; i < hist.length; i++) {
      const y = pad.top + cH - ((nextCumulative[i] - chartMin) / rangeS * cH);
      if (i === 0) ctx.moveTo(xs[i], y);
      else ctx.lineTo(xs[i], y);
    }
    for (let i = hist.length - 1; i >= 0; i--) {
      const y = pad.top + cH - ((cumulative[i] - chartMin) / rangeS * cH);
      ctx.lineTo(xs[i], y);
    }
    ctx.closePath();
    ctx.fillStyle = withAlpha(color, 0.22);
    ctx.fill();

    ctx.beginPath();
    for (let i = 0; i < hist.length; i++) {
      const y = pad.top + cH - ((nextCumulative[i] - chartMin) / rangeS * cH);
      if (i === 0) ctx.moveTo(xs[i], y);
      else ctx.lineTo(xs[i], y);
    }
    ctx.strokeStyle = withAlpha(color, 0.9);
    ctx.lineWidth = 1.2;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();

    cumulative = nextCumulative;
  });

  ctx.beginPath();
  for (let i = 0; i < hist.length; i++) {
    const y = pad.top + cH - ((hist[i].ratio - chartMin) / rangeS * cH);
    if (i === 0) ctx.moveTo(xs[i], y);
    else ctx.lineTo(xs[i], y);
  }
  ctx.strokeStyle = colors.text;
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';
  ctx.stroke();
}

function drawTrendLine(ctx, hist, xs, values, color, pad, cH, chartMin, rangeS, dash, lineWidth) {
  if (!values || values.every(function(value) { return value === null; })) {
    return;
  }

  ctx.save();
  ctx.beginPath();
  let hasStarted = false;
  for (let i = 0; i < hist.length; i++) {
    const value = values[i];
    if (value === null) {
      hasStarted = false;
      continue;
    }
    const y = pad.top + cH - ((value - chartMin) / rangeS * cH);
    if (!hasStarted) {
      ctx.moveTo(xs[i], y);
      hasStarted = true;
    } else {
      ctx.lineTo(xs[i], y);
    }
  }
  ctx.setLineDash(dash || []);
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth || 2;
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';
  ctx.stroke();
  ctx.restore();
}

function createDashboardCharts(app) {
  // --- Chart ---
  function renderChart() {
    const p = app.pairs[app.selectedIdx];
    document.getElementById('chartTitle').textContent = p.name + ' 비율 추이';
    const legendEl = document.getElementById('chartLegend');

    const canvas = document.getElementById('chart');
    canvas.setAttribute('aria-label', p.name + '의 보유지분가치/시가총액 비율 추이 선 차트');
    const container = canvas.parentElement;
    const dpr = window.devicePixelRatio || 1;
    const rect = container.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = rect.height + 'px';

    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    const W = rect.width;
    const H = rect.height;

    const baseHist = filterHistoryByDays(p.history, app.periodDays);
    updateDataZoom('ratio', baseHist, app.ratioZoom);
    const hist = applyZoomToHistory(baseHist, app.ratioZoom);
    if (hist.length < 2) {
      ctx.clearRect(0, 0, W, H); // 하이드레이션 대기 중 직전 종목 차트 잔상 제거
      renderChartLegend([], legendEl);
      return;
    }

    const stackedSubs = getStackedSubsidiarySeries(hist);
    const ratios = hist.map(h => h.ratio);
    const trendLines = buildRatioTrendLines(p.history, hist);
    const trendValues = [trendLines.sma.values, trendLines.ema.values]
      .flat()
      .filter(function(value) { return value !== null; });
    const rangeValues = ratios.concat(trendValues);
    const maxS = Math.max(...rangeValues);
    const isStacked = stackedSubs.length > 1;
    const minS = isStacked ? 0 : Math.min(...rangeValues);
    const dataSpan = Math.max(maxS - minS, 1);
    const include100 = 100 >= minS - dataSpan * 0.5 && 100 <= maxS + dataSpan * 0.5;
    const chartPadding = Math.max(maxS - minS, 1) * 0.05;
    const chartMin = (isStacked ? 0 : (include100 ? Math.min(minS, 100) : minS) - chartPadding);
    const chartMax = (include100 ? Math.max(maxS, 100) : maxS) + chartPadding;
    const rangeS = chartMax - chartMin || 1;
    const avg = ratios.reduce((a, b) => a + b, 0) / ratios.length;

    const pad = { top: 20, right: 16, bottom: 36, left: 64 };
    const cW = W - pad.left - pad.right;
    const cH = H - pad.top - pad.bottom;
    const xs = buildTimeScale(hist, pad.left, cW);
    const colors = getThemeColors();
    const palette = getStackPalette();
    const legendItems = buildChartLegendItems(stackedSubs, palette, colors);

    // Clear
    ctx.clearRect(0, 0, W, H);
    renderChartLegend(legendItems, legendEl);

    // Y axis grid
    ctx.strokeStyle = colors.grid;
    ctx.lineWidth = 1;
    const yTicks = 5;
    ctx.font = '12px -apple-system, sans-serif';
    ctx.fillStyle = colors.textDim;
    ctx.textAlign = 'right';
    for (let i = 0; i <= yTicks; i++) {
      const v = chartMin + (rangeS * i / yTicks);
      const y = pad.top + cH - (cH * i / yTicks);
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(W - pad.right, y);
      ctx.stroke();
      ctx.fillText(v.toFixed(1) + '%', pad.left - 8, y + 4);
    }

    // ±σ 밴드 (단일 라인 차트 전용) — 평균선/SMA/EMA/데이터 라인보다 먼저 그려 밴드가 바닥에 깔리게 한다
    if (!isStacked) {
      const variance = ratios.reduce((s, r) => s + (r - avg) * (r - avg), 0) / ratios.length;
      const sd = Math.sqrt(variance);
      if (sd > 0) {
        ctx.save();
        const bandTop = Math.max(pad.top, pad.top + cH - ((avg + sd - chartMin) / rangeS * cH));
        const bandBottom = Math.min(pad.top + cH, pad.top + cH - ((avg - sd - chartMin) / rangeS * cH));
        if (bandBottom > bandTop) {
          ctx.fillStyle = withAlpha(colors.accent, 0.08);
          ctx.fillRect(pad.left, bandTop, cW, bandBottom - bandTop);
        }
        ctx.font = '11px -apple-system, sans-serif';
        [
          { value: avg + sd * 2, label: '+2σ' },
          { value: avg - sd * 2, label: '−2σ' },
        ].forEach(function(sigma) {
          if (sigma.value < chartMin || sigma.value > chartMax) return;
          const y = pad.top + cH - ((sigma.value - chartMin) / rangeS * cH);
          ctx.strokeStyle = withAlpha(colors.accent, 0.45);
          ctx.lineWidth = 1;
          ctx.setLineDash([4, 4]);
          ctx.beginPath();
          ctx.moveTo(pad.left, y);
          ctx.lineTo(W - pad.right, y);
          ctx.stroke();
          ctx.setLineDash([]);
          ctx.fillStyle = colors.textDim;
          ctx.textAlign = 'right';
          ctx.fillText(sigma.label, W - pad.right - 4, y - 4);
        });
        ctx.restore();
      }
    }

    // 100% reference line
    if (100 >= chartMin && 100 <= chartMax) {
      const refY = pad.top + cH - ((100 - chartMin) / rangeS * cH);
      ctx.strokeStyle = colors.warnLine;
      ctx.lineWidth = 1.5;
      ctx.setLineDash([6, 4]);
      ctx.beginPath();
      ctx.moveTo(pad.left, refY);
      ctx.lineTo(W - pad.right, refY);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = colors.warnText;
      ctx.textAlign = 'left';
      ctx.fillText('100%', W - pad.right - 40, refY - 6);
    }

    // Average line
    const avgY = pad.top + cH - ((avg - chartMin) / rangeS * cH);
    ctx.strokeStyle = colors.avgLine;
    ctx.setLineDash([6, 4]);
    ctx.beginPath();
    ctx.moveTo(pad.left, avgY);
    ctx.lineTo(W - pad.right, avgY);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = colors.avgText;
    ctx.textAlign = 'left';
    ctx.fillText('평균 ' + avg.toFixed(1) + '%', pad.left + 4, avgY - 6);

    // X axis labels (시간 위치 기준)
    ctx.fillStyle = colors.textDim;
    ctx.textAlign = 'center';
    drawTimeAxisLabels(ctx, hist, xs, pad.left, cW, H - pad.bottom + 20);

    if (isStacked) {
      drawStackedRatioChart(ctx, hist, xs, stackedSubs, palette, colors, pad, cW, cH, chartMin, rangeS);
    } else {
      drawSingleRatioChart(ctx, hist, xs, colors, pad, cW, cH, chartMin, rangeS);
    }

    drawTrendLine(ctx, hist, xs, trendLines.sma.values, colors.smaLine, pad, cH, chartMin, rangeS, [10, 6], 2);
    drawTrendLine(ctx, hist, xs, trendLines.ema.values, colors.emaLine, pad, cH, chartMin, rangeS, [], 2);

    // Tooltip interaction
    const tooltip = document.getElementById('tooltip');
    canvas.onpointermove = function(e) {
      const r = canvas.getBoundingClientRect();
      const mx = e.clientX - r.left;
      const my = e.clientY - r.top;
      if (mx < pad.left || mx > W - pad.right || my < pad.top || my > pad.top + cH) {
        tooltip.classList.remove('visible');
        return;
      }
      const idx = nearestIndexForX(xs, mx);
      const d = hist[idx];
      if (!d) return;

      const dotX = xs[idx];
      const dotY = pad.top + cH - ((d.ratio - chartMin) / rangeS * cH);

      let ttRows;
      if (p.isAverage) {
        ttRows = `<div class="tt-row"><span><strong>비율</strong></span><span><strong>${d.ratio.toFixed(2)}%</strong></span></div>`;
        if (typeof d.count === 'number') {
          ttRows += `<div class="tt-row"><span>구성 종목</span><span>${d.count}개</span></div>`;
        }
      } else if (Array.isArray(d.subsidiaries) && d.subsidiaries.length > 1) {
        ttRows = `<div class="tt-row"><span>${escapeHtml(p.holdingName)}</span><span>${formatPrice(d.holdingPrice)}</span></div>`;
        d.subsidiaries.forEach(function(sub) {
          ttRows += `<div class="tt-row"><span>${escapeHtml(sub.name)}</span><span>${formatRatio(sub.ratio)}</span></div>`;
        });
        ttRows += `<div class="tt-row"><span><strong>총 비율</strong></span><span><strong>${d.ratio.toFixed(2)}%</strong></span></div>`;
      } else {
        ttRows = `<div class="tt-row"><span>${escapeHtml(p.holdingName)}</span><span>${formatPrice(d.holdingPrice)}</span></div>`;
        if (d.subsidiaryPrice) {
          ttRows += `<div class="tt-row"><span>${escapeHtml(p.subsidiaryName)}</span><span>${formatPrice(d.subsidiaryPrice)}</span></div>`;
        }
        ttRows += `<div class="tt-row"><span>보유지분가치</span><span>${formatOk(d.holdingValue)}</span></div>
           <div class="tt-row"><span>조정시가총액</span><span>${formatOk(d.marketCap)}</span></div>
           <div class="tt-row"><span><strong>비율</strong></span><span><strong>${d.ratio.toFixed(2)}%</strong></span></div>`;
      }

      tooltip.innerHTML = `
        <div class="tt-date">${d.date}</div>
        ${ttRows}
      `;

      let tx = dotX + 12;
      let ty = dotY - 80;
      tooltip.style.left = '0px';
      tooltip.style.top = '0px';
      tooltip.classList.add('visible');
      const ttWidth = tooltip.offsetWidth || 210;
      const ttHeight = tooltip.offsetHeight || 80;
      if (tx + ttWidth + 12 > W) tx = dotX - ttWidth - 12;
      ty = dotY - ttHeight - 12;
      if (ty < 0) ty = dotY + 12;
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
    };
    canvas.onpointerleave = function() {
      tooltip.classList.remove('visible');
    };
  }

  // --- Period buttons ---
  function bindPeriodBtns() {
    document.getElementById('periodBtns').addEventListener('click', function(e) {
      if (e.target.tagName !== 'BUTTON') return;
      this.querySelectorAll('button').forEach(b => b.classList.remove('active'));
      e.target.classList.add('active');
      app.periodDays = parseInt(e.target.dataset.days);
      resetZoomState(app.ratioZoom);
      renderChart();
      app.renderStats();
    });
  }

  // --- Price Chart ---
  function renderPriceChart() {
    const p = app.pairs[app.selectedIdx];
    const section = document.getElementById('priceChartSection');
    if (p.isAverage) {
      section.style.display = 'none';
      return;
    }
    section.style.display = '';
    document.getElementById('priceChartTitle').textContent = p.holdingName + ' 주가 추이';

    const canvas = document.getElementById('priceChart');
    canvas.setAttribute('aria-label', p.holdingName + ' 주가 추이 선 차트');
    const container = canvas.parentElement;
    const dpr = window.devicePixelRatio || 1;
    const rect = container.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = rect.height + 'px';

    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    const W = rect.width;
    const H = rect.height;

    const baseHist = filterHistoryByDays(p.history, app.pricePeriodDays);
    updateDataZoom('price', baseHist, app.priceZoom);
    const hist = applyZoomToHistory(baseHist, app.priceZoom);
    if (hist.length < 2) {
      ctx.clearRect(0, 0, W, H);
      return;
    }

    const prices = hist.map(h => h.holdingPrice);
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const chartMin = minP - (maxP - minP) * 0.05;
    const chartMax = maxP + (maxP - minP) * 0.05;
    const rangeP = chartMax - chartMin || 1;

    const pad = { top: 20, right: 16, bottom: 36, left: 80 };
    const cW = W - pad.left - pad.right;
    const cH = H - pad.top - pad.bottom;
    const xs = buildTimeScale(hist, pad.left, cW);
    const colors = getThemeColors();

    ctx.clearRect(0, 0, W, H);

    // Y axis grid
    ctx.strokeStyle = colors.grid;
    ctx.lineWidth = 1;
    const yTicks = 5;
    ctx.font = '12px -apple-system, sans-serif';
    ctx.fillStyle = colors.textDim;
    ctx.textAlign = 'right';
    for (let i = 0; i <= yTicks; i++) {
      const v = chartMin + (rangeP * i / yTicks);
      const y = pad.top + cH - (cH * i / yTicks);
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(W - pad.right, y);
      ctx.stroke();
      ctx.fillText(Math.round(v).toLocaleString('ko-KR'), pad.left - 8, y + 4);
    }

    // X axis labels (시간 위치 기준)
    ctx.fillStyle = colors.textDim;
    ctx.textAlign = 'center';
    drawTimeAxisLabels(ctx, hist, xs, pad.left, cW, H - pad.bottom + 20);

    // Price change color
    const priceUp = prices[prices.length - 1] >= prices[0];
    const lineColor = priceUp ? colors.up : colors.down;
    const fillStart = priceUp ? colors.priceUpFillStart : colors.priceDownFillStart;
    const fillEnd = priceUp ? colors.priceUpFillEnd : colors.priceDownFillEnd;

    // Gradient fill
    const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
    gradient.addColorStop(0, fillStart);
    gradient.addColorStop(1, fillEnd);

    ctx.beginPath();
    for (let i = 0; i < hist.length; i++) {
      const y = pad.top + cH - ((hist[i].holdingPrice - chartMin) / rangeP * cH);
      if (i === 0) ctx.moveTo(xs[i], y);
      else ctx.lineTo(xs[i], y);
    }
    ctx.lineTo(pad.left + cW, pad.top + cH);
    ctx.lineTo(pad.left, pad.top + cH);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    // Line
    ctx.beginPath();
    for (let i = 0; i < hist.length; i++) {
      const y = pad.top + cH - ((hist[i].holdingPrice - chartMin) / rangeP * cH);
      if (i === 0) ctx.moveTo(xs[i], y);
      else ctx.lineTo(xs[i], y);
    }
    ctx.strokeStyle = lineColor;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();

    // Tooltip
    const tooltip = document.getElementById('priceTooltip');
    canvas.onpointermove = function(e) {
      const r = canvas.getBoundingClientRect();
      const mx = e.clientX - r.left;
      const my = e.clientY - r.top;
      if (mx < pad.left || mx > W - pad.right || my < pad.top || my > pad.top + cH) {
        tooltip.classList.remove('visible');
        return;
      }
      const idx = nearestIndexForX(xs, mx);
      const d = hist[idx];
      if (!d) return;

      const dotX = xs[idx];
      const dotY = pad.top + cH - ((d.holdingPrice - chartMin) / rangeP * cH);

      const change = idx > 0 ? d.holdingPrice - hist[idx - 1].holdingPrice : 0;
      const changePct = idx > 0 ? (change / hist[idx - 1].holdingPrice * 100) : 0;
      const changeDir = change >= 0 ? 'up' : 'down';
      const arrow = change >= 0 ? '▲' : '▼';

      tooltip.innerHTML = `
        <div class="tt-date">${d.date}</div>
        <div class="tt-row"><span>${escapeHtml(p.holdingName)}</span><span>${formatPrice(d.holdingPrice)}</span></div>
        <div class="tt-row"><span>전일 대비</span><span class="${changeDir}-color">${arrow} ${Math.abs(changePct).toFixed(2)}%</span></div>
      `;

      let tx = dotX + 12;
      let ty = dotY - 60;
      if (tx + 210 > W) tx = dotX - 210;
      if (ty < 0) ty = dotY + 12;
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
      tooltip.classList.add('visible');
    };
    canvas.onpointerleave = function() {
      tooltip.classList.remove('visible');
    };
  }

  function bindPricePeriodBtns() {
    document.getElementById('pricePeriodBtns').addEventListener('click', function(e) {
      if (e.target.tagName !== 'BUTTON') return;
      this.querySelectorAll('button').forEach(b => b.classList.remove('active'));
      e.target.classList.add('active');
      app.pricePeriodDays = parseInt(e.target.dataset.days);
      resetZoomState(app.priceZoom);
      renderPriceChart();
    });
  }

  function bindDataZoomControls() {
    ['ratio', 'price'].forEach(function(type) {
      const root = document.getElementById(type + 'DataZoom');
      if (!root) return;
      const track = root.querySelector('.data-zoom-track');
      const selection = root.querySelector('.data-zoom-selection');
      if (!track || !selection) return;

      track.addEventListener('pointerdown', function(e) {
        if (root.classList.contains('disabled')) return;
        const zoom = getZoomState(type);
        normalizeZoomState(zoom);

        const rect = track.getBoundingClientRect();
        const startPoint = clamp((e.clientX - rect.left) / rect.width, 0, 1);
        const targetHandle = e.target && e.target.dataset ? e.target.dataset.handle : '';
        const mode = targetHandle || (selection.contains(e.target) ? 'pan' : 'center');
        const initialStart = zoom.start;
        const initialEnd = zoom.end;
        const span = initialEnd - initialStart;

        selection.classList.add('dragging');
        track.setPointerCapture(e.pointerId);

        const move = function(ev) {
          const point = clamp((ev.clientX - rect.left) / rect.width, 0, 1);
          if (mode === 'start') {
            zoom.start = clamp(point, 0, initialEnd - MIN_ZOOM_SPAN);
          } else if (mode === 'end') {
            zoom.end = clamp(point, initialStart + MIN_ZOOM_SPAN, 1);
          } else {
            const center = mode === 'center' ? point : ((initialStart + initialEnd) / 2) + (point - startPoint);
            zoom.start = clamp(center - span / 2, 0, 1 - span);
            zoom.end = zoom.start + span;
          }
          normalizeZoomState(zoom);
          renderZoomedChart(type);
        };

        const up = function(ev) {
          selection.classList.remove('dragging');
          try {
            track.releasePointerCapture(ev.pointerId);
          } catch (err) {}
          track.removeEventListener('pointermove', move);
          track.removeEventListener('pointerup', up);
          track.removeEventListener('pointercancel', up);
        };

        track.addEventListener('pointermove', move);
        track.addEventListener('pointerup', up);
        track.addEventListener('pointercancel', up);
        move(e);
      });

      track.addEventListener('dblclick', function() {
        resetZoomState(getZoomState(type));
        renderZoomedChart(type);
      });
    });
  }

  function renderZoomedChart(type) {
    if (type === 'ratio') {
      renderChart();
      app.renderStats();
    } else {
      renderPriceChart();
    }
  }

  function getZoomState(type) {
    return type === 'ratio' ? app.ratioZoom : app.priceZoom;
  }

  function updateDataZoom(type, baseHist, zoom) {
    const root = document.getElementById(type + 'DataZoom');
    if (!root) return;
    const selection = root.querySelector('.data-zoom-selection');
    const startLabel = root.querySelector('[data-zoom-start]');
    const summaryLabel = root.querySelector('[data-zoom-summary]');
    const endLabel = root.querySelector('[data-zoom-end]');
    const disabled = !baseHist || baseHist.length < 3;

    root.classList.toggle('disabled', disabled);
    if (disabled) {
      if (selection) {
        selection.style.left = '0%';
        selection.style.width = '100%';
      }
      if (startLabel) startLabel.textContent = '';
      if (summaryLabel) summaryLabel.textContent = '';
      if (endLabel) endLabel.textContent = '';
      return;
    }

    normalizeZoomState(zoom);
    const startPct = zoom.start * 100;
    const endPct = zoom.end * 100;
    const visibleHist = applyZoomToHistory(baseHist, zoom);
    if (selection) {
      selection.style.left = startPct.toFixed(2) + '%';
      selection.style.width = Math.max(0, endPct - startPct).toFixed(2) + '%';
    }
    if (startLabel) startLabel.textContent = visibleHist[0].date;
    if (summaryLabel) {
      const isFull = zoom.start <= 0.001 && zoom.end >= 0.999;
      summaryLabel.textContent = isFull ? '전체 구간' : visibleHist.length.toLocaleString('ko-KR') + '개 포인트';
    }
    if (endLabel) endLabel.textContent = visibleHist[visibleHist.length - 1].date;
  }

  return {
    renderChart,
    renderPriceChart,
    bindPeriodBtns,
    bindPricePeriodBtns,
    bindDataZoomControls,
  };
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export (구조 계약 테스트용).
// 차트 함수는 DOM/canvas 의존이라 브라우저에서만 실제 동작한다.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createZoomState, resetZoomState, createDashboardCharts };
}
