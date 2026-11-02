// js/render.js — 카드·오늘의현황·종목비교표·통계·기여 분해 렌더 (DOM 의존)
// classic script 전역 공유. 로드 순서 계약(index.html):
//   format.js → calc.js → dashboard-core.js → render.js → charts-ui.js → live-ui.js → app-boot.js
// app-boot.js(startDashboard)가 createDashboardRenderers(app)를 호출해 반환 함수를
// app 컨텍스트에 붙인다. app은 공유 상태(pairs/selectedIdx/todayOverviewData …)와
// 다른 모듈의 함수(buildTodaySummary/setSelectedIdx/getFilteredHistory …)를 담는다.
// 포맷/이스케이프 헬퍼(escapeHtml, formatRatio …)와 계산부(computeRatioStats,
// buildContributionRows …)는 format.js/dashboard-core.js 전역을 그대로 쓴다.
function createDashboardRenderers(app) {
  const expandedHoldings = new Set();
  const expandedIndirectPairs = new Set();
  function renderOwnershipLink(stock, name, compact, targetStock) {
    if (!stock) return '';
    const icon = '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><rect x="7" y="1.5" width="6" height="5" rx="1.3"/><path d="M10 6.5v4M4 14v-3.5h12V14"/><rect x="1" y="14" width="6" height="4.5" rx="1.3"/><rect x="13" y="14" width="6" height="4.5" rx="1.3"/></svg>';
    return `<button type="button" class="${compact ? 'ownership-mini-link' : 'ownership-open'}" data-ownership-stock="${escapeHtml(stock)}"${targetStock ? ` data-ownership-target="${escapeHtml(targetStock)}"` : ''} aria-label="${escapeHtml(name || stock)} 지분 연결도 열기" title="지분 연결도" aria-haspopup="dialog">${icon}${compact ? '' : '지분 연결도 <span aria-hidden="true">→</span>'}</button>`;
  }

  function renderPriceLine(label, price, change, suffix, ticker) {
    const changeText = formatPriceChange(change);
    const changeHtml = changeText
      ? `<span class="price-change ${getPriceChangeClass(change)}">${changeText}</span>`
      : '<span class="price-change flat-color">-</span>';
    const suffixText = suffix ? `<span class="price-suffix">${suffix}</span>` : '<span class="price-suffix"></span>';
    return `<div class="price-line">
      <span class="price-label" data-portfolio-code="${escapeHtml(ticker || '')}" data-portfolio-price="${escapeHtml(price ?? '')}">${escapeHtml(label)}${renderOwnershipLink(ticker, label, true)}</span>
      <span class="price-value">${formatPrice(price)}</span>
      <span class="price-meta">${changeHtml}${suffixText}</span>
    </div>`;
  }

  function renderPriceCell(price, change) {
    const changeText = formatPriceChange(change);
    if (!changeText) return formatPrice(price);
    return `${formatPrice(price)} <span class="price-change ${getPriceChangeClass(change)}">${changeText}</span>`;
  }

  function updateLastUpdatedText() {
    var parts = ['최종 업데이트: ' + app.stockData.lastUpdated];
    if (window.CURRENT_DATA && window.CURRENT_DATA.isPartial) {
      var preserved = (window.CURRENT_DATA.preservedPairIds || []).length;
      var missing = (window.CURRENT_DATA.missingPairIds || []).length;
      if (preserved > 0) {
        parts.push('일부 종목은 직전 정상값 유지');
      }
      if (missing > 0) {
        parts.push('미반영 종목 ' + missing + '개');
      }
    }
    if (app.lastRefreshError) {
      parts.push('새로고침 실패: ' + app.lastRefreshError);
    }
    document.getElementById('lastUpdated').textContent = parts.join(' · ');
  }

  function renderAverageSparkline() {
    const avgPair = app.pairs.find(function(pair) { return pair.isAverage; });
    const history = (avgPair?.history || [])
      .filter(function(row) { return typeof row.ratio === 'number' && Number.isFinite(row.ratio); })
      .slice(-18);
    if (history.length < 2) {
      return `<div class="average-sparkline empty">추이 데이터 대기</div>`;
    }

    const width = 128;
    const height = 56;
    const pad = 4;
    const values = history.map(function(row) { return row.ratio; });
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    const points = values.map(function(value, idx) {
      const x = pad + (idx / (values.length - 1)) * (width - pad * 2);
      const y = pad + (1 - (value - min) / span) * (height - pad * 2);
      return [x.toFixed(1), y.toFixed(1)];
    });
    const line = points.map(function(point) { return point.join(','); }).join(' ');
    const area = [
      `${pad},${height - pad}`,
      line,
      `${width - pad},${height - pad}`,
    ].join(' ');
    const direction = values[values.length - 1] - values[0];
    const dir = getDirectionClass(direction);
    return `<div class="average-sparkline ${dir}">
      <div class="average-sparkline-label">최근 ${history.length}개 관측치</div>
      <svg viewBox="0 0 ${width} ${height}" aria-hidden="true">
        <polygon class="average-sparkline-fill" points="${area}"></polygon>
        <polyline class="average-sparkline-line" points="${line}"></polyline>
      </svg>
    </div>`;
  }

  function renderMarketMiniItems(market) {
    const extras = Array.isArray(market.extras) ? market.extras.slice(0, 4) : [];
    if (!extras.length) {
      return `<div class="market-row">
        <span class="market-row-name">보조 지표</span>
        <span class="market-row-value">-</span>
        <span class="market-row-change flat">대기</span>
      </div>`;
    }
    return extras.map(function(item) {
      const dir = getDirectionClass(item.changePct);
      const decimals = typeof item.priceDecimals === 'number' ? item.priceDecimals : 2;
      return `<div class="market-row">
        <span class="market-row-name">${escapeHtml(item.name || item.id || '지표')}</span>
        <span class="market-row-value">${formatMarketNumber(item.price, decimals)}</span>
        <span class="market-row-change ${dir}">${formatSignedPercent(item.changePct)}</span>
      </div>`;
    }).join('');
  }

  function renderMarketCard(market, marketDir) {
    const source = market.source || market.marketStatus || 'KIS proxy';
    return `<div class="overview-card market-card">
      <div class="market-main">
        <div class="overview-label">KOSPI</div>
        <div class="market-headline">
          <div class="overview-value">${formatMarketNumber(market.price, 2)}</div>
          <div class="overview-change ${marketDir}">${formatSignedPercent(market.changePct)}</div>
        </div>
        <div class="overview-detail">국내 시장 대표 지수</div>
      </div>
      <div class="market-row-list">
        ${renderMarketMiniItems(market)}
      </div>
      <div class="overview-summary-grid market-summary">
        <div class="overview-stat">
          <div class="overview-stat-label">변화폭</div>
          <div class="overview-stat-value ${marketDir}">${formatMarketNumber(market.change, 2)}pt</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-label">출처</div>
          <div class="overview-stat-value flat">${escapeHtml(source)}</div>
        </div>
      </div>
    </div>`;
  }

  function renderTodayOverview() {
    const el = document.getElementById('todayOverview');
    if (!el) return;
    const summary = app.todayOverviewData || app.buildTodaySummary(window.CURRENT_DATA);
    const avgPairRef = app.pairs.find(function(pair) { return pair.isAverage; });
    const avgCount = (avgPairRef && avgPairRef.current && avgPairRef.current.count) || summary.representativeCount || 0;
    const avgDir = getDirectionClass(summary.averageRatioChange);
    const holdingDir = getDirectionClass(summary.averageHoldingChange);
    const subDir = getDirectionClass(summary.averageSubsidiaryChange);
    const market = summary.market || {};
    const marketDir = getDirectionClass(market.changePct);

    el.innerHTML = `
      <div class="today-overview-head">
        <div class="today-overview-title-wrap">
          <div class="today-overview-kicker">오늘의 지주회사 현황</div>
          <div class="today-overview-title">실시간 지분가치 스냅샷</div>
        </div>
        <div class="today-overview-meta">${escapeHtml(summary.source || 'current.json')} · 대표 ${summary.representativeCount || 0}개 그룹 기준</div>
      </div>
      <div class="today-overview-grid">
        <div class="overview-card primary average-card">
          <div class="average-card-body">
            <div class="average-main-metric">
              <div class="overview-label">지분가치 비율 중앙값</div>
              <div class="overview-value">${formatRatio(summary.averageRatio)}</div>
              <div class="overview-change ${avgDir}">${formatSignedPoints(summary.averageRatioChange)}</div>
              <div class="overview-points">${avgCount ? '구성 ' + avgCount + '종목 중앙값' : ''}</div>
            </div>
            ${renderAverageSparkline()}
          </div>
          <div class="overview-summary-grid">
            <div class="overview-stat">
              <div class="overview-stat-label">지주사 평균 상승률</div>
              <div class="overview-stat-value ${holdingDir}">${formatSignedPercent(summary.averageHoldingChange)}</div>
            </div>
            <div class="overview-stat">
              <div class="overview-stat-label">자회사 평균 상승률</div>
              <div class="overview-stat-value ${subDir}">${formatSignedPercent(summary.averageSubsidiaryChange)}</div>
            </div>
          </div>
        </div>
        ${renderMarketCard(market, marketDir)}
        ${renderLeaderCard('최고 비율 확대', summary.topExpansion, summary.expansionRunners)}
        ${renderLeaderCard('최고 비율 축소', summary.topContraction, summary.contractionRunners)}
      </div>`;

    el.querySelectorAll('[data-pair-id]').forEach(function(node) {
      node.addEventListener('click', function(event) {
        event.stopPropagation();
        app.setSelectedPairById(node.dataset.pairId);
      });
      if (node.tagName === 'BUTTON') return;
      // div 리더 카드도 키보드로 선택 가능하게 (rank 항목은 원래 button이라 제외)
      node.addEventListener('keydown', function(event) {
        if (event.target !== node) return;
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        const pairId = node.dataset.pairId;
        app.setSelectedPairById(pairId);
        // 재렌더로 기존 노드가 사라지므로 같은 카드에 포커스 복원 (차트 스크롤은 유지)
        const next = el.querySelector('.leader-card[data-pair-id="' + pairId + '"]');
        if (next) next.focus({ preventScroll: true });
      });
    });
  }

  function renderLeaderCard(label, pair, runners) {
    if (!pair) {
      return `<div class="overview-card"><div class="overview-label">${label}</div><div class="overview-detail">표시할 데이터가 없습니다.</div></div>`;
    }
    const dir = getDirectionClass(pair.current.ratioChange);
    const isActive = !!(app.pairs[app.selectedIdx] && app.pairs[app.selectedIdx].id === pair.id);
    const active = isActive ? ' active' : '';
    const sideClass = runners && runners.length ? '' : ' no-side';
    return `<div class="overview-card clickable leader-card${active}" data-pair-id="${escapeHtml(pair.id)}" role="button" tabindex="0" aria-pressed="${isActive}">
      <div class="overview-split runners${sideClass}">
        <div class="overview-main-copy">
          <div class="overview-label">${label}</div>
          <div class="name" data-portfolio-code="${escapeHtml(pair.holdingTicker || app.holdingCodeById?.[pair.id] || '')}" data-portfolio-price="${escapeHtml(pair.current?.holdingPrice ?? '')}">${escapeHtml(pair.name)}</div>
          <div class="leader-ratio-block">
            <div class="ratio-val">${formatRatio(pair.current.ratio)}</div>
            <div class="ratio-change ${dir}">${formatSignedPoints(pair.current.ratioChange)}</div>
          </div>
          <div class="overview-detail">${escapeHtml(pair.holdingName || '')} 기준</div>
        </div>
        ${renderRankList(runners)}
      </div>
    </div>`;
  }

  function renderRankList(runners) {
    if (!runners || !runners.length) return '';
    return `<div class="overview-rank-list">${runners.map(function(pair) {
      const active = app.pairs[app.selectedIdx] && app.pairs[app.selectedIdx].id === pair.id ? ' active' : '';
      const dir = getDirectionClass(pair.current.ratioChange);
      return `<button type="button" class="overview-rank-item${active}" data-pair-id="${escapeHtml(pair.id)}">
        <span class="overview-rank-name" data-portfolio-code="${escapeHtml(pair.holdingTicker || app.holdingCodeById?.[pair.id] || '')}" data-portfolio-price="${escapeHtml(pair.current?.holdingPrice ?? '')}">${escapeHtml(pair.name)}</span>
        <span class="overview-rank-meta">
          <span>${formatRatio(pair.current.ratio)}</span>
          <span class="ratio-change ${dir}">${formatSignedPoints(pair.current.ratioChange)}</span>
        </span>
      </button>`;
    }).join('')}</div>`;
  }

  // --- Cards ---
  // 잔존자본·실질가치 — data/fundamentals.json이 있는 종목만 노출한다(추이 없이 현재 상태만).
  function renderResidualRows(pair) {
    const effective = buildEffectiveValue(pair);
    if (!effective) return '';
    const warned = effective.warnings.length ? ' *' : '';
    return `<span class="amount-label">잔존자본</span><span class="amount-value">${formatOk(Math.round(effective.residualEquityOk))} (${formatRatio(effective.residualRatio)})</span>
          <span class="amount-label">실질가치${warned}</span><span class="amount-value effective-ratio">${formatRatio(effective.effectiveRatio)}</span>`;
  }

  function renderEffectiveCell(pair) {
    const effective = buildEffectiveValue(pair);
    if (!effective) return '-';
    const title = `잔존자본 ${formatOk(Math.round(effective.residualEquityOk))} (${formatRatio(effective.residualRatio)}) · `
      + `자본총계 ${effective.equityReport || '-'} / 장부가액 ${effective.bookValueReport || '-'}`;
    return `<strong title="${escapeHtml(title)}">${formatRatio(effective.effectiveRatio)}</strong>`;
  }

  function renderCards() {
    const el = document.getElementById('cards');
    el.innerHTML = app.pairs.map((p, i) => {
      const c = p.current;
      const dir = getDirectionClass(c.ratioChange);
      let prices = '';
      if (!p.isAverage) {
        let subLines = '';
        // 자회사 티커는 config.json(app.pairConfigById) → fundamentals 순으로 이름을 맞춰 찾는다
        // (resolveSubsidiaryTicker, js/dashboard-core.js). 보유 배지(data-portfolio-code)용.
        const pairConfig = app.pairConfigById && typeof app.pairConfigById.get === 'function'
          ? app.pairConfigById.get(p.id) : null;
        if (c.subsidiaries) {
          subLines = c.subsidiaries.map(s => renderPriceLine(s.name, s.price, s.change, `(${formatRatio(s.ratio)})`,
            resolveSubsidiaryTicker(p, s.name, pairConfig))).join('');
        } else {
          const subTicker = resolveSubsidiaryTicker(p, p.subsidiaryName, pairConfig)
            || resolveSubsidiaryTicker(p, null, pairConfig);
          subLines = renderPriceLine(p.subsidiaryName, c.subsidiaryPrice, c.subsidiaryChange, null, subTicker);
        }
        // 저장된 종가 백분위 대신 현재 카드 비율로 계산한다. 표본 30개 미만은 생략.
        const percentiles = computePairPercentiles(p);
        let pctileRow = '';
        if (percentiles.pctile1y !== null || percentiles.pctile3y !== null) {
          const pct = (v) => typeof v === 'number' ? v + '%' : '-';
          pctileRow = `<span class="amount-label" title="현재 비율의 최근 1년 / 3년 백분위 · 각 기간 표본 중 현재 비율 이하인 비중">백분위 1y/3y</span><span class="amount-value">${pct(percentiles.pctile1y)} / ${pct(percentiles.pctile3y)}</span>`;
        }
        prices = `<div class="prices">
          ${renderPriceLine(p.holdingName, c.holdingPrice, c.holdingChange, null, p.holdingTicker || app.holdingCodeById?.[p.id])}
          ${subLines}
        </div>
        <div class="amounts">
          <span class="amount-label">보유지분</span><span class="amount-value">${formatOk(c.holdingValue)}</span>
          <span class="amount-label">조정시총</span><span class="amount-value">${formatOk(c.marketCap)}</span>
          ${renderResidualRows(p)}
          ${pctileRow}
        </div>`;
      }
      const pinned = app.isPairPinned(p);
      const pinButton = p.isAverage ? '' : `<button type="button" class="card-pin${pinned ? ' pinned' : ''}" data-pin-idx="${i}" aria-label="관심종목 ${pinned ? '해제' : '등록'}" aria-pressed="${pinned}">${pinned ? '★' : '☆'}</button>`;
      return `<div class="card${i === app.selectedIdx ? ' active' : ''}${p.isAverage ? ' avg-card' : ''}" data-idx="${i}" role="button" tabindex="0" aria-pressed="${i === app.selectedIdx}">
        ${pinButton}
        <div class="name" data-portfolio-code="${escapeHtml(p.isAverage ? '' : (p.holdingTicker || app.holdingCodeById?.[p.id] || ''))}" data-portfolio-price="${escapeHtml(c.holdingPrice ?? '')}">${escapeHtml(p.name)}</div>
        <div class="ratio-line">
          <div class="ratio-val">${formatRatio(c.ratio)}</div>
          <div class="ratio-change ${dir}">${formatSignedPoints(c.ratioChange)}</div>
        </div>
        ${prices}
        ${p.isAverage ? '' : renderOwnershipLink(p.holdingTicker || p.id, p.holdingName, false)}
      </div>`;
    }).join('');
    el.querySelectorAll('.card').forEach(card => {
      card.addEventListener('click', () => {
        app.setSelectedIdx(parseInt(card.dataset.idx), { syncUrl: true });
      });
      // 키보드 접근: role="button" 카드에서 Enter/Space로 선택 (내부 핀 버튼 키 입력은 제외)
      card.addEventListener('keydown', event => {
        if (event.target !== card) return;
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        const idx = parseInt(card.dataset.idx);
        app.setSelectedIdx(idx, { syncUrl: true });
        // 재렌더로 기존 노드가 사라지므로 같은 카드에 포커스 복원
        const next = el.querySelector('.card[data-idx="' + idx + '"]');
        if (next) next.focus();
      });
    });
    el.querySelectorAll('.card-pin').forEach(button => {
      button.addEventListener('click', event => {
        event.stopPropagation();
        app.togglePinnedPair(parseInt(button.dataset.pinIdx, 10));
      });
    });
  }

  // --- Table ---
  function renderTable() {
    const maxRatio = Math.max(...app.pairs.filter(p => !p.isAverage).map(p => p.current.ratio));

    document.getElementById('tableBody').innerHTML = app.pairs.map(p => {
      const c = p.current;
      const dir = c.ratioChange >= 0 ? 'up' : 'down';
      const arrow = c.ratioChange >= 0 ? '▲' : '▼';
      const barW = (c.ratio / maxRatio * 100).toFixed(1);
      if (p.isAverage) {
        return `<tr style="border-top:2px solid var(--accent);border-bottom:2px solid var(--accent);background:var(--surface2)">
          <td><strong>${escapeHtml(p.name)}</strong></td>
          <td></td>
          <td></td>
          <td></td>
          <td></td>
          <td class="num"><strong>${c.ratio.toFixed(2)}%</strong></td>
          <td></td>
          <td class="num ${dir}-color">${arrow} ${Math.abs(c.ratioChange).toFixed(2)}%p</td>
          <td><div class="bar-cell"><div class="bar" style="width:${barW}%;background:var(--green)"></div>${c.ratio.toFixed(1)}%</div></td>
        </tr>`;
      }
      const subPrice = c.subsidiaries
        ? c.subsidiaries.map(s => `${escapeHtml(s.name)} ${renderPriceCell(s.price, s.change)}`).join('<br>')
        : renderPriceCell(c.subsidiaryPrice, c.subsidiaryChange);
      return `<tr>
        <td><strong data-portfolio-code="${escapeHtml(p.holdingTicker || app.holdingCodeById?.[p.id] || '')}" data-portfolio-price="${escapeHtml(c.holdingPrice ?? '')}">${escapeHtml(p.name)}</strong>${renderOwnershipLink(p.holdingTicker || p.id, p.holdingName, true)}</td>
        <td class="num">${renderPriceCell(c.holdingPrice, c.holdingChange)}</td>
        <td class="num">${subPrice}</td>
        <td class="num">${formatOk(c.holdingValue)}</td>
        <td class="num">${formatOk(c.marketCap)}</td>
        <td class="num"><strong>${c.ratio.toFixed(2)}%</strong></td>
        <td class="num effective-ratio">${renderEffectiveCell(p)}</td>
        <td class="num ${dir}-color">${arrow} ${Math.abs(c.ratioChange).toFixed(2)}%p</td>
        <td><div class="bar-cell"><div class="bar" style="width:${barW}%"></div>${c.ratio.toFixed(1)}%</div></td>
      </tr>`;
    }).join('');
  }

  // --- Stats ---
  function renderStats() {
    const p = app.pairs[app.selectedIdx];
    renderHoldingsDetail(); // 히스토리와 무관 — 통계 계산 전에 먼저 갱신한다
    const hist = app.getFilteredHistory(p);
    if (!hist.length) {
      document.getElementById('statsRow').innerHTML = '';
      return;
    }
    const current = p.current.ratio;
    // 최소/최대/평균/백분위/Z-score 계산은 js/dashboard-core.js(computeRatioStats)
    const { min, max, avg, percentile, zScore } = computeRatioStats(hist.map(h => h.ratio), current);

    const effective = buildEffectiveValue(p);
    const effectiveBox = effective
      ? `<div class="stat-box" title="잔존자본 ${formatOk(Math.round(effective.residualEquityOk))} = 별도 총자본(${effective.equityReport || '-'}) − 자회사 지분 장부가액(${effective.bookValueReport || '-'})">
        <div class="label">실질가치 (지분 ${formatRatio(p.current.ratio)} + 잔존 ${formatRatio(effective.residualRatio)})</div>
        <div class="value">${formatRatio(effective.effectiveRatio)}</div></div>`
      : '';

    const statsEl = document.getElementById('statsRow');
    statsEl.innerHTML = `
      <div class="stat-box"><div class="label">현재 비율</div><div class="value">${current.toFixed(2)}%</div></div>
      ${effectiveBox}
      <div class="stat-box"><div class="label">평균</div><div class="value">${avg.toFixed(2)}%</div></div>
      <div class="stat-box"><div class="label">최저</div><div class="value">${min.toFixed(2)}%</div></div>
      <div class="stat-box"><div class="label">최고</div><div class="value">${max.toFixed(2)}%</div></div>
      <div class="stat-box" title="현재 그래프에 표시된 기간·확대 구간에서 현재 비율 이하인 표본의 비중"><div class="label">백분위 (그래프 구간)</div><div class="value">${percentile}%</div></div>
      <div class="stat-box"><div class="label">Z-SCORE (구간)</div><div class="value">${zScore === null ? '-' : (zScore > 0 ? '+' : '') + zScore.toFixed(2)}</div></div>
      <div class="stat-box"><div class="label">데이터 수</div><div class="value">${hist.length}일</div></div>
    `;
  }

  // --- 보유 지분 상세 ---
  // 선택 지주사의 자회사별 시세·보유수량·지분율·피출자법인 순이익·기중 지분 변동 표.
  // 행 조립은 js/dashboard-core.js(buildHoldingsDetailRows) — 공시(fundamentals) 없으면
  // 시세 기반 열만 채워진다. renderStats에서 함께 갱신한다(별도 export 없음).
  function formatShareCount(value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
    return value.toLocaleString('ko-KR') + '주';
  }

  function renderHoldingsChangeCell(row) {
    let text = '-';
    if (row.acqQty !== null && row.acqQty !== 0) {
      const verb = row.acqQty > 0 ? '취득' : '처분';
      const amount = row.acqAmount !== null ? ' · ' + formatPrice(Math.abs(row.acqAmount)) : '';
      text = `기중 ${row.acqQty > 0 ? '+' : '−'}${formatShareCount(Math.abs(row.acqQty))} ${verb}${amount}`;
    } else if (row.beginQty !== null && row.reportQty !== null) {
      text = row.beginQty === row.reportQty
        ? '변동 없음'
        : `${formatShareCount(row.beginQty)} → ${formatShareCount(row.reportQty)}`;
    }
    const mismatchNote = row.qtyMismatch
      ? `<div class="holdings-note" title="정기보고서 이후의 취득·처분, 무상증자·분할, 또는 설정값 미갱신 가능성">보고서 기말 ${formatShareCount(row.reportQty)} ≠ 현재 반영 ${formatShareCount(row.sharesHeld)}</div>`
      : '';
    const note = row.note ? `<div class="holdings-note">${escapeHtml(row.note)}</div>` : '';
    return `${escapeHtml(text)}${mismatchNote}${note}`;
  }

  function formatHoldingPct(value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
    if (value > 0 && value < 0.000001) return '<0.000001%';
    const digits = value >= 1 ? 2 : value >= 0.01 ? 3 : value >= 0.0001 ? 4 : 6;
    return value.toLocaleString('ko-KR', { maximumFractionDigits: digits }) + '%';
  }

  function formatNativeValue(value, currency) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
    if (currency === 'KRW') return formatOk(+(value / 1e8).toFixed(Math.abs(value) < 1e8 ? 3 : 1));
    if (currency === 'USD') return '$' + value.toLocaleString('ko-KR', { maximumFractionDigits: 2 });
    return value.toLocaleString('ko-KR', { maximumFractionDigits: 2 }) + ' ' + (currency || '');
  }

  function formatHoldingsSummary(valueOk) {
    if (typeof valueOk !== 'number' || !Number.isFinite(valueOk)) return '-';
    return Math.abs(valueOk) >= 10000 ? (valueOk / 10000).toLocaleString('ko-KR', { maximumFractionDigits: 2 }) + '조' : formatOk(valueOk);
  }

  function renderHoldingsDetail() {
    const section = document.getElementById('holdingsDetailSection');
    if (!section) return;
    const previousScroll = section.querySelector('.table-scroll');
    const scrollLeft = previousScroll ? previousScroll.scrollLeft : 0;
    const active = document.activeElement;
    const activeKey = active && section.contains(active) ? active.dataset.holdingsToggle : null;
    const moreHadFocus = active && section.contains(active) && active.hasAttribute('data-holdings-more');
    const scrollHadFocus = active === previousScroll;
    const pair = app.pairs[app.selectedIdx];
    const history = pair ? app.getFilteredHistory(pair) : [];
    const graph = app.ownershipGraphStatus === 'unavailable' ? undefined : app.ownershipGraph || null;
    const overview = buildHoldingsOverview(pair, graph, history);
    if (!overview || !overview.directRows.length) {
      section.style.display = 'none';
      section.innerHTML = '';
      return;
    }
    const contribution = overview.contribution;
    const graphNodes = new Map((graph && graph.nodes || []).map(node => [node.id, node]));
    const sourceStock = pair.holdingTicker || pair.id;
    const moreExpanded = expandedIndirectPairs.has(pair.id);
    const visibleIndirectCount = 4;
    const formatDelta = value => typeof value === 'number' && Number.isFinite(value)
      ? (value > 0 ? '+' : '') + value.toFixed(1) + '%p' : '-';
    const range = history.length >= 2 ? history[0].date + ' → ' + history[history.length - 1].date : '구간 데이터 없음';
    const directValue = typeof pair.current.holdingValue === 'number' ? pair.current.holdingValue : overview.totals.directSum;
    const totalDelta = contribution ? contribution.totalDelta : null;

    function contributionCell(row) {
      if (row.type === 'indirect') return '<span class="holdings-group-note" title="중복 평가를 막기 위해 보유 합계와 비율 기여에서 제외합니다">합계 미포함</span>';
      const currentSub = (pair.current.subsidiaries || []).find(sub => sub.name === row.name);
      const currentRatio = currentSub && typeof currentSub.ratio === 'number' ? currentSub.ratio
        : typeof row.valueOk === 'number' && pair.current.marketCap > 0 ? row.valueOk / pair.current.marketCap * 100 : null;
      const delta = row.contribution && row.contribution.delta;
      const deltaText = typeof delta === 'number' ? formatDelta(delta) : '-';
      const width = typeof delta === 'number' && contribution ? Math.min(100, Math.abs(delta) / contribution.maxAbs * 100) : 0;
      return `<span class="holdings-contribution-value">${formatRatio(currentRatio)}</span><span class="holdings-contribution-delta ${getDirectionClass(delta)}-color" title="선택 구간 비율 변화: ${escapeHtml(range)}">${deltaText}</span>${typeof delta === 'number' ? `<div class="holdings-contribution-track" aria-hidden="true"><div class="holdings-contribution-bar ${getDirectionClass(delta)}-color" style="width:${width}%"></div></div>` : ''}`;
    }

    function detailContent(row) {
      if (row.type === 'indirect') {
        const paths = row.paths || [];
        const pathRows = paths.slice(0, 12).map(function(path) {
          const names = path.nodeIds.map(id => graphNodes.get(id)?.name || id).join(' → ');
          const formula = path.stakePcts.map(formatHoldingPct).join(' × ') + ' = ' + formatHoldingPct(path.pct);
          return `<li><strong>${escapeHtml(names)}</strong><div class="ownership-path-formula">${escapeHtml(formula)}</div></li>`;
        }).join('');
        const node = row.node || graphNodes.get(row.ticker) || {};
        const stamp = node.marketCapAsOf || node.marketCapObservedAt;
        const timestamp = stamp ? ownershipTimestamp(stamp) : null;
        const time = timestamp !== null ? (node.marketCapAsOf ? '시세 기준 ' : '시세 조회 ') + formatKstTimestamp(new Date(timestamp)) + ' KST' : '시세 시점 미확인';
        return `<div class="holdings-detail-item"><span>간접 환산 지분율</span><strong>${escapeHtml(formatHoldingPct(row.indirectPct))}</strong></div><div class="holdings-detail-item"><span>대상 종류주식 시총</span><strong>${escapeHtml(formatNativeValue(row.rawTargetMarketCap, row.currency))}</strong></div><div class="holdings-detail-item"><span>간접 환산가치</span><strong>${escapeHtml(formatNativeValue(row.nativeValueRaw, row.currency))}</strong></div><div class="holdings-detail-note">환산가치 = 간접 지분율 × 대상 시총. ${escapeHtml(time)}. ${row.currency === 'USD' ? 'USD 참고값이며 원화 보유 합계에 포함하지 않습니다. ' : ''}중간 회사의 지분가치에 이미 반영된 하위 지분이므로 보유 합계에 더하지 않습니다.</div><ol class="holdings-path-list">${pathRows}</ol>${paths.length > 12 ? '<div class="holdings-detail-note">나머지 경로는 지분 연결도에서 확인할 수 있습니다.</div>' : ''}${(row.caveats || []).map(note => `<div class="holdings-detail-note">${escapeHtml(note)}</div>`).join('')}`;
      }
      const periodText = row.contribution
        ? `${formatRatio(row.contribution.first)} → ${formatRatio(row.contribution.last)} (${formatDelta(row.contribution.delta)})`
        : '선택 구간 양 끝의 데이터가 없어 계산하지 않습니다.';
      const disclosed = typeof row.reportedStakePct === 'number'
        ? formatHoldingPct(row.reportedStakePct) + (overview.reportDate ? ' · ' + overview.reportDate : '') : '-';
      const basis = row.edge && row.edge.stakeSource === 'calculated'
        ? `등록 수량 ${formatShareCount(row.sharesHeld)} ÷ 대상 주식수 ${formatShareCount(row.edge.totalShares)}`
        : row.edge && row.edge.stakeSource === 'disclosure' ? '공시 기말 지분율' : '등록·공시 자료 기준';
      return `<div class="holdings-detail-item"><span>보유주식수</span><strong>${formatShareCount(row.sharesHeld)}</strong></div><div class="holdings-detail-item"><span>공시 기말 지분율</span><strong>${escapeHtml(disclosed)}</strong></div><div class="holdings-detail-item"><span>피출자법인 최근 순이익</span><strong>${formatPrice(row.netIncome)}</strong></div><div class="holdings-detail-item"><span>공시 기간 지분 변동</span>${renderHoldingsChangeCell(row)}</div><div class="holdings-detail-item"><span>선택 구간 비율 기여</span><strong>${escapeHtml(periodText)}</strong></div><div class="holdings-detail-item"><span>현재 지분율 기준</span>${escapeHtml(basis)}</div><div class="holdings-detail-note">${escapeHtml(range)} · 기여 비율 = 해당 보유지분 평가가치 ÷ ${escapeHtml(pair.holdingName)} 조정시총. 기간 변화는 모회사 주가 변동도 반영합니다. 순이익은 ${escapeHtml(overview.reportLabel || '수집된 공시')}에 기재된 피출자법인 최근사업연도 값입니다.</div>`;
    }

    function holdingRow(row, index) {
      const indirect = row.type === 'indirect';
      const key = pair.id + ':' + row.type + ':' + (row.ticker || row.name);
      const id = 'holding-detail-' + encodeURIComponent(key).replace(/%/g, '_');
      const extra = indirect && index >= visibleIndirectCount;
      const shown = !extra || moreExpanded;
      const expanded = expandedHoldings.has(key);
      const price = indirect ? (row.node || graphNodes.get(row.ticker) || {}).price : row.price;
      const priceText = indirect && row.currency === 'USD'
        ? (typeof price === 'number' ? '$' + price.toLocaleString('ko-KR', { maximumFractionDigits: 2 }) : '-')
        : indirect ? formatPrice(price) : renderPriceCell(price, row.change);
      const valueText = indirect ? formatNativeValue(row.nativeValueRaw, row.currency) : formatOk(row.valueOk);
      const stake = indirect ? row.indirectPct : row.stakePct;
      const meta = indirect ? `${row.paths.length}개 경로 · ${row.minSteps === row.maxSteps ? row.minSteps : row.minSteps + '–' + row.maxSteps}단계` : row.edge?.relationshipKind === 'tracked-indirect' ? '중간 법인 경유' : '';
      const extraAttrs = extra ? ' data-holdings-extra' : '';
      return `<tr class="${indirect ? 'holdings-indirect-row' : 'holdings-registered-row'}" data-holding-kind="${row.type}" data-holding-ticker="${escapeHtml(row.ticker || '')}"${extraAttrs}${shown ? '' : ' hidden'}><td><div class="holdings-stock"><div class="holdings-stock-name"><strong data-portfolio-code="${escapeHtml(row.ticker || '')}"${indirect ? '' : ` data-portfolio-price="${escapeHtml(price ?? '')}"`}>${escapeHtml(row.name)}</strong>${renderOwnershipLink(sourceStock, row.name, true, row.ticker)}</div><div class="holdings-stock-meta"><span class="holdings-kind${indirect ? ' is-indirect' : ''}">${indirect ? '간접' : '보유'}</span><span>${escapeHtml(row.ticker ? getTickerCode(row.ticker) : '')}</span>${meta ? `<span>${escapeHtml(meta)}</span>` : ''}</div></div></td><td class="num">${escapeHtml(formatHoldingPct(stake))}</td><td class="num"><strong>${escapeHtml(valueText)}</strong></td><td class="num">${contributionCell(row)}</td><td class="num">${priceText}</td><td class="holdings-row-actions"><button type="button" class="holdings-detail-toggle" data-holdings-toggle="${escapeHtml(key)}" aria-expanded="${expanded}" aria-controls="${id}" aria-label="${escapeHtml(row.name)} ${indirect ? '간접 보유 계산' : '보유 지분'} 상세">${expanded ? '접기' : '상세'}</button></td></tr><tr class="holdings-detail-row" id="${id}" data-holdings-detail-key="${escapeHtml(key)}"${extraAttrs}${shown && expanded ? '' : ' hidden'}><td colspan="6"><div class="holdings-detail-content">${detailContent(row)}</div></td></tr>`;
    }

    const directRows = overview.directRows.map(holdingRow).join('');
    const indirectRows = overview.indirectRows.map(holdingRow).join('');
    const groupText = overview.indirectStatus === 'loading' ? '간접 보유 정보를 불러오는 중…'
      : overview.indirectStatus === 'unavailable' ? '지분 연결도에서 연결 정보를 다시 불러올 수 있습니다.' : '';
    const extraCount = Math.max(0, overview.indirectRows.length - visibleIndirectCount);
    const sourceText = overview.reportLabel ? `공시 ${overview.reportLabel}${overview.reportDate ? ' · ' + overview.reportDate : ''}` : '공시 상세 수집 전';
    section.innerHTML = `<div class="holdings-analysis-header"><h2>보유 지분 분석 — <span data-portfolio-code="${escapeHtml(pair.holdingTicker || '')}" data-portfolio-price="${escapeHtml(pair.current.holdingPrice ?? '')}">${escapeHtml(pair.holdingName || pair.name)}</span></h2>${renderOwnershipLink(sourceStock, pair.holdingName, false)}</div><div class="holdings-analysis-meta">현재 평가가치와 선택 구간의 비율 변화를 함께 봅니다. ${escapeHtml(sourceText)}${app.stockData.lastUpdated ? ' · 보유 시세 ' + escapeHtml(app.stockData.lastUpdated) : ''}</div><div class="holdings-analysis-summary"><div class="holdings-summary-item"><span class="holdings-summary-label">보유지분 가치</span><strong class="holdings-summary-value" title="${formatOk(directValue)}">${formatHoldingsSummary(directValue)}</strong><span class="holdings-summary-note">반영 보유 ${overview.directRows.length}종목</span></div><div class="holdings-summary-item"><span class="holdings-summary-label">현재 비율</span><strong class="holdings-summary-value">${formatRatio(pair.current.ratio)}</strong><span class="holdings-summary-note">보유지분 가치 / 조정시총</span></div><div class="holdings-summary-item"><span class="holdings-summary-label">선택 구간 비율 변화</span><strong class="holdings-summary-value ${getDirectionClass(totalDelta)}-color">${formatDelta(totalDelta)}</strong><span class="holdings-summary-note">${escapeHtml(range)}</span></div></div><div class="table-scroll" tabindex="0" role="region" aria-label="${escapeHtml(pair.holdingName)} 보유 지분 표. 좌우로 스크롤할 수 있습니다."><table class="holdings-analysis-table"><thead><tr><th>보유 종목</th><th class="num">지분율</th><th class="num">현재 평가가치</th><th class="num" title="위: 현재 평가가치 / 모회사 조정시총. 아래: 선택 구간 시작 대비 종료 비율 변화(%p)">현재 비율 기여 / 구간 변화</th><th class="num">현재가</th><th>상세</th></tr></thead><tbody>${directRows}<tr class="holdings-total-row"><td>보유 합계</td><td></td><td class="num" data-holdings-total>${formatOk(directValue)}</td><td class="num"><span class="holdings-contribution-value">${formatRatio(pair.current.ratio)}</span><span class="holdings-contribution-delta ${getDirectionClass(totalDelta)}-color">${formatDelta(totalDelta)}</span></td><td colspan="2"></td></tr>${overview.indirectRows.length ? `<tr class="holdings-group-row"><td colspan="6"><strong>간접 보유 환산 · ${overview.indirectRows.length}종목</strong><span class="holdings-group-note">중간 회사에 반영된 지분 · 합계 미포함</span></td></tr>${indirectRows}` : groupText ? `<tr class="holdings-group-row"><td colspan="6">${groupText}</td></tr>` : ''}</tbody></table></div>${extraCount ? `<button type="button" class="holdings-more-button" data-holdings-more aria-expanded="${moreExpanded}">${moreExpanded ? '간접 보유 접기' : '간접 보유 ' + extraCount + '종목 더 보기'}</button>` : ''}`;
    section.style.display = '';
    const scroll = section.querySelector('.table-scroll');
    if (scroll) scroll.scrollLeft = scrollLeft;
    section.querySelectorAll('[data-holdings-toggle]').forEach(function(button) {
      button.addEventListener('click', function() {
        const key = button.dataset.holdingsToggle;
        const expanded = !expandedHoldings.has(key);
        if (expanded) expandedHoldings.add(key); else expandedHoldings.delete(key);
        document.getElementById(button.getAttribute('aria-controls')).hidden = !expanded;
        button.setAttribute('aria-expanded', String(expanded));
        button.textContent = expanded ? '접기' : '상세';
      });
    });
    const more = section.querySelector('[data-holdings-more]');
    if (more) more.addEventListener('click', function() {
      const expanded = !expandedIndirectPairs.has(pair.id);
      if (expanded) expandedIndirectPairs.add(pair.id); else expandedIndirectPairs.delete(pair.id);
      section.querySelectorAll('tr[data-holdings-extra]').forEach(function(row) {
        row.hidden = !expanded || (row.dataset.holdingsDetailKey !== undefined && !expandedHoldings.has(row.dataset.holdingsDetailKey));
      });
      more.setAttribute('aria-expanded', String(expanded));
      more.textContent = expanded ? '간접 보유 접기' : '간접 보유 ' + extraCount + '종목 더 보기';
    });
    if (activeKey) {
      const replacement = Array.from(section.querySelectorAll('[data-holdings-toggle]')).find(button => button.dataset.holdingsToggle === activeKey);
      if (replacement) replacement.focus({ preventScroll: true });
    } else if (moreHadFocus && more) more.focus({ preventScroll: true });
    else if (scrollHadFocus && scroll) scroll.focus({ preventScroll: true });
  }

  return {
    updateLastUpdatedText,
    renderTodayOverview,
    renderCards,
    renderTable,
    renderStats,
  };
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export (구조 계약 테스트용).
// 렌더 함수는 DOM 의존이라 브라우저에서만 실제 동작한다.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createDashboardRenderers };
}
