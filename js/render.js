// js/render.js — 카드·오늘의현황·종목비교표·통계·기여 분해 렌더 (DOM 의존)
// classic script 전역 공유. 로드 순서 계약(index.html):
//   format.js → calc.js → dashboard-core.js → render.js → charts-ui.js → live-ui.js → app-boot.js
// app-boot.js(startDashboard)가 createDashboardRenderers(app)를 호출해 반환 함수를
// app 컨텍스트에 붙인다. app은 공유 상태(pairs/selectedIdx/todayOverviewData …)와
// 다른 모듈의 함수(buildTodaySummary/setSelectedIdx/getFilteredHistory …)를 담는다.
// 포맷/이스케이프 헬퍼(escapeHtml, formatRatio …)와 계산부(computeRatioStats,
// buildContributionRows …)는 format.js/dashboard-core.js 전역을 그대로 쓴다.
function createDashboardRenderers(app) {
  function renderPriceLine(label, price, change, suffix) {
    const changeText = formatPriceChange(change);
    const changeHtml = changeText
      ? `<span class="price-change ${getPriceChangeClass(change)}">${changeText}</span>`
      : '<span class="price-change flat-color">-</span>';
    const suffixText = suffix ? `<span class="price-suffix">${suffix}</span>` : '<span class="price-suffix"></span>';
    return `<div class="price-line">
      <span class="price-label">${escapeHtml(label)}</span>
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
          <div class="name">${escapeHtml(pair.name)}</div>
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
        <span class="overview-rank-name">${escapeHtml(pair.name)}</span>
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
        if (c.subsidiaries) {
          subLines = c.subsidiaries.map(s => renderPriceLine(s.name, s.price, s.change, `(${formatRatio(s.ratio)})`)).join('');
        } else {
          subLines = renderPriceLine(p.subsidiaryName, c.subsidiaryPrice, c.subsidiaryChange);
        }
        // 백분위(1년/3년) 필드는 파이프라인 표본 부족 시 없을 수 있다 — 둘 다 없으면 행 생략
        let pctileRow = '';
        if (typeof c.pctile1y === 'number' || typeof c.pctile3y === 'number') {
          const pct = (v) => typeof v === 'number' ? v + '%' : '-';
          pctileRow = `<span class="amount-label">백분위 1y/3y</span><span class="amount-value">${pct(c.pctile1y)} / ${pct(c.pctile3y)}</span>`;
        }
        prices = `<div class="prices">
          ${renderPriceLine(p.holdingName, c.holdingPrice, c.holdingChange)}
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
        <div class="name">${escapeHtml(p.name)}</div>
        <div class="ratio-line">
          <div class="ratio-val">${formatRatio(c.ratio)}</div>
          <div class="ratio-change ${dir}">${formatSignedPoints(c.ratioChange)}</div>
        </div>
        ${prices}
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
          <td><strong>${c.ratio.toFixed(2)}%</strong></td>
          <td></td>
          <td class="${dir}-color">${arrow} ${Math.abs(c.ratioChange).toFixed(2)}%p</td>
          <td><div class="bar-cell"><div class="bar" style="width:${barW}%;background:var(--green)"></div>${c.ratio.toFixed(1)}%</div></td>
        </tr>`;
      }
      const subPrice = c.subsidiaries
        ? c.subsidiaries.map(s => `${escapeHtml(s.name)} ${renderPriceCell(s.price, s.change)}`).join('<br>')
        : renderPriceCell(c.subsidiaryPrice, c.subsidiaryChange);
      return `<tr>
        <td><strong>${escapeHtml(p.name)}</strong></td>
        <td>${renderPriceCell(c.holdingPrice, c.holdingChange)}</td>
        <td>${subPrice}</td>
        <td>${formatOk(c.holdingValue)}</td>
        <td>${formatOk(c.marketCap)}</td>
        <td><strong>${c.ratio.toFixed(2)}%</strong></td>
        <td class="effective-ratio">${renderEffectiveCell(p)}</td>
        <td class="${dir}-color">${arrow} ${Math.abs(c.ratioChange).toFixed(2)}%p</td>
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
      const contribSection = document.getElementById('contribSection');
      if (contribSection) contribSection.style.display = 'none';
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
      <div class="stat-box"><div class="label">백분위</div><div class="value">${percentile}%</div></div>
      <div class="stat-box"><div class="label">Z-SCORE (구간)</div><div class="value">${zScore === null ? '-' : (zScore > 0 ? '+' : '') + zScore.toFixed(2)}</div></div>
      <div class="stat-box"><div class="label">데이터 수</div><div class="value">${hist.length}일</div></div>
    `;
    renderContribution();
  }

  // --- 자회사 기여 분해 ---
  function renderContribution() {
    const section = document.getElementById('contribSection');
    if (!section) return;
    const p = app.pairs[app.selectedIdx];
    const hist = app.getFilteredHistory(p);
    // 기여 분해 행 계산은 js/dashboard-core.js(buildContributionRows)
    const contribution = p.isAverage ? null : buildContributionRows(hist);
    if (!contribution) {
      section.style.display = 'none';
      return;
    }

    const rows = contribution.rows;
    const maxAbs = contribution.maxAbs;
    const totalDelta = contribution.totalDelta;
    const last = hist[hist.length - 1];
    const formatDelta = (v) => (v > 0 ? '+' : '') + v.toFixed(1) + '%p';

    const rowsHtml = rows.map(function(row) {
      const barWidth = (Math.abs(row.delta) / maxAbs * 100).toFixed(1);
      const barColor = row.delta >= 0 ? 'var(--up)' : 'var(--down)';
      return `<div class="contrib-row">
        <span class="contrib-name">${escapeHtml(row.name)}</span>
        <span class="contrib-span">${row.first.toFixed(1)}% → ${row.last.toFixed(1)}%</span>
        <span class="contrib-delta ${getDirectionClass(row.delta)}-color">${formatDelta(row.delta)}</span>
        <div class="contrib-bar-track"><div class="contrib-bar" style="width:${barWidth}%;background:${barColor}"></div></div>
      </div>`;
    }).join('');

    // 자회사 Δ는 총 비율과 같은 분모(지주 조정시총)를 쓰므로 Σd ≈ totalDelta (반올림 차이만) — 잔차 행 불필요
    section.innerHTML = `
      <div class="contrib-head">
        <div class="contrib-title">자회사 기여 분해</div>
        <div class="contrib-range">${hist[0].date} → ${last.date}</div>
      </div>
      <div>
        ${rowsHtml}
        <div class="contrib-row contrib-total">
          <span class="contrib-name">합계 (지주가 변동 포함)</span>
          <span class="contrib-span">${hist[0].ratio}% → ${last.ratio}%</span>
          <span class="contrib-delta ${getDirectionClass(totalDelta)}-color">${formatDelta(totalDelta)}</span>
          <span></span>
        </div>
      </div>`;
    section.style.display = '';
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

  function renderHoldingsDetail() {
    const section = document.getElementById('holdingsDetailSection');
    if (!section) return;
    const pair = app.pairs[app.selectedIdx];
    const detail = pair ? buildHoldingsDetailRows(pair) : null;
    if (!detail) {
      section.style.display = 'none';
      section.innerHTML = '';
      return;
    }

    const rowsHtml = detail.rows.map(function(row) {
      const netIncomeClass = row.netIncome !== null && row.netIncome < 0 ? ' class="down-color"' : '';
      return `<tr>
        <td><strong>${escapeHtml(row.name)}</strong>${row.ticker ? ` <span class="holdings-ticker">${escapeHtml(getTickerCode(row.ticker))}</span>` : ''}</td>
        <td>${renderPriceCell(row.price, row.change)}</td>
        <td>${formatShareCount(row.sharesHeld)}</td>
        <td>${formatRatio(row.stakePct)}</td>
        <td>${formatOk(row.valueOk)}</td>
        <td${netIncomeClass}>${formatPrice(row.netIncome)}</td>
        <td>${renderHoldingsChangeCell(row)}</td>
      </tr>`;
    }).join('');

    const sourceText = detail.reportLabel
      ? `공시 출처: ${detail.reportLabel} 타법인 출자현황${detail.reportDate ? ` (기준일 ${detail.reportDate})` : ''} · 순이익은 피출자법인 최근사업연도 당기순이익(보고서 기재 단위)`
      : '공시 데이터 수집 전 — 시세 기반 항목만 표시';

    section.innerHTML = `
      <h2>보유 지분 상세 — ${escapeHtml(pair.holdingName || pair.name)}</h2>
      <div class="holdings-detail-meta">${escapeHtml(sourceText)}</div>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>보유 종목</th>
              <th>현재가</th>
              <th title="현재 지분가치 계산에 쓰는 보유주식수 (config 기준)">보유주식수</th>
              <th title="타법인 출자현황 기말 지분율">지분율</th>
              <th title="보유주식수 × 현재가">평가가치</th>
              <th title="피출자법인 최근사업연도 당기순이익 (타법인 출자현황 기재값)">최근 순이익</th>
              <th title="보고서 기간 중 취득·처분 및 보고서 이후 변동 신호">지분 변동</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    section.style.display = '';
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
