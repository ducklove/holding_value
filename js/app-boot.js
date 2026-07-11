// js/app-boot.js — 데이터 로드 + 대시보드 부트스트랩(초기화 순서·선택/URL 동기화·핀·테마·CSV) (DOM 의존)
// classic script 전역 공유. 로드 순서 계약(index.html):
//   format.js → calc.js → dashboard-core.js → render.js → charts-ui.js → live-ui.js → app-boot.js
// index.html 인라인은 loadDashboardData()로 데이터를 받고 startDashboard(stockData)를 호출한다.
// startDashboard는 공유 상태를 담는 app 컨텍스트를 만들고, render/charts/live 모듈 팩토리의
// 반환 함수를 app에 붙인 뒤 초기 렌더·이벤트 배선을 수행한다.

const PINNED_PAIRS_STORAGE_KEY = 'holdingValuePinnedPairIds';

// 분할 데이터(data/summary.json + data/history/*.json 지연 로드) 우선, 실패 시 data.js 폴백.
// 첫 화면은 요약(~25KB)만으로 그리고, 차트 히스토리는 선택 종목만 받아온다.
function loadDashboardData() {
  function fromSplit() {
    return fetch('data/summary.json', { cache: 'no-store' })
      .then(function(res) {
        if (!res.ok) throw new Error('summary HTTP ' + res.status);
        return res.json();
      })
      .then(function(summary) {
        if (!summary || !Array.isArray(summary.pairs) || !summary.pairs.length) {
          throw new Error('summary 비어 있음');
        }
        var pairs = summary.pairs.map(function(pair) {
          return Object.assign({}, pair, { history: [] });
        });
        return {
          lastUpdated: summary.lastUpdated,
          pairs: pairs,
          __split: true,
          __version: encodeURIComponent(summary.lastUpdated || ''),
        };
      });
  }
  function fromLegacy() {
    return new Promise(function(resolve, reject) {
      var script = document.createElement('script');
      script.src = 'data.js';
      script.onload = function() {
        if (typeof STOCK_DATA !== 'undefined') resolve(STOCK_DATA);
        else reject(new Error('STOCK_DATA 없음'));
      };
      script.onerror = function() { reject(new Error('data.js 로드 실패')); };
      document.head.appendChild(script);
    });
  }
  return fromSplit().catch(function(err) {
    console.warn('분할 데이터 로드 실패, data.js로 폴백:', err);
    return fromLegacy();
  });
}

function startDashboard(STOCK_DATA) {
  window.STOCK_DATA = STOCK_DATA; // 콘솔 디버깅/외부 접근용
  const queryParams = new URLSearchParams(location.search);
  const initialCode = (queryParams.get('code') || '').trim();
  let holdingCodePromise = null;
  let hasRendered = false;
  const pinnedPairIds = loadPinnedPairIds();

  // 모듈 간 공유 상태 컨텍스트. 원본 인라인 스크립트의 클로저 변수를 속성으로 옮긴 것.
  // (렌더/차트/라이브 팩토리가 같은 객체를 읽고 쓴다 — 재할당이 아닌 속성 갱신만 한다.)
  const app = {
    stockData: STOCK_DATA,
    pairs: STOCK_DATA.pairs,
    selectedIdx: 0,
    periodDays: 0, // 0 = all
    pricePeriodDays: 0,
    ratioZoom: createZoomState(),
    priceZoom: createZoomState(),
    holdingCodeById: {},
    latestSnapshotVersion: 0,
    latestAppliedSnapshotMs: 0,
    todayOverviewData: null,
    lastRefreshError: '',
  };
  app.holdingCodeById = buildHoldingCodeMapFromPairs();
  app.selectedIdx = getInitialSelectedIdx();
  app.latestSnapshotVersion = getSnapshotVersion(window.CURRENT_DATA) || getSnapshotVersion(STOCK_DATA);
  app.latestAppliedSnapshotMs = app.latestSnapshotVersion || 0;

  // 부트 스코프 함수를 app에 공유(렌더/차트/라이브 모듈이 사용)
  app.buildTodaySummary = buildTodaySummary;
  app.renderAll = renderAll;
  app.setSelectedIdx = setSelectedIdx;
  app.setSelectedPairById = setSelectedPairById;
  app.isPairPinned = isPairPinned;
  app.togglePinnedPair = togglePinnedPair;
  app.getFilteredHistory = getFilteredHistory;

  Object.assign(app, createDashboardRenderers(app));
  Object.assign(app, createDashboardCharts(app));
  Object.assign(app, createDashboardLive(app));

  applyTheme(loadTheme(), false);

  // --- Apply live data ---
  app.applyCurrentData();
  sortPairsByCurrentRatio();
  app.todayOverviewData = buildTodaySummary();

  // --- Init ---
  // 분할 모드면 카드/표를 즉시 그리고, 평균·선택 종목 히스토리만 받아 차트를 그린다.
  app.updateLastUpdatedText();
  app.renderTodayOverview();
  app.renderCards();
  app.renderTable();
  Promise.all([
    app.ensureHistory(app.pairs.find(function(pair) { return pair.isAverage; })),
    app.ensureHistory(app.pairs[app.selectedIdx]),
  ]).then(function() {
    app.renderTodayOverview(); // 평균 스파크라인은 히스토리 하이드레이션 후 갱신
    app.renderChart();
    app.renderPriceChart();
    app.renderStats();
    hasRendered = true;
  });
  app.bindPeriodBtns();
  app.bindPricePeriodBtns();
  app.bindDataZoomControls();
  app.bindRefreshButton();
  bindCsvExport();
  bindThemeToggle();
  resolveSelectionFromQuery();

  // --- Auto-refresh ---
  app.bindAutoRefresh();
  app.refreshCurrentPrices();

  // Resize
  window.addEventListener('resize', () => { app.renderChart(); app.renderPriceChart(); });

  function bindCsvExport() {
    var button = document.getElementById('csvExportBtn');
    if (!button) return;
    button.addEventListener('click', function() {
      const pair = app.pairs[app.selectedIdx];
      if (!pair) return;
      const hist = getFilteredHistory(pair);
      if (!hist.length) return;

      const columns = ['date', 'holdingPrice', 'subsidiaryPrice', 'holdingValue', 'marketCap', 'ratio', 'sma250', 'ema01'];
      if (pair.isAverage) {
        columns.push('mean', 'count');
      }
      const lastEntry = hist[hist.length - 1];
      const subNames = Array.isArray(lastEntry.subsidiaries)
        ? lastEntry.subsidiaries.map(function(sub) { return sub.name; })
        : [];

      const header = columns.slice();
      subNames.forEach(function(name) {
        header.push(name + ' price', name + ' ratio');
      });
      const lines = [header.map(escapeCsvValue).join(',')];
      hist.forEach(function(entry) {
        const row = columns.map(function(key) { return escapeCsvValue(entry[key]); });
        subNames.forEach(function(name) {
          const sub = Array.isArray(entry.subsidiaries)
            ? entry.subsidiaries.find(function(item) { return item.name === name; })
            : null;
          row.push(escapeCsvValue(sub ? sub.price : null), escapeCsvValue(sub ? sub.ratio : null));
        });
        lines.push(row.join(','));
      });

      const blob = new Blob(['\ufeff' + lines.join('\n')], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = pair.id + '_' + hist[0].date + '_' + lastEntry.date + '.csv';
      a.click();
      URL.revokeObjectURL(a.href);
    });
  }

  function sortPairsByCurrentRatio() {
    var selectedPairId = app.pairs[app.selectedIdx] ? app.pairs[app.selectedIdx].id : '';
    // 정렬 비교자(comparePairOrder)는 js/dashboard-core.js — 즐겨찾기 판별 함수를 주입한다.
    app.pairs.sort(function(a, b) { return comparePairOrder(a, b, isPairPinned); });
    if (!selectedPairId) return;
    var nextIdx = app.pairs.findIndex(function(pair) {
      return pair.id === selectedPairId;
    });
    if (nextIdx >= 0) {
      app.selectedIdx = nextIdx;
    }
  }

  function loadPinnedPairIds() {
    try {
      var parsed = JSON.parse(localStorage.getItem(PINNED_PAIRS_STORAGE_KEY) || '[]');
      return new Set(Array.isArray(parsed) ? parsed.filter(function(id) {
        return typeof id === 'string';
      }) : []);
    } catch (e) {
      return new Set();
    }
  }

  function savePinnedPairIds() {
    try {
      localStorage.setItem(PINNED_PAIRS_STORAGE_KEY, JSON.stringify(Array.from(pinnedPairIds)));
    } catch (e) {
      // 저장이 막힌 환경에서는 현재 세션의 정렬만 유지한다.
    }
  }

  function isPairPinned(pair) {
    return !!pair && !pair.isAverage && pinnedPairIds.has(pair.id);
  }

  function togglePinnedPair(idx) {
    var pair = app.pairs[idx];
    if (!pair || pair.isAverage) return;
    if (isPairPinned(pair)) {
      pinnedPairIds.delete(pair.id);
    } else {
      pinnedPairIds.add(pair.id);
    }
    savePinnedPairIds();
    sortPairsByCurrentRatio();
    app.renderCards();
    app.renderTable();
  }

  function renderAll() {
    sortPairsByCurrentRatio();
    app.updateLastUpdatedText();
    app.renderTodayOverview();
    app.renderCards();
    app.renderTable();
    app.renderChart();
    app.renderPriceChart();
    app.renderStats();
  }

  function buildTodaySummary(snapshot) {
    // 계산부(buildTodaySummaryFrom)는 js/dashboard-core.js — pairs·이전 요약을 주입한다.
    return buildTodaySummaryFrom(app.pairs, snapshot, app.todayOverviewData);
  }

  function setSelectedPairById(pairId) {
    const idx = app.pairs.findIndex(function(pair) { return pair.id === pairId; });
    if (idx < 0) return;
    setSelectedIdx(idx, { syncUrl: true, scroll: true });
  }

  // --- 선택/URL 동기화 ---
  function getInitialSelectedIdx() {
    if (!initialCode) return 0;
    const normalized = initialCode.toUpperCase();
    const idx = app.pairs.findIndex(function(pair) {
      if (pair.isAverage) return false;
      const code = resolvePairCode(pair).toUpperCase();
      return code === normalized || pair.id === initialCode;
    });
    return idx >= 0 ? idx : 0;
  }

  function setSelectedIdx(idx, opts) {
    opts = opts || {};
    app.selectedIdx = idx;
    app.renderTodayOverview();
    app.renderCards();
    app.renderChart();
    app.renderPriceChart();
    app.renderStats();
    const selectedPair = app.pairs[app.selectedIdx];
    if (selectedPair && (!selectedPair.history || !selectedPair.history.length)) {
      app.ensureHistory(selectedPair).then(function() {
        if (app.pairs[app.selectedIdx] !== selectedPair) return; // 대기 중 선택 변경
        app.renderChart();
        app.renderPriceChart();
        app.renderStats();
      });
    }
    if (opts.syncUrl) syncSelectedCodeToUrl();
    if (opts.scroll) scrollToRatioChart();
  }

  function syncSelectedCodeToUrl() {
    const pair = app.pairs[app.selectedIdx];
    const url = new URL(location.href);
    const code = pair ? resolvePairCode(pair) : '';
    if (pair && !pair.isAverage && code) {
      url.searchParams.set('code', code);
    } else {
      url.searchParams.delete('code');
    }
    history.replaceState(null, '', url.toString());
  }

  function scrollToRatioChart() {
    const section = document.getElementById('ratioChartSection');
    if (!section) return;
    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function resolveSelectionFromQuery() {
    if (!initialCode) return;
    const idx = findPairIndexByCode(initialCode);
    if (idx >= 0) {
      if (idx !== app.selectedIdx) {
        setSelectedIdx(idx);
      }
      requestAnimationFrame(function() {
        scrollToRatioChart();
      });
      return;
    }
    loadHoldingCodes().then(function() {
      const resolvedIdx = findPairIndexByCode(initialCode);
      if (resolvedIdx < 0) return;
      setSelectedIdx(resolvedIdx);
      requestAnimationFrame(function() {
        scrollToRatioChart();
      });
    }).catch(function(err) {
      console.warn('종목 코드 매핑 로드 실패:', err);
    });
  }

  function findPairIndexByCode(code) {
    const normalized = (code || '').trim().toUpperCase();
    if (!normalized) return -1;
    return app.pairs.findIndex(function(pair) {
      if (pair.isAverage) return false;
      return resolvePairCode(pair).toUpperCase() === normalized || pair.id === code;
    });
  }

  function buildHoldingCodeMapFromPairs() {
    const map = {};
    app.pairs.forEach(function(pair) {
      if (pair.holdingTicker) {
        map[pair.id] = pair.holdingTicker.split('.')[0];
      }
    });
    return map;
  }

  function loadHoldingCodes() {
    if (holdingCodePromise) return holdingCodePromise;
    holdingCodePromise = fetch('config.json')
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function(config) {
        config.forEach(function(entry) {
          if (!entry.id || !entry.holdingTicker) return;
          app.holdingCodeById[entry.id] = entry.holdingTicker.split('.')[0];
        });
        return app.holdingCodeById;
      });
    return holdingCodePromise;
  }

  function resolvePairCode(pair) {
    if (!pair || pair.isAverage) return '';
    if (pair.holdingTicker) return pair.holdingTicker.split('.')[0];
    return app.holdingCodeById[pair.id] || '';
  }

  function getFilteredHistory(pair) {
    return applyZoomToHistory(filterHistoryByDays(pair.history, app.periodDays), app.ratioZoom);
  }

  // --- 테마 ---
  function loadTheme() {
    const urlTheme = new URLSearchParams(location.search).get('theme');
    if (urlTheme === 'dark' || urlTheme === 'light') return urlTheme;
    return localStorage.getItem('theme') || 'light';
  }

  function applyTheme(theme, persist) {
    const nextTheme = theme === 'light' ? 'light' : 'dark';
    document.documentElement.dataset.theme = nextTheme;
    if (persist) localStorage.setItem('theme', nextTheme);
    updateThemeButtons();
    if (hasRendered) {
      app.renderChart();
      app.renderPriceChart();
    }
  }

  function toggleTheme() {
    const theme = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
    applyTheme(theme === 'dark' ? 'light' : 'dark', true);
  }

  function updateThemeButtons() {
    const theme = document.documentElement.dataset.theme || 'light';
    const button = document.getElementById('themeToggle');
    if (!button) return;
    button.setAttribute('aria-label', theme === 'dark' ? '일반 모드로 전환' : '다크 모드로 전환');
    button.title = '테마 전환';
  }

  function bindThemeToggle() {
    const toggle = document.getElementById('themeToggle');
    updateThemeButtons();
    if (!toggle) return;
    toggle.addEventListener('click', toggleTheme);
  }
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export (구조 계약 테스트용).
// 부트 함수는 DOM/fetch 의존이라 브라우저에서만 실제 동작한다.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { PINNED_PAIRS_STORAGE_KEY, loadDashboardData, startDashboard };
}
