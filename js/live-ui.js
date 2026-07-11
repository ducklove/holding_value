// js/live-ui.js — 실시간 갱신: current.json 폴링 + 브라우저 프록시 실시간 조회 + 자동 새로고침 (DOM 의존)
// classic script 전역 공유. 로드 순서 계약(index.html):
//   format.js → calc.js → dashboard-core.js → render.js → charts-ui.js → live-ui.js → app-boot.js
// app-boot.js(startDashboard)가 createDashboardLive(app)를 호출해 반환 함수를 app에 붙인다.
// 스냅샷 병합 규칙(getSnapshotVersion/upsertTodayHistory/buildLivePairEntry)과 재시도 정책
// (isRetryableLiveFetchError/getLiveFetchRetryDelayMs)은 dashboard-core.js 전역,
// 시세 파싱(parseProxyQuote/buildProxyMarketMetric/rowsFromColumnar …)은 calc.js 전역을 쓴다.

// --- 갱신 주기/타임아웃/프록시 상수 ---
const AUTO_REFRESH_INTERVAL_MS = 300 * 1000;
const LIVE_REFRESH_MAX_AGE_MS = 60 * 1000;
const LIVE_REFRESH_RETRY_DELAY_MS = 1500;
const LIVE_REFRESH_RETRY_ATTEMPTS = 3;
const LIVE_REFRESH_FORCE_ATTEMPTS = 3;
const LIVE_FETCH_TIMEOUT_MS = 8000;
const LIVE_REFRESH_TIMEOUT_MS = 20000;
const LIVE_REFRESH_FORCE_TIMEOUT_MS = 35000;
const LIVE_PROXY_BASE_URL = 'https://cantabile.tplinkdns.com:3298';
const LIVE_FETCH_BATCH_SIZE = 100;
const LIVE_FETCH_BATCH_CONCURRENCY = 1;
const LIVE_FETCH_MIN_INTERVAL_MS = 100;
const LIVE_FETCH_RETRY_ATTEMPTS = 3;
// LIVE_FETCH_RETRY_*_MS·MIN_ZOOM_SPAN 상수는 js/dashboard-core.js

function createDashboardLive(app) {
  // 분할 히스토리 진행 중 요청 캐시
  const historyRequests = {};
  let holdingConfigPromise = null;
  let holdingConfigById = null;
  let refreshInFlight = false;
  let forceRefreshInFlight = false;
  let lastRefreshStartedAt = 0;
  let autoRefreshTimer = null;
  let liveRefreshPromise = null;
  let nextLiveFetchAt = 0;

  // --- Apply live data ---
  function applyCurrentData(snapshot) {
    const currentData = snapshot || window.CURRENT_DATA;
    if (!currentData) return false;
    const incomingVersion = getSnapshotVersion(currentData);
    if (incomingVersion && app.latestAppliedSnapshotMs && incomingVersion < app.latestAppliedSnapshotMs) {
      return false;
    }
    const currentMap = {};
    for (const p of currentData.pairs || []) {
      currentMap[p.id] = p;
    }
    const today = (currentData.lastUpdated || app.stockData.lastUpdated || '').slice(0, 10);
    for (const pair of app.pairs) {
      const live = currentMap[pair.id];
      if (!live) continue;
      if (pair.isAverage && live.count === undefined) {
        // 구버전(단순평균) 스냅샷은 중앙값 지표·히스토리를 덮어쓰지 않고 참고값으로만 반영
        pair.current.mean = live.ratio;
        continue;
      }
      const lastHist = pair.history.length > 0 ? pair.history[pair.history.length - 1] : null;
      pair.current.ratio = live.ratio;
      pair.current.ratioChange = live.ratioChange !== undefined && live.ratioChange !== null
        ? live.ratioChange
        : lastHist ? +(live.ratio - lastHist.ratio).toFixed(2) : 0;
      if (pair.isAverage) {
        if (live.count !== undefined) pair.current.count = live.count;
        if (live.mean !== undefined) pair.current.mean = live.mean;
      }
      if (!pair.isAverage) {
        pair.current.holdingPrice = live.holdingPrice;
        if (live.holdingChange !== undefined) {
          pair.current.holdingChange = live.holdingChange;
        }
        pair.current.holdingValue = live.holdingValue;
        pair.current.marketCap = live.marketCap;
        if (live.subsidiaryPrice !== undefined) {
          pair.current.subsidiaryPrice = live.subsidiaryPrice;
        }
        if (live.subsidiaryChange !== undefined) {
          pair.current.subsidiaryChange = live.subsidiaryChange;
        }
        if (live.subsidiaries) {
          pair.current.subsidiaries = live.subsidiaries;
        }
      }
      upsertTodayHistory(pair, live, today);
    }
    if (currentData.lastUpdated) {
      app.stockData.lastUpdated = currentData.lastUpdated;
    }
    window.CURRENT_DATA = currentData;
    app.latestAppliedSnapshotMs = incomingVersion || Date.now();
    app.latestSnapshotVersion = Math.max(app.latestSnapshotVersion || 0, app.latestAppliedSnapshotMs || 0);
    app.todayOverviewData = app.buildTodaySummary(currentData);
    return true;
  }

  // --- 분할 히스토리 지연 로드 ---
  function ensureHistory(pair) {
    if (!pair || !app.stockData.__split || (pair.history && pair.history.length)) {
      return Promise.resolve(pair);
    }
    if (historyRequests[pair.id]) return historyRequests[pair.id];
    historyRequests[pair.id] = fetch('data/history/' + encodeURIComponent(pair.id) + '.json?v=' + app.stockData.__version)
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function(columnar) {
        pair.history = rowsFromColumnar(columnar);
        applyLiveToHistory(pair);
        return pair;
      })
      .catch(function(err) {
        delete historyRequests[pair.id];
        console.warn('히스토리 로드 실패: ' + pair.id, err);
        return pair;
      });
    return historyRequests[pair.id];
  }

  // 늦게 하이드레이션된 히스토리에 최신 스냅샷의 오늘 포인트를 반영
  function applyLiveToHistory(pair) {
    const snapshot = window.CURRENT_DATA;
    if (!snapshot || !Array.isArray(snapshot.pairs)) return;
    const live = snapshot.pairs.find(function(entry) { return entry.id === pair.id; });
    if (!live) return;
    if (pair.isAverage && live.count === undefined) return; // 구버전(단순평균) 스냅샷 제외
    const today = (snapshot.lastUpdated || app.stockData.lastUpdated || '').slice(0, 10);
    upsertTodayHistory(pair, live, today);
  }

  async function loadCurrentSnapshotFile() {
    try {
      const res = await fetch('current.json?t=' + Date.now(), { cache: 'no-store' });
      if (res.ok) return await res.json();
    } catch (err) {
      console.warn('current.json 로드 실패:', err);
    }
    return null;
  }

  async function refreshCurrentPrices(opts) {
    opts = opts || {};
    const forceLiveRefresh = !!(opts.manual || opts.forceLiveRefresh);

    if (!forceLiveRefresh && document.visibilityState !== 'visible') {
      return false;
    }
    if (refreshInFlight) {
      return false;
    }

    refreshInFlight = true;
    forceRefreshInFlight = forceLiveRefresh;
    lastRefreshStartedAt = Date.now();
    updateRefreshButton();

    try {
      let baseSnapshot = {
        lastUpdated: app.stockData.lastUpdated,
        snapshotTimestamp: app.latestAppliedSnapshotMs || getSnapshotVersion(app.stockData),
      };

      const nextSnapshot = await loadCurrentSnapshotFile();
      if (nextSnapshot && applyCurrentData(nextSnapshot)) {
        baseSnapshot = nextSnapshot;
      }

      const refreshed = await refreshLivePricesWithRetry(
        baseSnapshot,
        forceLiveRefresh ? LIVE_REFRESH_FORCE_ATTEMPTS : LIVE_REFRESH_RETRY_ATTEMPTS,
        { forceRefresh: forceLiveRefresh },
      );
      if (forceLiveRefresh && !refreshed) {
        throw new Error('현재가를 새로 받지 못했습니다 (프록시/네트워크 확인)');
      }

      app.lastRefreshError = '';
      app.renderAll();
      return true;
    } catch (err) {
      console.warn('현재가 갱신 실패:', err);
      if (forceLiveRefresh) {
        app.lastRefreshError = err && err.message ? err.message : '현재가 갱신 실패';
        app.renderAll();
      }
      return false;
    } finally {
      refreshInFlight = false;
      forceRefreshInFlight = false;
      updateRefreshButton();
    }
  }

  function scheduleAutoRefresh(delayMs) {
    const nextDelay = delayMs == null ? AUTO_REFRESH_INTERVAL_MS : delayMs;
    if (autoRefreshTimer) {
      clearTimeout(autoRefreshTimer);
    }
    autoRefreshTimer = setTimeout(function() {
      if (document.visibilityState !== 'visible') {
        scheduleAutoRefresh(AUTO_REFRESH_INTERVAL_MS);
        return;
      }
      refreshCurrentPrices().finally(function() {
        scheduleAutoRefresh(AUTO_REFRESH_INTERVAL_MS);
      });
    }, nextDelay);
  }

  function bindAutoRefresh() {
    scheduleAutoRefresh();

    document.addEventListener('visibilitychange', function() {
      if (document.visibilityState !== 'visible') return;
      const elapsed = Date.now() - lastRefreshStartedAt;
      if (!lastRefreshStartedAt || elapsed >= AUTO_REFRESH_INTERVAL_MS) {
        refreshCurrentPrices();
      } else {
        scheduleAutoRefresh(Math.max(1000, AUTO_REFRESH_INTERVAL_MS - elapsed));
      }
    });
  }

  function bindRefreshButton() {
    var button = document.getElementById('refreshBtn');
    if (!button) return;
    button.addEventListener('click', function() {
      refreshCurrentPrices({ forceLiveRefresh: true });
    });
    updateRefreshButton();
  }

  function updateRefreshButton() {
    var button = document.getElementById('refreshBtn');
    if (!button) return;
    button.disabled = refreshInFlight;
    button.classList.toggle('is-refreshing', refreshInFlight);
    button.textContent = refreshInFlight
      ? forceRefreshInFlight ? '전체 현재가 갱신 중...' : '새로고침 중...'
      : '새로고침';
  }

  function isSnapshotMsStale(snapshotMs) {
    return !snapshotMs || Date.now() - snapshotMs > LIVE_REFRESH_MAX_AGE_MS;
  }

  function delay(ms) {
    return new Promise(function(resolve) { setTimeout(resolve, ms); });
  }

  function withCacheBustingParam(url) {
    return url + (url.includes('?') ? '&' : '?') + 't=' + Date.now();
  }

  function buildLiveProxyUrl(path) {
    return LIVE_PROXY_BASE_URL.replace(/\/$/, '') + path;
  }

  function shouldBypassCacheBuster(url) {
    return url.indexOf(LIVE_PROXY_BASE_URL.replace(/\/$/, '')) === 0;
  }

  async function waitForLiveFetchSlot() {
    const now = Date.now();
    const scheduledAt = Math.max(now, nextLiveFetchAt);
    nextLiveFetchAt = scheduledAt + LIVE_FETCH_MIN_INTERVAL_MS;
    const waitMs = scheduledAt - now;
    if (waitMs > 0) await delay(waitMs);
  }

  async function fetchWithTimeout(url, responseType, timeoutMs) {
    const controller = new AbortController();
    const timer = setTimeout(function() { controller.abort(); }, timeoutMs || LIVE_FETCH_TIMEOUT_MS);
    try {
      await waitForLiveFetchSlot();
      const requestUrl = shouldBypassCacheBuster(url) ? url : withCacheBustingParam(url);
      const resp = await fetch(requestUrl, {
        signal: controller.signal,
        cache: 'no-store',
      });
      if (!resp.ok) {
        const err = new Error('HTTP ' + resp.status);
        err.status = resp.status;
        err.retryAfter = resp.headers.get('Retry-After');
        try {
          err.body = (await resp.text()).slice(0, 200);
        } catch (bodyErr) {}
        throw err;
      }
      return responseType === 'text' ? await resp.text() : await resp.json();
    } finally {
      clearTimeout(timer);
    }
  }

  async function runWithTimeout(task, timeoutMs, errorMessage) {
    let timer = null;
    try {
      return await Promise.race([
        task,
        new Promise(function(_, reject) {
          timer = setTimeout(function() { reject(new Error(errorMessage)); }, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  async function fetchLiveJson(url, timeoutMs) {
    for (let attempt = 0; attempt < LIVE_FETCH_RETRY_ATTEMPTS; attempt += 1) {
      try {
        return await fetchWithTimeout(url, 'json', timeoutMs || LIVE_FETCH_TIMEOUT_MS);
      } catch (err) {
        if (attempt >= LIVE_FETCH_RETRY_ATTEMPTS - 1 || !isRetryableLiveFetchError(err)) {
          throw err;
        }
        await delay(getLiveFetchRetryDelayMs(err, attempt));
      }
    }
    return null;
  }

  async function mapWithConcurrency(items, limit, mapper) {
    const results = new Array(items.length);
    let nextIndex = 0;

    async function worker() {
      while (true) {
        const idx = nextIndex;
        nextIndex += 1;
        if (idx >= items.length) return;
        try {
          results[idx] = await mapper(items[idx], idx);
        } catch (err) {
          results[idx] = [];
        }
      }
    }

    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
    return results;
  }

  async function fetchLiveStockQuoteMap(codes) {
    const uniqueCodes = [...new Set(codes.filter(Boolean))];
    if (!uniqueCodes.length) return new Map();

    async function fetchQuoteBatch(batchCodes) {
      const payload = await fetchLiveJson(
        buildLiveProxyUrl('/v1/naverfinance/stocks/quotes?symbols=' + encodeURIComponent(batchCodes.join(','))),
      );
      if (Array.isArray(payload?.items)) {
        return payload.items.map(function(item) {
          return {
            code: String(item.symbol || item.summary?.symbol || item.raw?.cd || '').trim(),
            quote: parseProxyQuote(item),
          };
        });
      }
      if (payload?.summary || payload?.raw) {
        return [{
          code: String(payload.symbol || payload.summary?.symbol || payload.raw?.cd || batchCodes[0] || '').trim(),
          quote: parseProxyQuote(payload),
        }];
      }
      return [];
    }

    const batchResults = await mapWithConcurrency(
      chunkArray(uniqueCodes, LIVE_FETCH_BATCH_SIZE),
      LIVE_FETCH_BATCH_CONCURRENCY,
      function(batchCodes) {
        return fetchQuoteBatch(batchCodes);
      },
    );

    return new Map(
      batchResults
        .flat()
        .filter(function(result) {
          return result && result.code && result.quote && result.quote.price != null;
        })
        .map(function(result) { return [result.code, result.quote]; }),
    );
  }

  async function loadHoldingConfigMap() {
    if (holdingConfigPromise) return holdingConfigPromise;
    holdingConfigPromise = fetch('config.json?t=' + Date.now(), { cache: 'no-store' })
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function(config) {
        holdingConfigById = new Map();
        config.forEach(function(entry) {
          if (!entry.id) return;
          holdingConfigById.set(entry.id, entry);
          if (entry.holdingTicker) {
            app.holdingCodeById[entry.id] = getTickerCode(entry.holdingTicker);
          }
        });
        return holdingConfigById;
      });
    return holdingConfigPromise;
  }

  async function buildLiveSnapshot() {
    const configMap = await loadHoldingConfigMap();
    const codes = [];
    app.pairs.filter(function(pair) { return !pair.isAverage; }).forEach(function(pair) {
      const config = configMap.get(pair.id);
      if (!config) return;
      if (isKoreanTicker(config.holdingTicker)) codes.push(getTickerCode(config.holdingTicker));
      (config.subsidiaries || []).forEach(function(sub) {
        if (isKoreanTicker(sub.ticker)) codes.push(getTickerCode(sub.ticker));
      });
    });

    const uniqueCodes = [...new Set(codes.filter(Boolean))];
    const [quoteMap, market] = await Promise.all([
      fetchLiveStockQuoteMap(uniqueCodes),
      buildLiveMarketSummary().catch(function() { return null; }),
    ]);
    // 부분 성공 허용: 받은 종목만 갱신하고 누락 종목은 직전 값을 유지한다.
    // (서버 스냅샷의 isPartial/missingPairIds 계약과 동일 — 전부-아니면-실패 제거)
    const livePairs = [];
    const missingPairIds = [];
    app.pairs.filter(function(pair) { return !pair.isAverage; }).forEach(function(pair) {
      const config = configMap.get(pair.id);
      const entry = config ? buildLivePairEntry(pair, config, quoteMap) : null;
      if (entry) livePairs.push(entry);
      else missingPairIds.push(pair.id);
    });
    if (!livePairs.length) throw new Error('현재가를 하나도 받지 못했습니다 (프록시/네트워크 확인)');

    // 전체 지표는 라이브 + 누락 종목의 직전 값까지 합친 전체 구성 기준으로 계산
    const liveIds = new Set(livePairs.map(function(entry) { return entry.id; }));
    const ratioValues = livePairs.map(function(pair) { return pair.ratio; });
    const changeValues = livePairs
      .map(function(pair) { return pair.ratioChange; })
      .filter(function(value) { return typeof value === 'number' && Number.isFinite(value); });
    app.pairs.forEach(function(pair) {
      if (pair.isAverage || liveIds.has(pair.id)) return;
      if (typeof pair.current.ratio === 'number' && Number.isFinite(pair.current.ratio)) {
        ratioValues.push(pair.current.ratio);
      }
      if (typeof pair.current.ratioChange === 'number' && Number.isFinite(pair.current.ratioChange)) {
        changeValues.push(pair.current.ratioChange);
      }
    });
    const medianRatio = medianOf(ratioValues);
    const meanRatio = ratioValues.reduce(function(sum, v) { return sum + v; }, 0) / ratioValues.length;
    const medianChange = changeValues.length ? medianOf(changeValues) : null;
    livePairs.push({
      id: '_average',
      ratio: +medianRatio.toFixed(2),
      mean: +meanRatio.toFixed(2),
      count: ratioValues.length,
      ratioChange: medianChange == null ? null : +medianChange.toFixed(2),
      quoteSource: 'derived',
    });
    const now = new Date();
    return {
      source: '내부 프록시 실시간',
      lastUpdated: formatKstTimestamp(now),
      generatedAt: now.toISOString(),
      snapshotTimestamp: Date.now(),
      isPartial: missingPairIds.length > 0,
      missingPairIds: missingPairIds,
      preservedPairIds: [],
      market,
      pairs: livePairs,
    };
  }

  async function buildLiveMarketSummary() {
    const [kospiPayload, kosdaqPayload] = await Promise.all([
      fetchLiveJson(buildLiveProxyUrl('/v1/indexes/KOSPI/quote')),
      fetchLiveJson(buildLiveProxyUrl('/v1/indexes/KOSDAQ/quote')).catch(function() { return null; }),
    ]);
    const kospi = buildProxyMarketMetric(kospiPayload, { id: 'KOSPI', name: 'KOSPI', priceDecimals: 2 });
    const kosdaq = buildProxyMarketMetric(kosdaqPayload, { id: 'KOSDAQ', name: 'KOSDAQ', priceDecimals: 2 });
    if (!kospi) return null;
    if (kosdaq) kospi.extras = [kosdaq];
    return kospi;
  }

  async function refreshLivePricesIfNeeded(baseSnapshot, opts) {
    opts = opts || {};
    const baseSnapshotMs = getSnapshotVersion(baseSnapshot);
    const referenceSnapshotMs = Math.max(baseSnapshotMs || 0, app.latestAppliedSnapshotMs || 0);
    if (!opts.forceRefresh && !isSnapshotMsStale(referenceSnapshotMs)) return false;
    if (liveRefreshPromise) return liveRefreshPromise;

    liveRefreshPromise = (async function() {
      try {
        const liveSnapshot = await runWithTimeout(
          buildLiveSnapshot(),
          opts.forceRefresh ? LIVE_REFRESH_FORCE_TIMEOUT_MS : LIVE_REFRESH_TIMEOUT_MS,
          '실시간 갱신 시간이 초과되었습니다.',
        );
        return applyCurrentData(liveSnapshot);
      } catch (err) {
        console.warn('브라우저 실시간 갱신 실패:', err);
        return false;
      } finally {
        liveRefreshPromise = null;
      }
    })();
    return liveRefreshPromise;
  }

  async function refreshLivePricesWithRetry(baseSnapshot, attempts, opts) {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const refreshed = await refreshLivePricesIfNeeded(baseSnapshot, opts);
      if (refreshed || (!opts?.forceRefresh && !isSnapshotMsStale(app.latestAppliedSnapshotMs || 0))) {
        return refreshed;
      }
      if (attempt < attempts - 1) await delay(LIVE_REFRESH_RETRY_DELAY_MS);
    }
    return false;
  }

  return {
    applyCurrentData,
    ensureHistory,
    refreshCurrentPrices,
    bindAutoRefresh,
    bindRefreshButton,
  };
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export (구조 계약 테스트용).
// 실시간 갱신 함수는 fetch/document 의존이라 브라우저에서만 실제 동작한다.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createDashboardLive };
}
