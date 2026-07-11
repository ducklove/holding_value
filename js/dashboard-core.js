// js/dashboard-core.js — index.html 인라인 스크립트에서 추출한 DOM 비의존 순수 로직
// (classic script 전역 공유. GitHub Pages 정적 서빙 환경이라 ES module 대신 전역 함수로 공유한다.)
// 로드 순서: js/format.js → js/calc.js → js/dashboard-core.js → index.html 본체(startDashboard).
// 브라우저에선 format.js/calc.js가 만든 전역을 그대로 쓰고,
// Node(node:test) 환경에서만 require로 같은 이름을 파일 스코프에 바인딩한다.
// (브라우저에서 아래 var 선언은 이미 존재하는 전역 바인딩에 대한 no-op이다.)
if (typeof module !== 'undefined' && module.exports) {
  var __format = require('./format.js');
  var parseSeoulTimestamp = __format.parseSeoulTimestamp;
  var parseDateKey = __format.parseDateKey;
  var formatDateKey = __format.formatDateKey;
  var getTickerCode = __format.getTickerCode;
  var __calc = require('./calc.js');
  var clamp = __calc.clamp;
  var medianOf = __calc.medianOf;
  var isKoreanTicker = __calc.isKoreanTicker;
  var derivePreviousPrice = __calc.derivePreviousPrice;
}

// --- 상수 (startDashboard에서 이동) ---
const MIN_ZOOM_SPAN = 0.04;
const LIVE_FETCH_RETRY_BASE_DELAY_MS = 1000;
const LIVE_FETCH_RATE_LIMIT_DELAY_MS = 3500;
const LIVE_FETCH_RETRY_MAX_DELAY_MS = 7000;

// --- 스냅샷 버전/병합 ---
// 스냅샷 버전(ms). 버전 역행 가드(applyCurrentData)의 비교 기준.
function getSnapshotVersion(snapshot) {
  if (!snapshot) return 0;
  if (typeof snapshot.snapshotTimestamp === 'number') return snapshot.snapshotTimestamp;
  if (typeof snapshot.generatedAt === 'string') {
    var generated = Date.parse(snapshot.generatedAt);
    if (!Number.isNaN(generated)) return generated;
  }
  return parseSeoulTimestamp(snapshot.lastUpdated || '');
}

// 카드/표 정렬 비교자: 평균 → 즐겨찾기 → 비율 내림차순 → 이름(ko).
// isPinned는 호출부(startDashboard)가 즐겨찾기 판별 함수를 주입한다.
function comparePairOrder(a, b, isPinned) {
  if (a.isAverage) return b.isAverage ? 0 : -1;
  if (b.isAverage) return 1;
  var pinnedOf = isPinned || function() { return false; };
  var pinDiff = Number(!!pinnedOf(b)) - Number(!!pinnedOf(a));
  if (pinDiff !== 0) return pinDiff;
  if (b.current.ratio !== a.current.ratio) return b.current.ratio - a.current.ratio;
  return a.name.localeCompare(b.name, 'ko');
}

// 실시간 스냅샷의 오늘 포인트를 히스토리 마지막 행에 병합(같은 날짜면 갱신, 새 날짜면 append)
function upsertTodayHistory(pair, live, today) {
  if (!today || !pair.history || !pair.history.length) return;
  if (pair.isAverage) {
    const averageEntry = { date: today, ratio: live.ratio };
    if (live.count !== undefined) averageEntry.count = live.count;
    if (live.mean !== undefined) averageEntry.mean = live.mean;
    const lastAverage = pair.history[pair.history.length - 1];
    if (lastAverage && lastAverage.date === today) {
      Object.assign(lastAverage, averageEntry);
    } else {
      pair.history.push(averageEntry);
    }
    return;
  }
  const entry = {
    date: today,
    holdingPrice: live.holdingPrice,
    holdingValue: live.holdingValue,
    marketCap: live.marketCap,
    ratio: live.ratio,
  };
  if (live.subsidiaryPrice !== undefined) entry.subsidiaryPrice = live.subsidiaryPrice;
  if (live.subsidiaries) entry.subsidiaries = live.subsidiaries;

  const last = pair.history[pair.history.length - 1];
  if (last && last.date === today) {
    Object.assign(last, entry);
  } else {
    pair.history.push(entry);
  }
}

// 오늘의 현황 요약. previousSummary는 이전 요약(todayOverviewData) — source/market 폴백에만 쓴다.
function buildTodaySummaryFrom(pairs, snapshot, previousSummary) {
  const livePairs = pairs.filter(function(pair) { return !pair.isAverage; });
  const avgPair = pairs.find(function(pair) { return pair.isAverage; });
  const ratioPairs = livePairs.filter(function(pair) {
    return typeof pair.current.ratio === 'number' && Number.isFinite(pair.current.ratio);
  });
  const changePairs = livePairs.filter(function(pair) {
    return typeof pair.current.ratioChange === 'number' && Number.isFinite(pair.current.ratioChange);
  });
  const holdingChangePairs = livePairs.filter(function(pair) {
    return typeof pair.current.holdingChange === 'number' && Number.isFinite(pair.current.holdingChange);
  });
  const subsidiaryChangePairs = livePairs.flatMap(function(pair) {
    if (Array.isArray(pair.current.subsidiaries)) {
      return pair.current.subsidiaries.map(function(sub) { return sub.change; });
    }
    return [pair.current.subsidiaryChange];
  }).filter(function(value) {
    return typeof value === 'number' && Number.isFinite(value);
  });
  const sortedByExpansion = changePairs.slice().sort(function(a, b) {
    return b.current.ratioChange - a.current.ratioChange;
  });
  const sortedByContraction = changePairs.slice().sort(function(a, b) {
    return a.current.ratioChange - b.current.ratioChange;
  });
  const averageRatio = avgPair && typeof avgPair.current.ratio === 'number'
    ? avgPair.current.ratio
    : medianOf(ratioPairs.map(function(pair) { return pair.current.ratio; }));
  const averageRatioChange = avgPair && typeof avgPair.current.ratioChange === 'number'
    ? avgPair.current.ratioChange
    : medianOf(changePairs.map(function(pair) { return pair.current.ratioChange; }));
  const averageHoldingChange = holdingChangePairs.reduce(function(sum, pair) {
    return sum + pair.current.holdingChange;
  }, 0) / Math.max(1, holdingChangePairs.length);
  const averageSubsidiaryChange = subsidiaryChangePairs.reduce(function(sum, value) {
    return sum + value;
  }, 0) / Math.max(1, subsidiaryChangePairs.length);
  const market = snapshot?.market || snapshot?.summary?.market || previousSummary?.market || null;

  return {
    source: snapshot?.source || inferSnapshotSource(snapshot) || previousSummary?.source || 'current.json',
    market,
    averageRatio,
    averageRatioChange,
    averageHoldingChange,
    averageSubsidiaryChange,
    representativeCount: ratioPairs.length,
    topExpansion: sortedByExpansion[0] || null,
    expansionRunners: sortedByExpansion.slice(1, 5),
    topContraction: sortedByContraction[0] || null,
    contractionRunners: sortedByContraction.slice(1, 5),
  };
}

function inferSnapshotSource(snapshot) {
  if (!snapshot || !Array.isArray(snapshot.pairs)) return '';
  const sources = [...new Set(snapshot.pairs.map(function(pair) { return pair.quoteSource; }).filter(Boolean))];
  if (!sources.length) return '';
  if (sources.includes('internal_proxy')) return '내부 프록시 실시간';
  if (sources.includes('naver_browser')) return '네이버 증권 실시간';
  if (sources.includes('kis_proxy')) return 'KIS 프록시';
  if (sources.includes('kis_openapi')) return '한국투자증권 Open API';
  if (sources.includes('yfinance')) return 'yfinance';
  return sources.join(', ');
}

// --- CSV ---
// 쉼표·따옴표·개행 포함 값은 "..."로 감싸고 내부 따옴표는 ""로 이스케이프
function escapeCsvValue(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (/[",\n]/.test(text)) {
    return '"' + text.replace(/"/g, '""') + '"';
  }
  return text;
}

// --- 실시간 조회 재시도 정책 ---
function parseRetryAfterMs(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const parsed = Date.parse(value);
  if (Number.isFinite(parsed)) return Math.max(0, parsed - Date.now());
  return null;
}

function isRetryableLiveFetchError(err) {
  const status = err && err.status;
  if (status === 429) return true;
  if (status >= 500 && status < 600) return true;
  const name = err && err.name;
  if (name === 'AbortError') return true;
  const message = err && err.message ? err.message : '';
  return /Failed to fetch|NetworkError|Load failed/i.test(message);
}

function getLiveFetchRetryDelayMs(err, attempt) {
  const retryAfterMs = parseRetryAfterMs(err && err.retryAfter);
  if (retryAfterMs != null) return Math.min(retryAfterMs, LIVE_FETCH_RETRY_MAX_DELAY_MS);
  const baseDelay = err && err.status === 429 ? LIVE_FETCH_RATE_LIMIT_DELAY_MS : LIVE_FETCH_RETRY_BASE_DELAY_MS;
  return Math.min(LIVE_FETCH_RETRY_MAX_DELAY_MS, baseDelay * Math.pow(2, attempt));
}

// --- 실시간 종목 엔트리 조립 (프록시 시세 + 기존 스냅샷 폴백 병합 규칙) ---
function buildLivePairEntry(pair, config, quoteMap) {
  const holdingCode = getTickerCode(config.holdingTicker);
  const holdingQuote = quoteMap.get(holdingCode);
  if (!holdingQuote || holdingQuote.price == null) return null;
  const adjustedShares = config.holdingAdjustedShares || ((config.holdingTotalShares || 0) - (config.holdingTreasuryShares || 0));
  if (!adjustedShares) return null;

  const marketCap = adjustedShares * holdingQuote.price;
  let holdingValue = 0;
  let previousHoldingValue = 0;
  let hasPreviousRatio = holdingQuote.previousPrice != null && holdingQuote.previousPrice !== 0;
  let usedFallback = false;
  const subDetails = [];
  const currentSubMap = new Map((pair.current.subsidiaries || []).map(function(sub) { return [sub.name, sub]; }));

  (config.subsidiaries || []).forEach(function(sub) {
    const quote = isKoreanTicker(sub.ticker) ? quoteMap.get(getTickerCode(sub.ticker)) : null;
    const fallback = currentSubMap.get(sub.name) || null;
    const fallbackPrice = fallback ? fallback.price : pair.current.subsidiaryPrice;
    const fallbackChange = fallback ? fallback.change : pair.current.subsidiaryChange;
    const price = quote && quote.price != null ? quote.price : fallbackPrice;
    if (price == null) return;
    const previousPrice = quote && quote.previousPrice != null
      ? quote.previousPrice
      : derivePreviousPrice(price, fallbackChange);
    if (!quote) usedFallback = true;

    const value = sub.sharesHeld * price;
    holdingValue += value;
    if (previousPrice == null) {
      hasPreviousRatio = false;
    } else {
      previousHoldingValue += sub.sharesHeld * previousPrice;
    }
    subDetails.push({
      name: sub.name,
      price: Math.round(price),
      change: quote && quote.changePct != null ? quote.changePct : fallbackChange,
      value: +(value / 1e8).toFixed(1),
      rawValue: value,
    });
  });

  if (!subDetails.length || !holdingValue || !marketCap) return null;
  const ratio = holdingValue / marketCap * 100;
  let previousRatio = null;
  if (hasPreviousRatio) {
    const previousMarketCap = adjustedShares * holdingQuote.previousPrice;
    if (previousMarketCap) previousRatio = previousHoldingValue / previousMarketCap * 100;
  }
  subDetails.forEach(function(sub) {
    sub.ratio = +(sub.rawValue / marketCap * 100).toFixed(2);
    delete sub.rawValue;
  });

  const entry = {
    id: pair.id,
    holdingPrice: Math.round(holdingQuote.price),
    holdingChange: holdingQuote.changePct,
    holdingValue: +(holdingValue / 1e8).toFixed(1),
    marketCap: +(marketCap / 1e8).toFixed(1),
    ratio: +ratio.toFixed(2),
    ratioChange: previousRatio != null ? +(ratio - previousRatio).toFixed(2) : null,
    quoteSource: usedFallback ? 'mixed' : 'internal_proxy',
  };
  if (subDetails.length === 1) {
    entry.subsidiaryPrice = subDetails[0].price;
    entry.subsidiaryChange = subDetails[0].change;
  } else {
    entry.subsidiaries = subDetails;
  }
  return entry;
}

// --- 데이터줌/기간 필터 ---
function normalizeZoomState(zoom) {
  if (!Number.isFinite(zoom.start)) zoom.start = 0;
  if (!Number.isFinite(zoom.end)) zoom.end = 1;
  zoom.start = clamp(zoom.start, 0, 1);
  zoom.end = clamp(zoom.end, 0, 1);
  if (zoom.end < zoom.start) {
    const tmp = zoom.start;
    zoom.start = zoom.end;
    zoom.end = tmp;
  }
  if (zoom.end - zoom.start < MIN_ZOOM_SPAN) {
    const center = (zoom.start + zoom.end) / 2;
    zoom.start = clamp(center - MIN_ZOOM_SPAN / 2, 0, 1 - MIN_ZOOM_SPAN);
    zoom.end = zoom.start + MIN_ZOOM_SPAN;
  }
}

function applyZoomToHistory(hist, zoom) {
  if (!hist || hist.length < 3) return hist || [];
  normalizeZoomState(zoom);
  if (zoom.start <= 0 && zoom.end >= 1) return hist;
  const lastIdx = hist.length - 1;
  let startIdx = Math.floor(zoom.start * lastIdx);
  let endIdx = Math.ceil(zoom.end * lastIdx);
  if (endIdx <= startIdx) endIdx = Math.min(lastIdx, startIdx + 1);
  return hist.slice(startIdx, endIdx + 1);
}

function filterHistoryByDays(hist, days) {
  if (days === 0 || hist.length < 2) return hist;
  const latest = parseDateKey(hist[hist.length - 1].date);
  latest.setDate(latest.getDate() - days);
  const cutoff = formatDateKey(latest);
  const filtered = hist.filter(h => h.date >= cutoff);
  return filtered.length >= 2 ? filtered : hist;
}

function getStackedSubsidiarySeries(hist) {
  const sample = hist.find(function(entry) {
    return Array.isArray(entry.subsidiaries) && entry.subsidiaries.length > 1;
  });
  if (!sample) return [];
  return sample.subsidiaries.map(function(sub) {
    return { name: sub.name };
  });
}

// --- 통계/기여 분해 (renderStats/renderContribution의 계산부) ---
function computeRatioStats(ratios, current) {
  const min = Math.min(...ratios);
  const max = Math.max(...ratios);
  const avg = ratios.reduce((a, b) => a + b, 0) / ratios.length;

  // Percentile
  const sorted = [...ratios].sort((a, b) => a - b);
  const rank = sorted.filter(s => s <= current).length;
  const percentile = (rank / sorted.length * 100).toFixed(0);

  // Z-score (가시 구간 표준편차 기준)
  const variance = ratios.reduce((s, r) => s + (r - avg) * (r - avg), 0) / ratios.length;
  const sd = Math.sqrt(variance);
  const zScore = sd > 0 ? (current - avg) / sd : null;

  return { min, max, avg, percentile, zScore };
}

// 자회사 기여 분해 행 계산. 분해 불가(구간 부족/단일 자회사/유효 행 없음)면 null.
function buildContributionRows(hist) {
  const last = hist.length >= 2 ? hist[hist.length - 1] : null;
  if (!last || !Array.isArray(last.subsidiaries) || last.subsidiaries.length <= 1) {
    return null;
  }

  const rows = [];
  last.subsidiaries.forEach(function(lastSub) {
    if (typeof lastSub.ratio !== 'number' || !Number.isFinite(lastSub.ratio)) return;
    let firstSub = null;
    for (const entry of hist) {
      const subs = Array.isArray(entry.subsidiaries) ? entry.subsidiaries : [];
      const match = subs.find(function(sub) {
        return sub.name === lastSub.name && typeof sub.ratio === 'number' && Number.isFinite(sub.ratio);
      });
      if (match) {
        firstSub = match;
        break;
      }
    }
    if (!firstSub) return;
    rows.push({
      name: lastSub.name,
      first: firstSub.ratio,
      last: lastSub.ratio,
      delta: lastSub.ratio - firstSub.ratio,
    });
  });
  if (!rows.length) return null;

  rows.sort(function(a, b) { return Math.abs(b.delta) - Math.abs(a.delta); });
  const maxAbs = rows.reduce(function(acc, row) { return Math.max(acc, Math.abs(row.delta)); }, 0) || 1;
  const totalDelta = last.ratio - hist[0].ratio;
  return { rows, maxAbs, totalDelta };
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export. 브라우저에선 전역 함수로 사용.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    MIN_ZOOM_SPAN,
    LIVE_FETCH_RETRY_BASE_DELAY_MS,
    LIVE_FETCH_RATE_LIMIT_DELAY_MS,
    LIVE_FETCH_RETRY_MAX_DELAY_MS,
    getSnapshotVersion,
    comparePairOrder,
    upsertTodayHistory,
    buildTodaySummaryFrom,
    inferSnapshotSource,
    escapeCsvValue,
    parseRetryAfterMs,
    isRetryableLiveFetchError,
    getLiveFetchRetryDelayMs,
    buildLivePairEntry,
    normalizeZoomState,
    applyZoomToHistory,
    filterHistoryByDays,
    getStackedSubsidiarySeries,
    computeRatioStats,
    buildContributionRows,
  };
}
