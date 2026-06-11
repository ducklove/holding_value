// js/calc.js — 순수 계산/데이터 변환 유틸 (classic script 전역 공유)
// GitHub Pages 정적 서빙 환경이라 ES module 대신 전역 함수로 공유한다.
// 로드 순서: js/format.js → js/calc.js → index.html 본체(startDashboard).
// 브라우저에선 format.js가 만든 전역(parseRawNumber/pickDefined/parseDateKey)을 그대로 쓰고,
// Node(node:test) 환경에서만 require로 같은 이름을 파일 스코프에 바인딩한다.
// (브라우저에서 아래 var 선언은 이미 존재하는 전역 바인딩에 대한 no-op이다.)
if (typeof module !== 'undefined' && module.exports) {
  var __format = require('./format.js');
  var parseRawNumber = __format.parseRawNumber;
  var pickDefined = __format.pickDefined;
  var parseDateKey = __format.parseDateKey;
}

// 분할 히스토리(컬럼형) 행 필드
const HISTORY_COLUMNS = ['holdingPrice', 'subsidiaryPrice', 'holdingValue', 'marketCap', 'ratio', 'sma250', 'ema01', 'mean', 'count'];

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function medianOf(values) {
  if (!values || !values.length) return NaN;
  const sorted = values.slice().sort(function(a, b) { return a - b; });
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function withAlpha(color, alpha) {
  const hex = color.replace('#', '');
  if (hex.length !== 6) return color;
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  return 'rgba(' + r + ', ' + g + ', ' + b + ', ' + alpha + ')';
}

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function derivePreviousPrice(price, changePct) {
  if (typeof price !== 'number' || !Number.isFinite(price)) return null;
  if (typeof changePct !== 'number' || !Number.isFinite(changePct)) return null;
  const denominator = 1 + changePct / 100;
  return denominator ? price / denominator : null;
}

function isKoreanTicker(ticker) {
  return /\.K[QS]$/.test(String(ticker || ''));
}

function rowsFromColumnar(columnar) {
  const dates = columnar.dates || [];
  const subNames = columnar.subs ? Object.keys(columnar.subs) : [];
  const rows = new Array(dates.length);
  for (let i = 0; i < dates.length; i++) {
    const row = { date: dates[i] };
    for (const key of HISTORY_COLUMNS) {
      if (columnar[key]) row[key] = columnar[key][i];
    }
    if (subNames.length) {
      const subs = [];
      for (const name of subNames) {
        const series = columnar.subs[name];
        if (series.price[i] == null && series.value[i] == null && series.ratio[i] == null) continue;
        subs.push({ name: name, price: series.price[i], value: series.value[i], ratio: series.ratio[i] });
      }
      if (subs.length) row.subsidiaries = subs;
    }
    rows[i] = row;
  }
  return rows;
}

function parseNaverQuote(quote) {
  const price = parseRawNumber(pickDefined(quote?.closePriceRaw, quote?.closePrice));
  const change = parseRawNumber(pickDefined(
    quote?.compareToPreviousClosePriceRaw,
    quote?.compareToPreviousClosePrice,
  ));
  return {
    price,
    previousPrice: price != null && change != null ? price - change : null,
    changePct: parseRawNumber(pickDefined(quote?.fluctuationsRatioRaw, quote?.fluctuationsRatio)),
  };
}

function buildLiveMarketMetric(quote, defaults) {
  defaults = defaults || {};
  if (!quote) return null;
  const price = parseRawNumber(pickDefined(quote.closePriceRaw, quote.closePrice));
  if (price == null) return null;
  return {
    id: defaults.id || quote.itemCode || quote.symbolCode || quote.reutersCode,
    name: defaults.name || quote.stockName || quote.indexName || defaults.id,
    price,
    change: parseRawNumber(pickDefined(
      quote.compareToPreviousClosePriceRaw,
      quote.compareToPreviousClosePrice,
    )),
    changePct: parseRawNumber(pickDefined(quote.fluctuationsRatioRaw, quote.fluctuationsRatio)),
    source: '네이버 증권',
    priceDecimals: defaults.priceDecimals == null ? 2 : defaults.priceDecimals,
  };
}

// 시간축 스케일: 포인트 순번이 아닌 실제 날짜 간격으로 x좌표를 배치한다.
// (730일 이전 주간 다운샘플 구간이 일별 구간 대비 5배 압축되어 보이던 왜곡 해소)
function buildTimeScale(hist, padLeft, cW) {
  const xs = new Array(hist.length);
  if (!hist.length) return xs;
  const t0 = parseDateKey(hist[0].date).getTime();
  const span = parseDateKey(hist[hist.length - 1].date).getTime() - t0 || 1;
  for (let i = 0; i < hist.length; i++) {
    xs[i] = padLeft + ((parseDateKey(hist[i].date).getTime() - t0) / span) * cW;
  }
  return xs;
}

function nearestIndexForX(xs, mx) {
  if (!xs.length) return -1;
  let lo = 0;
  let hi = xs.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (xs[mid] < mx) lo = mid;
    else hi = mid;
  }
  return (mx - xs[lo]) <= (xs[hi] - mx) ? lo : hi;
}

function drawTimeAxisLabels(ctx, hist, xs, padLeft, cW, y) {
  const labelCount = Math.min(6, hist.length);
  let lastIdx = -1;
  for (let i = 0; i < labelCount; i++) {
    const targetX = padLeft + (labelCount === 1 ? 0 : (i / (labelCount - 1)) * cW);
    const idx = nearestIndexForX(xs, targetX);
    if (idx === lastIdx) continue;
    lastIdx = idx;
    ctx.fillText(hist[idx].date.slice(2), xs[idx], y);
  }
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export. 브라우저에선 전역 함수로 사용.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    HISTORY_COLUMNS,
    clamp,
    medianOf,
    withAlpha,
    chunkArray,
    derivePreviousPrice,
    isKoreanTicker,
    rowsFromColumnar,
    parseNaverQuote,
    buildLiveMarketMetric,
    buildTimeScale,
    nearestIndexForX,
    drawTimeAxisLabels,
  };
}
