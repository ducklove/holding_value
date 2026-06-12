// node --test tests/js/ 로 실행. classic script(js/calc.js)를 CommonJS로 로드한다.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const calc = require('../../js/calc.js');

test('medianOf: 홀수 개수면 가운데 값', () => {
  assert.equal(calc.medianOf([3, 1, 2]), 2);
  assert.equal(calc.medianOf([5]), 5);
  assert.equal(calc.medianOf([9, 1, 5, 3, 7]), 5);
});

test('medianOf: 짝수 개수면 가운데 두 값의 평균', () => {
  assert.equal(calc.medianOf([4, 1, 3, 2]), 2.5);
  assert.equal(calc.medianOf([10, 20]), 15);
});

test('medianOf: 빈 배열/없는 값은 NaN, 원본 배열은 보존', () => {
  assert.ok(Number.isNaN(calc.medianOf([])));
  assert.ok(Number.isNaN(calc.medianOf(null)));
  assert.ok(Number.isNaN(calc.medianOf(undefined)));
  const values = [3, 1, 2];
  calc.medianOf(values);
  assert.deepEqual(values, [3, 1, 2]); // sort는 복사본에서
});

test('clamp: 구간 [min, max]로 제한', () => {
  assert.equal(calc.clamp(5, 0, 10), 5);
  assert.equal(calc.clamp(-1, 0, 10), 0);
  assert.equal(calc.clamp(11, 0, 10), 10);
  assert.equal(calc.clamp(0.5, 0, 1), 0.5);
});

test('withAlpha: 6자리 hex → rgba, 비hex는 원본 반환', () => {
  assert.equal(calc.withAlpha('#6c8cff', 0.5), 'rgba(108, 140, 255, 0.5)');
  assert.equal(calc.withAlpha('#000000', 1), 'rgba(0, 0, 0, 1)');
  assert.equal(calc.withAlpha('red', 0.5), 'red');
  assert.equal(calc.withAlpha('rgba(1,2,3,0.4)', 0.5), 'rgba(1,2,3,0.4)');
  assert.equal(calc.withAlpha('#fff', 0.5), '#fff'); // 3자리 축약형도 원본 유지
});

test('chunkArray: size 단위 분할, 꼬리 청크 포함', () => {
  assert.deepEqual(calc.chunkArray([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(calc.chunkArray([1, 2], 5), [[1, 2]]);
  assert.deepEqual(calc.chunkArray([], 3), []);
});

test('derivePreviousPrice: 현재가와 등락률로 전일가 역산', () => {
  assert.equal(calc.derivePreviousPrice(200, 100), 100); // +100% → 절반
  assert.equal(calc.derivePreviousPrice(50, -50), 100);  // -50% → 두 배
  const approx = calc.derivePreviousPrice(110, 10);
  assert.ok(Math.abs(approx - 100) < 1e-9);
});

test('derivePreviousPrice: 비수치/분모 0이면 null', () => {
  assert.equal(calc.derivePreviousPrice(null, 5), null);
  assert.equal(calc.derivePreviousPrice(100, NaN), null);
  assert.equal(calc.derivePreviousPrice(100, null), null);
  assert.equal(calc.derivePreviousPrice('100', 5), null);
  assert.equal(calc.derivePreviousPrice(100, -100), null); // 분모 0
});

test('isKoreanTicker: .KS/.KQ 접미사만 true', () => {
  assert.equal(calc.isKoreanTicker('005930.KS'), true);
  assert.equal(calc.isKoreanTicker('247540.KQ'), true);
  assert.equal(calc.isKoreanTicker('AAPL'), false);
  assert.equal(calc.isKoreanTicker('005930'), false);
  assert.equal(calc.isKoreanTicker('005930.KX'), false);
  assert.equal(calc.isKoreanTicker(null), false);
});

test('HISTORY_COLUMNS: 분할 히스토리 행 필드 목록', () => {
  assert.deepEqual(calc.HISTORY_COLUMNS, [
    'holdingPrice', 'subsidiaryPrice', 'holdingValue', 'marketCap',
    'ratio', 'sma250', 'ema01', 'mean', 'count',
  ]);
});

test('rowsFromColumnar: 단일 종목(자회사 컬럼 없음) 변환', () => {
  const rows = calc.rowsFromColumnar({
    dates: ['2026-01-01', '2026-01-02'],
    holdingPrice: [100, 101],
    ratio: [50.1, 50.2],
  });
  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], { date: '2026-01-01', holdingPrice: 100, ratio: 50.1 });
  assert.equal(rows[1].ratio, 50.2);
  assert.ok(!('subsidiaries' in rows[0]));
  assert.ok(!('mean' in rows[0])); // 없는 컬럼은 키도 생기지 않음
});

test('rowsFromColumnar: 다중 자회사 + 특정 날짜에 모두 null인 자회사는 스킵', () => {
  const rows = calc.rowsFromColumnar({
    dates: ['2026-01-01', '2026-01-02'],
    ratio: [60, 61],
    subs: {
      A: { price: [10, 11], value: [1, 2], ratio: [5, 6] },
      B: { price: [20, null], value: [3, null], ratio: [7, null] },
    },
  });
  assert.equal(rows[0].subsidiaries.length, 2);
  assert.deepEqual(rows[0].subsidiaries[1], { name: 'B', price: 20, value: 3, ratio: 7 });
  // 둘째 날 B는 price/value/ratio 모두 null → 스킵되어 A만 남음
  assert.equal(rows[1].subsidiaries.length, 1);
  assert.equal(rows[1].subsidiaries[0].name, 'A');
});

test('rowsFromColumnar: 자회사가 일부만 null이면 유지, 전 자회사 null이면 subsidiaries 키 없음', () => {
  const rows = calc.rowsFromColumnar({
    dates: ['2026-01-01', '2026-01-02'],
    subs: {
      A: { price: [null, null], value: [1, null], ratio: [null, null] },
    },
  });
  // 첫날 value만 있어도 자회사 행 유지
  assert.deepEqual(rows[0].subsidiaries, [{ name: 'A', price: null, value: 1, ratio: null }]);
  // 둘째 날은 전부 null → subsidiaries 키 자체가 없음
  assert.ok(!('subsidiaries' in rows[1]));
  const empty = calc.rowsFromColumnar({});
  assert.deepEqual(empty, []);
});

test('parseNaverQuote: Raw 필드 우선, 전일가 = 현재가 - 대비', () => {
  const parsed = calc.parseNaverQuote({
    closePriceRaw: '1,000',
    closePrice: '999',
    compareToPreviousClosePriceRaw: '50',
    compareToPreviousClosePrice: '49',
    fluctuationsRatioRaw: '5.26',
    fluctuationsRatio: '5.00',
  });
  assert.deepEqual(parsed, { price: 1000, previousPrice: 950, changePct: 5.26 });
});

test('parseNaverQuote: Raw 없으면 일반 필드 폴백, 결측은 null', () => {
  const fallback = calc.parseNaverQuote({ closePrice: '2,500', fluctuationsRatio: '-1.2' });
  assert.equal(fallback.price, 2500);
  assert.equal(fallback.previousPrice, null); // 대비 결측 → 전일가 산출 불가
  assert.equal(fallback.changePct, -1.2);
  assert.deepEqual(calc.parseNaverQuote(undefined), { price: null, previousPrice: null, changePct: null });
});

test('parseProxyQuote: internal proxy quote', () => {
  const parsed = calc.parseProxyQuote({
    summary: { current_price: 332000, change: 33000, change_rate: 11.04 },
    raw: { stck_sdpr: '299000', prdy_vrss_sign: '2' },
  });
  assert.deepEqual(parsed, { price: 332000, previousPrice: 299000, changePct: 11.04 });
});

test('parseProxyQuote: falling sign derives previous price', () => {
  const parsed = calc.parseProxyQuote({
    summary: { current_price: 95000 },
    raw: { prdy_vrss: '5000', prdy_vrss_sign: '5', prdy_ctrt: '-5.00' },
  });
  assert.deepEqual(parsed, { price: 95000, previousPrice: 100000, changePct: -5 });
});

test('buildLiveMarketMetric: Raw 우선 파싱 + defaults 반영, 가격 없으면 null', () => {
  const metric = calc.buildLiveMarketMetric({
    itemCode: 'KOSPI',
    closePriceRaw: '2,500.5',
    closePrice: '0',
    compareToPreviousClosePriceRaw: '10.5',
    fluctuationsRatioRaw: '0.42',
  }, { id: 'KOSPI', name: 'KOSPI', priceDecimals: 2 });
  assert.equal(metric.id, 'KOSPI');
  assert.equal(metric.name, 'KOSPI');
  assert.equal(metric.price, 2500.5);
  assert.equal(metric.change, 10.5);
  assert.equal(metric.changePct, 0.42);
  assert.equal(metric.source, '네이버 증권');
  assert.equal(metric.priceDecimals, 2);
  assert.equal(calc.buildLiveMarketMetric(null), null);
  assert.equal(calc.buildLiveMarketMetric({ stockName: 'X' }), null); // 가격 결측
});

test('buildProxyMarketMetric: internal KIS proxy index', () => {
  const metric = calc.buildProxyMarketMetric({
    market: 'kospi',
    index_code: '0001',
    summary: { current_price: 8351.23, change: 587.28, change_rate: 7.56 },
    raw: { prdy_vrss_sign: '2' },
  }, { id: 'KOSPI', name: 'KOSPI', priceDecimals: 2 });
  assert.equal(metric.id, 'KOSPI');
  assert.equal(metric.name, 'KOSPI');
  assert.equal(metric.price, 8351.23);
  assert.equal(metric.change, 587.28);
  assert.equal(metric.changePct, 7.56);
  assert.equal(metric.source, '내부 KIS 프록시');
  assert.equal(metric.priceDecimals, 2);
  assert.equal(calc.buildProxyMarketMetric(null), null);
});

test('buildTimeScale: 날짜 간격 비례 x좌표 (등간격 아님)', () => {
  const hist = [{ date: '2026-01-01' }, { date: '2026-01-02' }, { date: '2026-01-11' }];
  const xs = calc.buildTimeScale(hist, 0, 100);
  assert.equal(xs.length, 3);
  assert.equal(xs[0], 0);
  assert.ok(Math.abs(xs[1] - 10) < 1e-9); // 10일 중 1일 경과 → 10%
  assert.equal(xs[2], 100);
  const padded = calc.buildTimeScale([{ date: '2026-01-01' }], 64, 100);
  assert.equal(padded[0], 64); // span 0 → 1로 보정, 시작점 = padLeft
  assert.deepEqual(calc.buildTimeScale([], 0, 100), []);
});

test('nearestIndexForX: 가장 가까운 포인트 인덱스 (이분 탐색)', () => {
  const xs = [0, 10, 100];
  assert.equal(calc.nearestIndexForX(xs, 2), 0);
  assert.equal(calc.nearestIndexForX(xs, 7), 1);
  assert.equal(calc.nearestIndexForX(xs, 56), 2);
  assert.equal(calc.nearestIndexForX(xs, -5), 0);
  assert.equal(calc.nearestIndexForX(xs, 999), 2);
  assert.equal(calc.nearestIndexForX([], 5), -1);
});

test('drawTimeAxisLabels: 인자(ctx)만 사용해 라벨을 그림 — 중복 인덱스는 한 번만', () => {
  const calls = [];
  const ctx = { fillText: (text, x, y) => calls.push({ text, x, y }) };
  const hist = [{ date: '2026-01-01' }, { date: '2026-01-02' }];
  const xs = calc.buildTimeScale(hist, 0, 100);
  calc.drawTimeAxisLabels(ctx, hist, xs, 0, 100, 20);
  assert.equal(calls.length, 2); // 포인트 2개 → 라벨 2개 (중복 제거 후)
  assert.equal(calls[0].text, '26-01-01'); // date.slice(2)
  assert.equal(calls[1].text, '26-01-02');
  assert.equal(calls[1].x, 100);
  assert.equal(calls[0].y, 20);
});
