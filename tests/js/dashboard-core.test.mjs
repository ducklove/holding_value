// node --test tests/js/ 로 실행. classic script(js/dashboard-core.js)를 CommonJS로 로드한다.
// index.html 인라인 스크립트에서 추출한 순수 로직(스냅샷 버전 가드, 카드 정렬 비교자,
// 오늘 히스토리 병합, 요약 집계, 재시도 정책, 실시간 폴백 병합, 줌/기간 필터, 통계)을 검증한다.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const core = require('../../js/dashboard-core.js');

test('getSnapshotVersion: snapshotTimestamp → generatedAt → lastUpdated(서울) 순 우선', () => {
  assert.equal(core.getSnapshotVersion(null), 0);
  assert.equal(core.getSnapshotVersion({ snapshotTimestamp: 1234 }), 1234);
  assert.equal(
    core.getSnapshotVersion({ generatedAt: '2026-07-10T00:00:00Z', lastUpdated: '2026-07-11 09:00:00' }),
    Date.parse('2026-07-10T00:00:00Z'),
  );
  // 서울 09:00 = UTC 00:00
  assert.equal(core.getSnapshotVersion({ lastUpdated: '2026-07-10 09:00:00' }), Date.UTC(2026, 6, 10, 0, 0, 0));
  assert.equal(core.getSnapshotVersion({ generatedAt: '유효하지 않음', lastUpdated: '' }), 0);
});

test('comparePairOrder: 평균 → 즐겨찾기 → 비율 내림차순 → 이름(ko) 순', () => {
  const avg = { isAverage: true, name: '중앙값', current: { ratio: 50 } };
  const a = { id: 'a', name: '가나', current: { ratio: 80 } };
  const b = { id: 'b', name: '다라', current: { ratio: 120 } };
  const sameRatio = { id: 'c', name: '나다', current: { ratio: 80 } };

  assert.ok(core.comparePairOrder(avg, b) < 0); // 평균이 항상 앞
  assert.ok(core.comparePairOrder(b, avg) > 0);
  assert.equal(core.comparePairOrder(avg, avg), 0);
  assert.ok(core.comparePairOrder(b, a) < 0); // 비율 내림차순
  assert.ok(core.comparePairOrder(a, sameRatio) < 0); // 동률이면 한글 이름 오름차순

  // 즐겨찾기 주입 시 비율보다 우선
  const isPinned = pair => pair.id === 'a';
  assert.ok(core.comparePairOrder(a, b, isPinned) < 0);
  assert.ok(core.comparePairOrder(b, a, isPinned) > 0);
  // 판별 함수 미주입이면 전부 비즐겨찾기로 취급
  assert.ok(core.comparePairOrder(a, b) > 0);
});

test('upsertTodayHistory: 같은 날짜는 병합 갱신, 새 날짜는 append, 빈 히스토리는 무시', () => {
  const pair = { history: [{ date: '2026-07-09', ratio: 100, holdingPrice: 50000 }] };
  core.upsertTodayHistory(pair, { ratio: 105, holdingPrice: 51000, holdingValue: 3.5, marketCap: 3.2 }, '2026-07-09');
  assert.equal(pair.history.length, 1);
  assert.equal(pair.history[0].ratio, 105);
  assert.equal(pair.history[0].holdingPrice, 51000);

  core.upsertTodayHistory(pair, { ratio: 110, subsidiaryPrice: 7000 }, '2026-07-10');
  assert.equal(pair.history.length, 2);
  assert.equal(pair.history[1].date, '2026-07-10');
  assert.equal(pair.history[1].subsidiaryPrice, 7000);

  const empty = { history: [] };
  core.upsertTodayHistory(empty, { ratio: 1 }, '2026-07-10'); // 하이드레이션 전에는 반영하지 않음
  assert.equal(empty.history.length, 0);
  core.upsertTodayHistory(pair, { ratio: 1 }, ''); // 날짜 없으면 무시
  assert.equal(pair.history.length, 2);
});

test('upsertTodayHistory: 평균쌍은 ratio/count/mean만 병합', () => {
  const avg = { isAverage: true, history: [{ date: '2026-07-09', ratio: 90 }] };
  core.upsertTodayHistory(avg, { ratio: 95, count: 12, mean: 93, holdingPrice: 999 }, '2026-07-10');
  assert.deepEqual(avg.history[1], { date: '2026-07-10', ratio: 95, count: 12, mean: 93 }); // holdingPrice 미포함
});

function makePair(id, current) {
  return { id, name: id, current };
}

test('buildTodaySummaryFrom: 평균쌍 값 우선, 없으면 중앙값 폴백', () => {
  const withAvg = [
    { isAverage: true, name: '중앙값', current: { ratio: 77, ratioChange: -1.5 } },
    makePair('a', { ratio: 50, ratioChange: 2 }),
    makePair('b', { ratio: 100, ratioChange: -3 }),
  ];
  const summary = core.buildTodaySummaryFrom(withAvg, null, null);
  assert.equal(summary.averageRatio, 77);
  assert.equal(summary.averageRatioChange, -1.5);
  assert.equal(summary.representativeCount, 2);

  const withoutAvg = [
    makePair('a', { ratio: 50, ratioChange: 2 }),
    makePair('b', { ratio: 100, ratioChange: -3 }),
    makePair('c', { ratio: 80, ratioChange: 1 }),
  ];
  const fallback = core.buildTodaySummaryFrom(withoutAvg, null, null);
  assert.equal(fallback.averageRatio, 80); // 중앙값
  assert.equal(fallback.averageRatioChange, 1);
});

test('buildTodaySummaryFrom: 확대/축소 리더와 러너, 지주·자회사 평균 등락', () => {
  const pairsList = [
    makePair('a', { ratio: 1, ratioChange: 5, holdingChange: 2, subsidiaryChange: 1 }),
    makePair('b', { ratio: 2, ratioChange: -4, holdingChange: 4, subsidiaries: [{ change: 3 }, { change: 5 }] }),
    makePair('c', { ratio: 3, ratioChange: 0.5 }), // holdingChange 없음 → 평균에서 제외
  ];
  const summary = core.buildTodaySummaryFrom(pairsList, null, null);
  assert.equal(summary.topExpansion.id, 'a');
  assert.deepEqual(summary.expansionRunners.map(p => p.id), ['c', 'b']);
  assert.equal(summary.topContraction.id, 'b');
  assert.deepEqual(summary.contractionRunners.map(p => p.id), ['c', 'a']);
  assert.equal(summary.averageHoldingChange, 3); // (2+4)/2
  assert.equal(summary.averageSubsidiaryChange, 3); // (1+3+5)/3
});

test('buildTodaySummaryFrom: source/market은 스냅샷 → 이전 요약 → 기본값 순 폴백', () => {
  const pairsList = [makePair('a', { ratio: 1, ratioChange: 0 })];
  const prev = { source: '이전 소스', market: { id: 'KOSPI' } };

  assert.equal(core.buildTodaySummaryFrom(pairsList, { source: '명시' }, prev).source, '명시');
  const inferred = core.buildTodaySummaryFrom(
    pairsList,
    { pairs: [{ quoteSource: 'internal_proxy' }] },
    prev,
  );
  assert.equal(inferred.source, '내부 프록시 실시간');
  assert.equal(core.buildTodaySummaryFrom(pairsList, null, prev).source, '이전 소스');
  assert.equal(core.buildTodaySummaryFrom(pairsList, null, null).source, 'current.json');

  assert.deepEqual(core.buildTodaySummaryFrom(pairsList, { market: { id: 'M1' } }, prev).market, { id: 'M1' });
  assert.deepEqual(core.buildTodaySummaryFrom(pairsList, { summary: { market: { id: 'M2' } } }, prev).market, { id: 'M2' });
  assert.deepEqual(core.buildTodaySummaryFrom(pairsList, null, prev).market, { id: 'KOSPI' });
  assert.equal(core.buildTodaySummaryFrom(pairsList, null, null).market, null);
});

test('inferSnapshotSource: 알려진 소스 우선순위, 미상은 나열', () => {
  assert.equal(core.inferSnapshotSource(null), '');
  assert.equal(core.inferSnapshotSource({ pairs: [] }), '');
  assert.equal(
    core.inferSnapshotSource({ pairs: [{ quoteSource: 'yfinance' }, { quoteSource: 'internal_proxy' }] }),
    '내부 프록시 실시간',
  );
  assert.equal(core.inferSnapshotSource({ pairs: [{ quoteSource: 'kis_proxy' }] }), 'KIS 프록시');
  assert.equal(
    core.inferSnapshotSource({ pairs: [{ quoteSource: 'x' }, { quoteSource: 'y' }, { quoteSource: 'x' }] }),
    'x, y',
  );
});

test('escapeCsvValue: 쉼표·따옴표·개행은 감싸고 내부 따옴표는 이중화', () => {
  assert.equal(core.escapeCsvValue(null), '');
  assert.equal(core.escapeCsvValue(undefined), '');
  assert.equal(core.escapeCsvValue(123.4), '123.4');
  assert.equal(core.escapeCsvValue('평범'), '평범');
  assert.equal(core.escapeCsvValue('a,b'), '"a,b"');
  assert.equal(core.escapeCsvValue('그는 "지주"라 했다'), '"그는 ""지주""라 했다"');
  assert.equal(core.escapeCsvValue('줄\n바꿈'), '"줄\n바꿈"');
});

test('parseRetryAfterMs: 초 단위 숫자와 HTTP 날짜 모두 ms로', () => {
  assert.equal(core.parseRetryAfterMs(null), null);
  assert.equal(core.parseRetryAfterMs(''), null);
  assert.equal(core.parseRetryAfterMs('2'), 2000);
  assert.equal(core.parseRetryAfterMs('0'), 0);
  assert.equal(core.parseRetryAfterMs(-3), 0); // 음수는 0으로 클램프
  const future = new Date(Date.now() + 5000).toUTCString();
  const ms = core.parseRetryAfterMs(future);
  assert.ok(ms > 3000 && ms <= 5000, `기대 범위 밖: ${ms}`);
  assert.equal(core.parseRetryAfterMs('언젠가'), null);
});

test('isRetryableLiveFetchError: 429/5xx/중단/네트워크 오류만 재시도', () => {
  assert.equal(core.isRetryableLiveFetchError({ status: 429 }), true);
  assert.equal(core.isRetryableLiveFetchError({ status: 503 }), true);
  assert.equal(core.isRetryableLiveFetchError({ status: 404 }), false);
  assert.equal(core.isRetryableLiveFetchError({ name: 'AbortError' }), true);
  assert.equal(core.isRetryableLiveFetchError({ message: 'Failed to fetch' }), true);
  assert.equal(core.isRetryableLiveFetchError({ message: 'NetworkError when attempting' }), true);
  assert.equal(core.isRetryableLiveFetchError({ message: '잘못된 응답' }), false);
  assert.equal(core.isRetryableLiveFetchError(null), false);
});

test('getLiveFetchRetryDelayMs: Retry-After 우선(상한 7초), 429는 3.5초 기반 지수 백오프', () => {
  assert.equal(core.getLiveFetchRetryDelayMs({ retryAfter: '2' }, 0), 2000);
  assert.equal(core.getLiveFetchRetryDelayMs({ retryAfter: '30' }, 0), 7000); // 상한
  assert.equal(core.getLiveFetchRetryDelayMs({}, 0), 1000); // 기본 1초 × 2^0
  assert.equal(core.getLiveFetchRetryDelayMs({}, 1), 2000);
  assert.equal(core.getLiveFetchRetryDelayMs({}, 5), 7000); // 상한
  assert.equal(core.getLiveFetchRetryDelayMs({ status: 429 }, 0), 3500);
  assert.equal(core.getLiveFetchRetryDelayMs({ status: 429 }, 1), 7000); // 3500×2 → 상한
});

test('normalizeZoomState: NaN 초기화, 범위 클램프, 역전 교환, 최소 폭 보장', () => {
  const zoom = { start: NaN, end: NaN };
  core.normalizeZoomState(zoom);
  assert.deepEqual(zoom, { start: 0, end: 1 });

  const reversed = { start: 0.8, end: 0.2 };
  core.normalizeZoomState(reversed);
  assert.equal(reversed.start, 0.2);
  assert.equal(reversed.end, 0.8);

  const tiny = { start: 0.5, end: 0.5 };
  core.normalizeZoomState(tiny);
  assert.ok(Math.abs((tiny.end - tiny.start) - core.MIN_ZOOM_SPAN) < 1e-12); // 중심 유지 최소 폭
  assert.ok(Math.abs((tiny.start + tiny.end) / 2 - 0.5) < 1e-12);

  const overflow = { start: -1, end: 2 };
  core.normalizeZoomState(overflow);
  assert.deepEqual(overflow, { start: 0, end: 1 });
});

test('applyZoomToHistory: 3행 미만·전체 범위는 그대로, 그 외 비율 슬라이스', () => {
  const hist = Array.from({ length: 11 }, (_, i) => ({ date: `2026-06-${String(i + 1).padStart(2, '0')}` }));
  assert.equal(core.applyZoomToHistory(hist, { start: 0, end: 1 }), hist); // 동일 참조
  assert.deepEqual(core.applyZoomToHistory(null, { start: 0, end: 1 }), []);
  const short = [{ date: 'a' }, { date: 'b' }];
  assert.equal(core.applyZoomToHistory(short, { start: 0.4, end: 0.5 }), short);

  const sliced = core.applyZoomToHistory(hist, { start: 0.2, end: 0.5 });
  assert.equal(sliced[0].date, hist[2].date); // floor(0.2×10)=2
  assert.equal(sliced[sliced.length - 1].date, hist[5].date); // ceil(0.5×10)=5
});

test('filterHistoryByDays: 0은 전체, 컷오프 필터, 표본 부족 시 전체 유지', () => {
  const hist = [
    { date: '2026-06-01' },
    { date: '2026-07-01' },
    { date: '2026-07-08' },
    { date: '2026-07-10' },
  ];
  assert.equal(core.filterHistoryByDays(hist, 0), hist);
  assert.deepEqual(
    core.filterHistoryByDays(hist, 10).map(h => h.date),
    ['2026-07-01', '2026-07-08', '2026-07-10'], // 컷오프 2026-06-30
  );
  assert.equal(core.filterHistoryByDays(hist, 1), hist); // 필터 결과 2건 미만 → 전체
  const single = [{ date: '2026-07-10' }];
  assert.equal(core.filterHistoryByDays(single, 30), single);
});

test('getStackedSubsidiarySeries: 자회사 2곳 이상인 첫 행 기준 시리즈, 없으면 빈 배열', () => {
  assert.deepEqual(core.getStackedSubsidiarySeries([{ ratio: 1 }]), []);
  assert.deepEqual(core.getStackedSubsidiarySeries([{ subsidiaries: [{ name: '단독' }] }]), []);
  assert.deepEqual(
    core.getStackedSubsidiarySeries([
      { ratio: 1 },
      { subsidiaries: [{ name: 'A', price: 1 }, { name: 'B', price: 2 }] },
    ]),
    [{ name: 'A' }, { name: 'B' }],
  );
});

test('buildLivePairEntry: 시세 완전 수신 시 비율/전일비 계산', () => {
  const pair = { id: 'hold', current: {} };
  const config = {
    holdingTicker: '000001.KS',
    holdingAdjustedShares: 1000,
    subsidiaries: [
      { name: 'SubA', ticker: '000002.KS', sharesHeld: 100 },
      { name: 'SubB', ticker: '000003.KQ', sharesHeld: 200 },
    ],
  };
  const quoteMap = new Map([
    ['000001', { price: 50000, previousPrice: 50000, changePct: 0 }],
    ['000002', { price: 100000, previousPrice: 100000, changePct: 0 }],
    ['000003', { price: 50000, previousPrice: 40000, changePct: 25 }],
  ]);
  const entry = core.buildLivePairEntry(pair, config, quoteMap);
  // marketCap = 1000×50000 = 5000만, holdingValue = 100×10만 + 200×5만 = 2000만 → 40%
  assert.equal(entry.id, 'hold');
  assert.equal(entry.holdingPrice, 50000);
  assert.equal(entry.marketCap, 0.5); // 억 단위
  assert.equal(entry.holdingValue, 0.2);
  assert.equal(entry.ratio, 40);
  // 전일 holdingValue = 100×10만 + 200×4만 = 1800만, 전일 시총 동일 → 36% → +4%p
  assert.equal(entry.ratioChange, 4);
  assert.equal(entry.quoteSource, 'internal_proxy');
  assert.equal(entry.subsidiaries.length, 2);
  assert.equal(entry.subsidiaries[0].ratio, 20); // 1000만/5000만
});

test('buildLivePairEntry: 자회사 시세 결측 시 기존 스냅샷 폴백 + mixed 표기', () => {
  const pair = {
    id: 'hold',
    current: {
      subsidiaries: [{ name: 'SubA', price: 80000, change: -2 }],
    },
  };
  const config = {
    holdingTicker: '000001.KS',
    holdingAdjustedShares: 1000,
    subsidiaries: [{ name: 'SubA', ticker: 'PRIVATE', sharesHeld: 100 }], // 비상장 → 시세 없음
  };
  const quoteMap = new Map([
    ['000001', { price: 50000, previousPrice: 50000, changePct: 0 }],
  ]);
  const entry = core.buildLivePairEntry(pair, config, quoteMap);
  assert.equal(entry.quoteSource, 'mixed');
  assert.equal(entry.subsidiaryPrice, 80000); // 단일 자회사는 평탄화
  assert.equal(entry.subsidiaryChange, -2);
  // 폴백 전일가 = 80000 / 0.98 → 전일비도 산출된다
  assert.ok(entry.ratioChange != null);
});

test('buildLivePairEntry: 지주 시세·조정주식수·자회사가 없으면 null', () => {
  const config = {
    holdingTicker: '000001.KS',
    holdingAdjustedShares: 1000,
    subsidiaries: [{ name: 'SubA', ticker: '000002.KS', sharesHeld: 100 }],
  };
  const quotes = new Map([['000002', { price: 1000, previousPrice: 1000, changePct: 0 }]]);
  assert.equal(core.buildLivePairEntry({ id: 'x', current: {} }, config, quotes), null); // 지주 시세 없음

  const noShares = Object.assign({}, config, { holdingAdjustedShares: 0, holdingTotalShares: 0 });
  const withHolding = new Map(quotes);
  withHolding.set('000001', { price: 50000, previousPrice: 50000, changePct: 0 });
  assert.equal(core.buildLivePairEntry({ id: 'x', current: {} }, noShares, withHolding), null);

  const noSubs = Object.assign({}, config, { subsidiaries: [] });
  assert.equal(core.buildLivePairEntry({ id: 'x', current: {} }, noSubs, withHolding), null);
});

test('computeRatioStats: 최소/최대/평균/백분위/Z-score', () => {
  const stats = core.computeRatioStats([10, 20, 30, 40], 30);
  assert.equal(stats.min, 10);
  assert.equal(stats.max, 40);
  assert.equal(stats.avg, 25);
  assert.equal(stats.percentile, '75'); // 4개 중 3개가 현재값 이하
  assert.ok(Math.abs(stats.zScore - 5 / Math.sqrt(125)) < 1e-12);

  const flat = core.computeRatioStats([50, 50, 50], 50);
  assert.equal(flat.zScore, null); // 표준편차 0
  assert.equal(flat.percentile, '100');
});

test('buildContributionRows: |Δ| 내림차순 행과 총 변화량, 분해 불가 시 null', () => {
  const hist = [
    { date: 'd1', ratio: 100, subsidiaries: [{ name: 'A', ratio: 60 }, { name: 'B', ratio: 40 }] },
    { date: 'd2', ratio: 103, subsidiaries: [{ name: 'A', ratio: 61 }, { name: 'B', ratio: 45 }] },
  ];
  const result = core.buildContributionRows(hist);
  assert.deepEqual(result.rows.map(r => r.name), ['B', 'A']); // |Δ5| > |Δ1|
  assert.equal(result.rows[0].delta, 5);
  assert.equal(result.maxAbs, 5);
  assert.equal(result.totalDelta, 3);

  assert.equal(core.buildContributionRows([hist[0]]), null); // 구간 부족
  assert.equal(core.buildContributionRows([
    { date: 'd1', ratio: 1 },
    { date: 'd2', ratio: 2, subsidiaries: [{ name: 'A', ratio: 1 }] }, // 단일 자회사
  ]), null);
  assert.equal(core.buildContributionRows([
    { date: 'd1', ratio: 1 },
    { date: 'd2', ratio: 2, subsidiaries: [{ name: 'A', ratio: NaN }, { name: 'B' }] }, // 유효 행 없음
  ]), null);
});

test('buildRatioTrendLines: 사전계산(sma250/ema01) 필드가 있으면 그대로 사용', () => {
  const visible = [
    { date: 'd1', ratio: 10, sma250: 9.5, ema01: 9.8 },
    { date: 'd2', ratio: 11 }, // 필드 결측 행은 null
  ];
  const lines = core.buildRatioTrendLines([], visible);
  assert.deepEqual(lines.sma.values, [9.5, null]);
  assert.deepEqual(lines.ema.values, [9.8, null]);
});

test('buildRatioTrendLines: 사전계산 없으면 전체 히스토리에서 SMA250/EMA(α=0.1) 계산', () => {
  const fullHist = [];
  for (let i = 0; i < 260; i += 1) {
    fullHist.push({ date: 'd' + i, ratio: 100 }); // 상수 시계열 → SMA=EMA=100
  }
  const visible = [fullHist[0], fullHist[248], fullHist[249], fullHist[259], { date: '없는날짜', ratio: 1 }];
  const lines = core.buildRatioTrendLines(fullHist, visible);
  // SMA250: 250번째 관측치(idx 249)부터 값, 이전은 null. 가시 구간에 없는 날짜도 null.
  assert.deepEqual(lines.sma.values, [null, null, 100, 100, null]);
  // EMA: 첫 관측치부터 값. 상수 시계열이라 항상 100.
  assert.deepEqual(lines.ema.values, [100, 100, 100, 100, null]);

  // 변동 시계열의 EMA 재귀식 검증: ema = α·x + (1-α)·ema
  const varied = [{ date: 'v0', ratio: 100 }, { date: 'v1', ratio: 110 }, { date: 'v2', ratio: 90 }];
  const variedLines = core.buildRatioTrendLines(varied, varied);
  assert.equal(variedLines.ema.values[0], 100);
  assert.ok(Math.abs(variedLines.ema.values[1] - 101) < 1e-12);
  assert.ok(Math.abs(variedLines.ema.values[2] - (0.1 * 90 + 0.9 * 101)) < 1e-12);
});

test('buildChartLegendItems: 스택(2개 이상)이면 자회사+총 비율, 아니면 단일 비율 범례', () => {
  const palette = ['#111111', '#222222'];
  const colors = { text: '#t', accent: '#a', avgLine: '#avg', smaLine: '#sma', emaLine: '#ema' };
  const stacked = core.buildChartLegendItems([{ name: 'A' }, { name: 'B' }, { name: 'C' }], palette, colors);
  assert.deepEqual(stacked.map(i => i.label), ['A', 'B', 'C', '총 비율', '기간 평균', 'SMA 250일', 'EMA α=0.1']);
  assert.deepEqual(stacked.slice(0, 3).map(i => i.color), ['#111111', '#222222', '#111111']); // 팔레트 순환
  assert.equal(stacked[3].color, '#t');

  const single = core.buildChartLegendItems([{ name: 'A' }], palette, colors);
  assert.deepEqual(single.map(i => i.label), ['비율', '기간 평균', 'SMA 250일', 'EMA α=0.1']);
  assert.equal(single[0].color, '#a');
});
