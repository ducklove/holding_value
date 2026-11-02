import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
const { buildHoldingsOverview } = require('../../js/holdings-analysis.js');
const { buildHoldingsDetailRows, buildContributionRows } = require('../../js/dashboard-core.js');
const { buildOwnershipGraph } = require('../../js/ownership-graph.js');

function config(id, subsidiaries) {
  return { id, holdingName: id, holdingTicker: id + '.KS', holdingTotalShares: 100,
    subsidiaries: subsidiaries.map(([id, sharesHeld]) => ({ name: id, ticker: id + '.KS', sharesHeld })) };
}

function fixture() {
  const configs = [config('A', [['B', 20], ['C', 10]]), config('B', [['C', 30]])];
  const pair = {
    id: 'A', holdingName: 'A', holdingTicker: 'A.KS', subsidiaryName: 'B+C',
    current: { holdingPrice: 1e8, holdingValue: 130.1, marketCap: 100,
      subsidiaries: [{ name: 'B', price: 4e8, value: 80 }, { name: 'C', price: 5e8, value: 50 }] },
    fundamentals: { bookValueReport: '2026 반기보고서', bookValueDate: '2026-06-30', subsidiaries: [
      { name: 'B', ticker: 'B.KS', sharesHeld: 20, qty: 20, detail: { stakePct: 19, recentNetIncome: 123 } },
      { name: 'C', ticker: 'C.KS', sharesHeld: 10, qty: 10, detail: { stakePct: 9, recentNetIncome: 456 } },
    ] },
  };
  const securities = { securities: {
    'A.KS': { totalShares: 100, currency: 'KRW' },
    'B.KS': { totalShares: 100, currency: 'KRW' },
    'C.KS': { totalShares: 100, currency: 'KRW', price: 5e8, priceObservedAt: '2026-11-02T04:00:00Z', priceSource: 'fixture' },
  } };
  const hist = [
    { date: '2026-01-01', ratio: 100, subsidiaries: [{ name: 'B', ratio: 60 }, { name: 'C', ratio: 40 }] },
    { date: '2026-11-02', ratio: 130, subsidiaries: [{ name: 'B', ratio: 80 }, { name: 'C', ratio: 50 }] },
  ];
  return { pair, configs, securities, hist, graph: buildOwnershipGraph(configs, { pairs: [pair], lastUpdated: '2026-11-02 12:00:00' }, null, securities) };
}

test('직접 보유 평가가치와 공시 상세를 보존하며 동일 행에 지분율과 기간 기여를 붙인다', () => {
  const { pair, graph, hist } = fixture();
  const original = buildHoldingsDetailRows(pair);
  const overview = buildHoldingsOverview(pair, graph, hist);
  assert.deepEqual(overview.directRows.map(r => r.valueOk), original.rows.map(r => r.valueOk));
  assert.deepEqual(overview.directRows.map(r => r.netIncome), [123, 456]);
  assert.deepEqual(overview.directRows.map(r => r.stakePct), [20, 10]);
  assert.deepEqual(overview.directRows.map(r => r.reportedStakePct), [19, 9]);
  assert.deepEqual(overview.directRows.map(r => r.contribution.delta), [20, 10]);
  assert.equal(overview.contribution.totalDelta, 30);
  assert.equal(overview.contribution.firstDate, '2026-01-01');
  assert.equal(overview.report.label, '2026 반기보고서');
  assert.equal(overview.report.date, '2026-06-30');
});

test('직접/간접으로 함께 보유한 종목의 간접 가치는 간접 비율만 사용하며 직접 합계에 더하지 않는다', () => {
  const { pair, graph, hist } = fixture();
  const overview = buildHoldingsOverview(pair, graph, hist);
  const indirect = overview.indirectRows.find(r => r.ticker === 'C.KS');
  assert.equal(indirect.indirectPct, 6);
  assert.equal(indirect.directPct, 10);
  assert.equal(indirect.totalPct, 16);
  assert.equal(indirect.rawTargetMarketCap, 500e8);
  assert.equal(indirect.nativeValueRaw, 30e8);
  assert.equal(indirect.valueOk, 30);
  assert.equal(indirect.pathCount, 1);
  assert.equal(indirect.minSteps, 2);
  assert.equal(indirect.maxSteps, 2);
  assert.equal(indirect.sharesHeld, null);
  assert.equal(indirect.contribution, null);
  assert.equal(overview.totals.directSum, 130);
  assert.equal(overview.totals.dashboardHoldingValueOk, 130.1);
  assert.equal(overview.totals.indirectExcluded, true);
  assert.equal(overview.indirectStatus, 'ready');
});

test('공유 말단 종목은 한 행에 여러 간접 경로를 모아 평가한다', () => {
  const { pair, securities, hist } = fixture();
  const configs = [config('A', [['B', 20], ['D', 30]]), config('B', [['C', 30]]), config('D', [['C', 10]])];
  const graph = buildOwnershipGraph(configs, { pairs: [pair] }, null, securities);
  const overview = buildHoldingsOverview(pair, graph, hist);
  const rows = overview.indirectRows.filter(r => r.ticker === 'C.KS');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].pathCount, 2);
  assert.equal(rows[0].indirectPct, 9);
  assert.equal(rows[0].valueOk, 45);
});

test('시작일 기여가 빠지면 최초 유효일을 기준일처럼 사용하지 않는다', () => {
  const { pair, graph } = fixture();
  const hist = [
    { date: '2026-01-01', ratio: 90, subsidiaries: [{ name: 'B', ratio: 60 }] },
    { date: '2026-02-01', ratio: 100, subsidiaries: [{ name: 'B', ratio: 65 }, { name: 'C', ratio: 35 }] },
    { date: '2026-03-01', ratio: 130, subsidiaries: [{ name: 'B', ratio: 80 }, { name: 'C', ratio: 50 }] },
  ];
  assert.equal(buildContributionRows(hist).rows.find(r => r.name === 'C').delta, 15, '기존 함수는 최초 유효일을 선택한다');
  const overview = buildHoldingsOverview(pair, graph, hist);
  const missing = overview.directRows.find(r => r.name === 'C');
  assert.equal(missing.contribution.first, null);
  assert.equal(missing.contribution.last, 50);
  assert.equal(missing.contribution.delta, null);
  assert.match(missing.caveats.join(' '), /시작일/);
  assert.equal(overview.directRows.find(r => r.name === 'B').contribution.delta, 20);
  assert.equal(overview.contribution.totalDelta, 40);
});

test('종료일 기여가 빠진 종목도 행을 유지하고 변화를 0으로 만들지 않는다', () => {
  const { pair, graph } = fixture();
  const hist = [
    { date: '2026-01-01', ratio: 100, subsidiaries: [{ name: 'B', ratio: 60 }, { name: 'C', ratio: 40 }] },
    { date: '2026-03-01', ratio: 130, subsidiaries: [{ name: 'B', ratio: 80 }] },
  ];
  const overview = buildHoldingsOverview(pair, graph, hist);
  const missing = overview.directRows.find(r => r.name === 'C');
  assert.equal(missing.contribution.first, 40);
  assert.equal(missing.contribution.last, null);
  assert.equal(missing.contribution.delta, null);
  assert.match(missing.caveats.join(' '), /종료일/);
});

test('단일 자회사 기여는 선택 구간 양 끝의 총 비율 변화를 사용한다', () => {
  const { pair } = fixture();
  const single = { ...pair, subsidiaryName: 'B', current: { holdingValue: 80, subsidiaryPrice: 4e8 },
    fundamentals: { subsidiaries: [pair.fundamentals.subsidiaries[0]] } };
  const overview = buildHoldingsOverview(single, undefined, [{ date: '2026-01-01', ratio: 50 }, { date: '2026-02-01', ratio: 70 }]);
  assert.equal(overview.directRows[0].contribution.first, 50);
  assert.equal(overview.directRows[0].contribution.last, 70);
  assert.equal(overview.directRows[0].contribution.delta, 20);
});

test('복수 자회사 내역이 없는 총 비율 히스토리를 자회사별로 임의 배분하지 않는다', () => {
  const { pair, graph } = fixture();
  const overview = buildHoldingsOverview(pair, graph, [{ date: '2026-01-01', ratio: 50 }, { date: '2026-02-01', ratio: 70 }]);
  assert.equal(overview.contribution, null);
  assert.ok(overview.directRows.every(r => r.contribution === null));
});

test('명시 ticker가 같으면 종목명 변경 전후의 기여를 같은 행에 연결한다', () => {
  const { pair, graph } = fixture();
  const hist = [
    { date: '2026-01-01', ratio: 100, subsidiaries: [{ name: '옛 이름', ticker: 'B.KS', ratio: 60 }, { name: 'C', ratio: 40 }] },
    { date: '2026-02-01', ratio: 130, subsidiaries: [{ name: '새 이름', ticker: 'B.KS', ratio: 80 }, { name: 'C', ratio: 50 }] },
  ];
  const overview = buildHoldingsOverview(pair, graph, hist);
  assert.equal(overview.directRows.find(r => r.ticker === 'B.KS').contribution.delta, 20);
  assert.deepEqual(overview.directRows.map(r => r.valueOk), [80, 50]);
});

test('명칭이 같아도 ticker가 다른 주식종류의 기여를 합치지 않는다', () => {
  const { pair, graph } = fixture();
  const duplicate = structuredClone(pair);
  duplicate.fundamentals.subsidiaries.forEach(sub => { sub.name = '동일명'; });
  duplicate.current.subsidiaries.forEach(sub => { sub.name = '동일명'; });
  const hist = [
    { date: '2026-01-01', ratio: 100, subsidiaries: [{ name: '동일명', ticker: 'B.KS', ratio: 60 }, { name: '동일명', ticker: 'C.KS', ratio: 40 }] },
    { date: '2026-02-01', ratio: 130, subsidiaries: [{ name: '동일명', ticker: 'B.KS', ratio: 80 }, { name: '동일명', ticker: 'C.KS', ratio: 50 }] },
  ];
  const overview = buildHoldingsOverview(duplicate, graph, hist);
  assert.deepEqual(overview.directRows.map(r => r.contribution.delta), [20, 10]);
});

test('순환이나 지분율 누락이 있으면 간접 평가가치를 미확인으로 보존한다', () => {
  const { pair, securities, hist } = fixture();
  const cycle = buildOwnershipGraph([config('A', [['B', 20]]), config('B', [['A', 10], ['C', 30]])], [pair], null, securities);
  const cycleOverview = buildHoldingsOverview(pair, cycle, hist);
  assert.equal(cycleOverview.indirectRows.find(r => r.ticker === 'C.KS').nativeValueRaw, null);
  assert.equal(cycleOverview.indirectStatus, 'partial');
  const missing = buildOwnershipGraph([config('A', [['B', 20]]), config('B', [['C', 30]])], [pair]);
  const missingOverview = buildHoldingsOverview(pair, missing, hist);
  assert.equal(missingOverview.indirectRows.find(r => r.ticker === 'C.KS').valueOk, null);
  assert.equal(missingOverview.totals.directSum, 130);
});

test('해외 간접 가치는 원 통화 USD를 유지하며 원화 억원 합계에 섞지 않는다', () => {
  const { pair, hist } = fixture();
  const graph = buildOwnershipGraph([config('A', [['B', 20]]), {
    ...config('B', []), subsidiaries: [{ name: 'Apple', ticker: 'AAPL', sharesHeld: 30 }],
  }], [pair], null, { securities: { AAPL: { totalShares: 100, price: 300, currency: 'USD', priceAsOf: '2026-10-30', priceSource: 'fixture' } } });
  const overview = buildHoldingsOverview(pair, graph, hist);
  const apple = overview.indirectRows.find(r => r.ticker === 'AAPL');
  assert.equal(apple.indirectPct, 6);
  assert.equal(apple.nativeValueRaw, 1800);
  assert.equal(apple.valueOk, null);
  assert.equal(apple.currency, 'USD');
  assert.equal(apple.price, 300);
  assert.equal(apple.priceAsOf, '2026-10-30');
  assert.equal(overview.totals.directSum, 130);
  assert.ok(overview.directRows.every(r => r.currency === 'KRW'));
});

test('평균/히스토리 없음/그래프 로딩과 오류를 구분한다', () => {
  const { pair, graph } = fixture();
  assert.equal(buildHoldingsOverview({ isAverage: true }, graph, []), null);
  assert.equal(buildHoldingsOverview(null, graph, []), null);
  const loading = buildHoldingsOverview(pair, null, []);
  assert.equal(loading.indirectStatus, 'loading');
  assert.equal(loading.contribution, null);
  assert.equal(loading.directRows.length, 2);
  assert.equal(buildHoldingsOverview(pair, undefined, []).indirectStatus, 'unavailable');
  const noIndirect = buildOwnershipGraph([config('A', [['B', 20], ['C', 10]])]);
  assert.equal(buildHoldingsOverview(pair, noIndirect, []).indirectStatus, 'none');
});

test('holdingTicker가 없어도 pair id로 기준 종목을 찾으며 입력 객체를 변경하지 않는다', () => {
  const { pair, graph, hist } = fixture();
  const copy = structuredClone(pair);
  delete copy.holdingTicker;
  const before = JSON.stringify({ copy, graph, hist });
  const overview = buildHoldingsOverview(copy, graph, hist);
  assert.equal(overview.indirectRows.length, 1);
  assert.equal(JSON.stringify({ copy, graph, hist }), before);
});

test('실제 KCC 삼성전자 3개 경로의 간접 비율과 시가총액으로 환산 가치를 계산한다', () => {
  const read = name => JSON.parse(readFileSync(new URL('../../' + name, import.meta.url)));
  const configs = read('config.json');
  const fundamentals = read('data/fundamentals.json');
  const dashboard = read('data/summary.json');
  const securities = read('data/ownership-securities.json');
  dashboard.pairs.forEach(p => { p.fundamentals = fundamentals.pairs[p.id]; });
  const graph = buildOwnershipGraph(configs, dashboard, null, securities);
  const pair = dashboard.pairs.find(p => p.id === 'kcc');
  const original = buildHoldingsDetailRows(pair);
  const overview = buildHoldingsOverview(pair, graph, []);
  const samsung = overview.indirectRows.find(r => r.ticker === '005930.KS');
  assert.equal(samsung.pathCount, 3);
  assert.equal(samsung.minSteps, 2);
  assert.equal(samsung.maxSteps, 4);
  const byId = id => configs.find(p => p.id === id);
  const held = (id, target) => byId(id).subsidiaries.find(s => s.ticker === target).sharesHeld;
  const shares = id => securities.securities[id].totalShares;
  const expected = 100 * held('kcc', '028260.KS') / shares('028260.KS') * (
    held('samsung_ct', '005930.KS') / shares('005930.KS')
    + held('samsung_ct', '032830.KS') / shares('032830.KS') * (
      held('samsung_life', '005930.KS') / shares('005930.KS')
      + held('samsung_life', '000810.KS') / shares('000810.KS') * held('samsung_fire', '005930.KS') / shares('005930.KS')
    )
  );
  assert.ok(Math.abs(samsung.indirectPct - expected) < 1e-10);
  assert.equal(samsung.nativeValueRaw, samsung.rawTargetMarketCap * samsung.indirectPct / 100);
  assert.equal(samsung.valueOk, samsung.nativeValueRaw / 1e8);
  assert.ok(samsung.valueOk > 0);
  assert.equal(overview.totals.directSum, original.rows.reduce((sum, r) => sum + r.valueOk, 0));
});
