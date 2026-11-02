import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
const { buildOwnershipGraph, getOwnershipComponent, layoutOwnershipGraph, getOwnershipExposure, getOwnershipDescendants } = require('../../js/ownership-graph.js');

const pair = (id, ticker, subsidiaries, totalShares = 100) => ({
  id, holdingTicker: ticker, holdingName: id, holdingTotalShares: totalShares,
  holdingAdjustedShares: 1, holdingTreasuryShares: 99,
  subsidiaries: subsidiaries.map(([ticker, sharesHeld, name]) => ({ ticker, sharesHeld, name: name || ticker })),
});
const chain = [
  pair('sk', '034730.KS', [['402340.KS', 32, 'SK스퀘어'], ['017670.KS', 30, 'SK텔레콤']]),
  pair('square', '402340.KS', [['000660.KS', 20, 'SK하이닉스']]),
  pair('other', '999999.KQ', [['888888.KQ', 5]]),
];

test('SK → SK스퀘어 → SK하이닉스를 모든 참여 종목에서 같은 연결망으로 탐색한다', () => {
  const graph = buildOwnershipGraph(chain);
  for (const focus of ['034730.KS', '402340.KS', '000660.KS', 'square', '000660']) {
    const component = getOwnershipComponent(graph, focus);
    assert.deepEqual(component.nodes.map(n => n.id), ['000660.KS', '017670.KS', '034730.KS', '402340.KS']);
    assert.equal(component.edges.length, 3);
    assert.ok(!component.nodes.some(n => n.id === '999999.KQ'));
  }
  assert.equal(getOwnershipComponent(graph, 'square').focusId, '402340.KS');
});

test('총발행주식수가 알려진 경우만 설정 보유수량으로 지분율을 산출한다', () => {
  const graph = buildOwnershipGraph(chain);
  const square = graph.edges.find(e => e.target === '402340.KS');
  assert.equal(square.stakePct, 32);
  assert.equal(square.stakeSource, 'calculated');
  // 조정주식수 1주/자사주 차감은 지분율의 분모가 아니다.
  assert.equal(square.sharesHeld, 32);
  const hynix = graph.edges.find(e => e.target === '000660.KS');
  assert.equal(hynix.stakePct, null);
  assert.equal(hynix.stakeSource, null);
  assert.equal(hynix.sharesHeld, 20);
});

test('계산 지분율을 통일해서 쓰면서 공시 기준일과 수량 불일치를 함께 보존한다', () => {
  const graph = buildOwnershipGraph(chain, [{ id: 'sk', fundamentals: {
    bookValueDate: '2026-06-30', bookValueReport: '2026 반기보고서', subsidiaries: [
      { ticker: '402340.KS', qty: 25, detail: { stakePct: 31.5 } },
    ],
  } }]);
  const edge = graph.edges.find(e => e.target === '402340.KS');
  assert.equal(edge.stakePct, 32);
  assert.equal(edge.stakeSource, 'calculated');
  assert.equal(edge.reportDate, null);
  assert.equal(edge.disclosedStakePct, 31.5);
  assert.equal(edge.disclosureDate, '2026-06-30');
  assert.equal(edge.quantityMismatch, true);
  assert.equal(edge.sharesHeld, 32);
  assert.equal(edge.disclosedShares, 25);
});

test('발행주식수가 없으면 날짜가 있는 공시 지분율로 폴백한다', () => {
  const graph = buildOwnershipGraph(chain, [{ id: 'square', fundamentals: {
    bookValueDate: '2026-06-30', subsidiaries: [{ ticker: '000660.KS', qty: 20, detail: { stakePct: 20.5 } }],
  } }]);
  const edge = graph.edges.find(e => e.target === '000660.KS');
  assert.equal(edge.stakePct, 20.5);
  assert.equal(edge.stakeSource, 'disclosure');
  assert.equal(edge.reportDate, '2026-06-30');
});

test('중간법인 또는 여러 주식종류를 합친 출자현황의 %를 직접 지분율로 쓰지 않는다', () => {
  const configs = [pair('yp', '000670.KS', [['010130.KS', 5]]), pair('a', 'A', [['B', 3]])];
  const payload = { pairs: {
    yp: { subsidiaries: [{ ticker: '010130.KS', note: 'YPC 투자주식으로 계상', detail: { stakePct: 100 } }] },
    a: { subsidiaries: [{ ticker: 'B', matchedNames: ['보통주', '우선주'], detail: { stakePct: 20 } }] },
  } };
  const graph = buildOwnershipGraph(configs, [], payload);
  assert.ok(graph.edges.every(e => e.stakePct === null));
  assert.match(graph.edges.find(e => e.source === '000670.KS').note, /YPC/);
});

test('시장가치 비율을 지분율로 오인하지 않으며 명시된 0%는 보존한다', () => {
  const configs = [pair('a', 'A', [['B', 0], ['C', 5]])];
  configs[0].subsidiaries[0].stakePct = 0;
  const graph = buildOwnershipGraph(configs, [{ id: 'a', current: { ratio: 850, subsidiaries: [{ name: 'C', ratio: 600 }] } }]);
  assert.equal(graph.edges.find(e => e.target === 'B').stakePct, 0);
  assert.equal(graph.edges.find(e => e.target === 'C').stakePct, null);
});

test('이름이 같아도 거래소와 주식종류가 다른 종목은 합치지 않는다', () => {
  const graph = buildOwnershipGraph([pair('a', 'A', [['005930.KS', 2, '삼성'], ['005935.KS', 3, '삼성'], ['005930.KQ', 4, '삼성']])]);
  assert.equal(graph.nodes.length, 4);
  assert.equal(graph.edges.length, 3);
  assert.equal(getOwnershipComponent(graph, '005930').focusId, null);
});

test('공유 투자처는 노드 하나와 소유자별 연결로 표현한다', () => {
  const graph = buildOwnershipGraph([pair('a', 'A', [['C', 10]]), pair('b', 'B', [['C', 20]])]);
  const component = getOwnershipComponent(graph, 'C');
  assert.equal(component.nodes.length, 3);
  assert.equal(component.edges.length, 2);
  assert.deepEqual(component.edges.map(e => e.sharesHeld), [10, 20]);
});

test('같은 소유자의 중복 페어는 주식을 합산하지 않고 충돌값을 숨긴다', () => {
  const graph = buildOwnershipGraph([pair('a', 'A', [['B', 10]]), pair('b', 'A', [['B', 10]])]);
  assert.equal(graph.edges.length, 1);
  assert.equal(graph.edges[0].sharesHeld, 10);
  assert.deepEqual(graph.edges[0].pairIds, ['a', 'b']);
  const conflict = buildOwnershipGraph([pair('a', 'A', [['B', 10]]), pair('b', 'A', [['B', 20]])]);
  assert.equal(conflict.edges[0].sharesHeld, null);
  assert.equal(conflict.edges[0].conflictingRecords, true);
});

test('소유방향을 따라 단계가 늘어나며 모든 카드가 그래프 경계 안에 놓인다', () => {
  const component = getOwnershipComponent(buildOwnershipGraph(chain), 'square');
  const layout = layoutOwnershipGraph(component);
  const byId = new Map(layout.nodes.map(n => [n.id, n]));
  assert.equal(byId.get('034730.KS').level, 0);
  assert.equal(byId.get('402340.KS').level, 1);
  assert.equal(byId.get('000660.KS').level, 2);
  assert.equal(layout.focusId, '402340.KS');
  for (const node of layout.nodes) {
    assert.equal(node.width, 188);
    assert.equal(node.height, 84);
    assert.ok(node.x >= 0 && node.y >= 0);
    assert.ok(node.x + node.width <= layout.width);
    assert.ok(node.y + node.height <= layout.height);
  }
});

test('직접/간접 연결이 겹치면 가장 깊은 단계에 배치해 모든 비순환 화살표가 아래를 향한다', () => {
  const graph = buildOwnershipGraph([pair('a', 'A', [['B', 10], ['C', 10]]), pair('b', 'B', [['C', 10]])]);
  const layout = layoutOwnershipGraph(graph);
  assert.equal(layout.nodes.find(n => n.id === 'C').level, 2);
  assert.ok(layout.edges.every(e => e.sourceLevel < e.targetLevel));
});

test('순환출자/자기주식 루프를 보존하면서 배치와 탐색이 종료한다', () => {
  const graph = buildOwnershipGraph([pair('a', 'A', [['B', 10], ['A', 1]]), pair('b', 'B', [['A', 20], ['C', 5]])]);
  const component = getOwnershipComponent(graph, 'C');
  const layout = layoutOwnershipGraph(component);
  assert.equal(layout.nodes.length, 3);
  assert.equal(layout.edges.length, 4);
  assert.equal(layout.nodes.find(n => n.id === 'A').level, layout.nodes.find(n => n.id === 'B').level);
  assert.equal(layout.nodes.find(n => n.id === 'C').level, 1);
  assert.equal(layout.edges.filter(e => e.cyclic).length, 3);
  assert.ok(layout.nodes.filter(n => n.cyclic).length === 2);
});

test('배치는 입력 순서와 무관하며 입력 데이터를 변경하지 않는다', () => {
  const before = JSON.stringify(chain);
  const forward = layoutOwnershipGraph(getOwnershipComponent(buildOwnershipGraph(chain), 'sk'));
  const reversed = chain.slice().reverse().map(p => ({ ...p, subsidiaries: p.subsidiaries.slice().reverse() }));
  const backward = layoutOwnershipGraph(getOwnershipComponent(buildOwnershipGraph(reversed), 'sk'));
  assert.deepEqual(forward, backward);
  assert.equal(JSON.stringify(chain), before);
});

test('누락/비정상 데이터와 빈 연결망을 안전하게 처리한다', () => {
  assert.deepEqual(buildOwnershipGraph(null), { nodes: [], edges: [] });
  const graph = buildOwnershipGraph([{ id: 'broken' }, pair('a', 'A', [['B', NaN], ['C', -2]])]);
  assert.ok(graph.edges.every(e => e.sharesHeld === null && e.stakePct === null));
  assert.deepEqual(getOwnershipComponent(graph, 'missing'), { nodes: [], edges: [], focusId: null });
  assert.equal(layoutOwnershipGraph({ nodes: [], edges: [] }).width, 0);
});

test('실제 config와 공시 데이터로 SK 3단계 및 다른 그룹과의 분리를 검증한다', () => {
  const configs = JSON.parse(readFileSync(new URL('../../config.json', import.meta.url)));
  const fundamentals = JSON.parse(readFileSync(new URL('../../data/fundamentals.json', import.meta.url)));
  const component = getOwnershipComponent(buildOwnershipGraph(configs, [], fundamentals), 'sk_square');
  assert.ok(component.nodes.some(n => n.id === '034730.KS'));
  assert.ok(component.nodes.some(n => n.id === '402340.KS'));
  assert.ok(component.nodes.some(n => n.id === '000660.KS'));
  assert.ok(!component.nodes.some(n => n.id === '006120.KS'));
  assert.ok(component.edges.some(e => e.source === '034730.KS' && e.target === '402340.KS'));
  assert.ok(component.edges.some(e => e.source === '402340.KS' && e.target === '000660.KS'));
  const layout = layoutOwnershipGraph(component);
  assert.equal(layout.nodes.find(n => n.id === '000660.KS').level, 2);
});

test('종목별 메타데이터 발행주식수로 말단 종목도 같은 기준의 지분율을 계산한다', () => {
  const metadata = { securities: { '000660.KS': {
    totalShares: 200, sharesAsOf: null, sharesObservedAt: '2026-11-02T04:00:00Z', sharesSource: 'KIS', sharesSourceField: 'lstn_stcn',
    price: 1000, priceAsOf: null, priceObservedAt: '2026-11-02T04:00:00Z', priceSource: 'KIS', priceSourceField: 'stck_prpr', currency: 'KRW',
  } } };
  const graph = buildOwnershipGraph(chain, [], null, metadata);
  const edge = graph.edges.find(e => e.target === '000660.KS');
  assert.equal(edge.stakePct, 10);
  assert.equal(edge.sharesSource, 'KIS');
  assert.equal(edge.sharesAsOf, null);
  assert.equal(edge.sharesObservedAt, '2026-11-02T04:00:00Z');
  const node = graph.nodes.find(n => n.id === '000660.KS');
  assert.equal(node.marketCap, 200000);
  assert.equal(node.sharesSourceField, 'lstn_stcn');
  assert.equal(node.priceSourceField, 'stck_prpr');
  assert.equal(node.marketCapAsOf, null);
  assert.equal(node.marketCapObservedAt, '2026-11-02T04:00:00Z');
});

test('시가총액은 조정시가총액 대신 해당 주식종류 발행주식수 × 더 최신 주가를 쓴다', () => {
  const configs = [pair('a', '000001.KS', [['000002.KS', 20]], 100)];
  const metadata = { securities: {
    '000001.KS': { totalShares: 120, sharesSource: 'KIS', price: 15, priceAsOf: '2026-11-02T02:00:00Z', currency: 'KRW' },
    '000002.KS': { totalShares: 200, sharesSource: 'KIS', price: 30, priceAsOf: '2026-11-02T04:00:00Z', currency: 'KRW' },
  } };
  const dashboard = { lastUpdated: '2026-11-02 12:00:00', pairs: [{ id: 'a', current: {
    holdingPrice: 20, subsidiaryPrice: 25, marketCap: 0.0000002, quoteSource: 'dashboard-feed',
  } }] };
  const graph = buildOwnershipGraph(configs, dashboard, null, metadata);
  const parent = graph.nodes.find(n => n.id === '000001.KS');
  const child = graph.nodes.find(n => n.id === '000002.KS');
  assert.equal(parent.marketCap, 120 * 20);
  assert.equal(parent.priceSource, 'dashboard-feed');
  assert.equal(parent.priceObservedAt, '2026-11-02 12:00:00');
  assert.equal(child.marketCap, 200 * 30);
  assert.equal(child.priceSource, 'securities');
  assert.equal(graph.edges[0].stakePct, 10);
});

test('거래일이 없는 제공자 스냅샷도 조회 시각을 비교하되 거래 기준일로 바꾸지 않는다', () => {
  const graph = buildOwnershipGraph([pair('a', '000001.KS', [])], {
    lastUpdated: '2026-11-02 12:00:00', pairs: [{ id: 'a', current: { holdingPrice: 20 } }],
  }, null, { securities: { '000001.KS': {
    totalShares: 100, price: 30, priceAsOf: null, priceObservedAt: '2026-11-02T04:00:00Z', currency: 'KRW',
  } } });
  assert.equal(graph.nodes[0].price, 30);
  assert.equal(graph.nodes[0].priceAsOf, null);
  assert.equal(graph.nodes[0].priceObservedAt, '2026-11-02T04:00:00Z');
});

test('해외 종목 시가총액에 대시보드의 원화환산 가격을 달러로 사용하지 않는다', () => {
  const graph = buildOwnershipGraph([pair('a', '000001.KS', [['AAPL', 10]])], {
    lastUpdated: '2026-11-02 14:00:00', pairs: [{ id: 'a', current: { subsidiaryPrice: 450000 } }],
  }, null, { securities: { AAPL: { totalShares: 1000, price: 300, priceAsOf: '2026-10-30', currency: 'USD' } } });
  const apple = graph.nodes.find(n => n.id === 'AAPL');
  assert.equal(apple.price, 300);
  assert.equal(apple.marketCap, 300000);
  assert.equal(apple.currency, 'USD');
});

test('메타데이터의 null·잘못된 숫자는 주식수/시가총액 0으로 변환하지 않는다', () => {
  const graph = buildOwnershipGraph([pair('a', '000001.KS', [['000002.KS', 10]])], [], null, {
    securities: { '000001.KS': null, '000002.KS': { totalShares: null, price: 0, currency: 'KRW' } },
  });
  const child = graph.nodes.find(n => n.id === '000002.KS');
  assert.equal(child.totalShares, null);
  assert.equal(child.price, null);
  assert.equal(child.marketCap, null);
  assert.equal(graph.edges[0].stakePct, null);
  assert.doesNotThrow(() => JSON.stringify(graph));
});

function exposureGraph(configs, additionalShares = {}) {
  return buildOwnershipGraph(configs, [], null, { securities: Object.fromEntries(
    Object.entries(additionalShares).map(([ticker, totalShares]) => [ticker, { totalShares, sharesSource: 'fixture' }]),
  ) });
}

test('간접 지분율은 경로의 지분율을 곱하고 직접 보유와 분리한다', () => {
  const graph = exposureGraph([pair('a', 'A', [['B', 30], ['C', 5]]), pair('b', 'B', [['C', 20]])], { C: 100 });
  const exposure = getOwnershipExposure(graph, 'A', 'C');
  assert.equal(exposure.directPct, 5);
  assert.equal(exposure.indirectPct, 6);
  assert.equal(exposure.totalPct, 11);
  assert.equal(exposure.complete, true);
  assert.deepEqual(exposure.paths.map(p => p.nodeIds), [['A', 'B', 'C'], ['A', 'C']]);
  assert.deepEqual(exposure.paths[0].stakePcts, [30, 20]);
  assert.equal(exposure.paths[0].pct, 6);
});

test('여러 간접 경로는 각각 곱한 뒤 합산하며 공유 말단 종목을 누락하지 않는다', () => {
  const graph = exposureGraph([
    pair('a', 'A', [['B', 30], ['C', 40]]), pair('b', 'B', [['D', 20]]), pair('c', 'C', [['D', 10]]),
  ], { D: 100 });
  const exposure = getOwnershipExposure(graph, 'a', 'D');
  assert.equal(exposure.directPct, 0);
  assert.equal(exposure.indirectPct, 10);
  assert.equal(exposure.paths.length, 2);
  assert.match(exposure.caveats.join(' '), /경제적 지분 노출/);
  assert.deepEqual(getOwnershipDescendants(graph, 'A').map(n => [n.id, n.distance]), [['B', 1], ['C', 1], ['D', 2]]);
});

test('일부 경로에 지분율이 없으면 알려진 직접 지분은 유지하고 합산은 하지 않는다', () => {
  const configs = [pair('a', 'A', [['B', 30], ['C', 5]]), pair('b', 'B', [['C', 20]])];
  configs[0].subsidiaries[1].stakePct = 5;
  const graph = buildOwnershipGraph(configs);
  const exposure = getOwnershipExposure(graph, 'A', 'C');
  assert.equal(exposure.directPct, 5);
  assert.equal(exposure.indirectPct, null);
  assert.equal(exposure.totalPct, null);
  assert.equal(exposure.complete, false);
  assert.ok(exposure.paths.find(p => p.edgeIds.length > 1).missing.includes('B→C'));
});

test('연관 순환출자는 단순 경로만 제시하고 간접 합산을 중단한다', () => {
  const graph = exposureGraph([pair('a', 'A', [['B', 30]]), pair('b', 'B', [['A', 20], ['C', 10]])], { C: 100 });
  const exposure = getOwnershipExposure(graph, 'A', 'C');
  assert.equal(exposure.hasCycle, true);
  assert.equal(exposure.paths.length, 1);
  assert.equal(exposure.paths[0].pct, 3);
  assert.equal(exposure.indirectPct, null);
  assert.equal(exposure.totalPct, null);
  assert.deepEqual(getOwnershipDescendants(graph, 'A').map(n => n.id), ['B', 'C']);
});

test('대상에 이르지 않는 다른 가지의 순환은 계산 가능한 경로를 막지 않는다', () => {
  const graph = exposureGraph([pair('a', 'A', [['B', 30], ['C', 20]]), pair('b', 'B', [['D', 10]]), pair('c', 'C', [['C', 10]])], { D: 100 });
  const exposure = getOwnershipExposure(graph, 'A', 'D');
  assert.equal(exposure.hasCycle, false);
  assert.equal(exposure.totalPct, 3);
});

test('경로 제한 초과나 100% 초과 자료는 그럴듯한 합계를 만들지 않는다', () => {
  const graph = exposureGraph([pair('a', 'A', [['B', 100], ['C', 100]]), pair('b', 'B', [['D', 100]]), pair('c', 'C', [['D', 100]])], { D: 100 });
  const excessive = getOwnershipExposure(graph, 'A', 'D');
  assert.equal(excessive.totalPct, null);
  assert.match(excessive.caveats.join(' '), /100%/);
  const limited = getOwnershipExposure(graph, 'A', 'D', { maxPaths: 1 });
  assert.equal(limited.truncated, true);
  assert.equal(limited.paths.length, 1);
  assert.equal(limited.totalPct, null);
});

test('도달할 수 없는 대상은 0%, 잘못된 종목이나 자기 노출은 계산 불가다', () => {
  const graph = exposureGraph([pair('a', 'A', [['B', 10]]), pair('c', 'C', [])], { B: 100 });
  assert.equal(getOwnershipExposure(graph, 'A', 'C').totalPct, 0);
  assert.equal(getOwnershipExposure(graph, 'A', 'unknown').totalPct, null);
  assert.equal(getOwnershipExposure(graph, 'A', 'A').totalPct, null);
});

test('중간법인을 거치는 설정 관계의 주석과 입력 기준일을 노출 결과까지 전달한다', () => {
  const configs = [pair('yp', '000670.KS', [['010130.KS', 5]])];
  const graph = buildOwnershipGraph(configs, [], { pairs: { yp: {
    subsidiaries: [{ ticker: '010130.KS', note: '100% 자회사 YPC에 현물출자', detail: { stakePct: 100 } }],
  } } }, { securities: { '010130.KS': { totalShares: 100, sharesObservedAt: '2026-11-02', sharesSource: 'KIS' } } });
  const exposure = getOwnershipExposure(graph, '000670.KS', '010130.KS');
  assert.equal(graph.edges[0].relationshipKind, 'tracked-indirect');
  assert.equal(exposure.totalPct, 5);
  assert.match(exposure.caveats.join(' '), /YPC/);
  assert.equal(exposure.paths[0].basis[0].sharesObservedAt, '2026-11-02');
});

test('실제 KCC 설정의 삼성전자 노출은 삼성물산·삼성생명·삼성화재의 3개 경로를 모두 반영한다', () => {
  const configs = JSON.parse(readFileSync(new URL('../../config.json', import.meta.url)));
  const samsungShares = 5969782550; // 수학 검증용 보통주 발행주식수 fixture, 실시간 수치로 주장하지 않는다.
  const graph = exposureGraph(configs, { '005930.KS': samsungShares });
  const exposure = getOwnershipExposure(graph, 'kcc', '005930');
  const kcc = configs.find(p => p.id === 'kcc');
  const ct = configs.find(p => p.id === 'samsung_ct');
  const life = configs.find(p => p.id === 'samsung_life');
  const fire = configs.find(p => p.id === 'samsung_fire');
  const held = (p, ticker) => p.subsidiaries.find(s => s.ticker === ticker).sharesHeld;
  const kccCt = held(kcc, ct.holdingTicker) / ct.holdingTotalShares;
  const ctElectronics = held(ct, '005930.KS') / samsungShares;
  const ctLife = held(ct, life.holdingTicker) / life.holdingTotalShares;
  const lifeElectronics = held(life, '005930.KS') / samsungShares;
  const lifeFire = held(life, fire.holdingTicker) / fire.holdingTotalShares;
  const fireElectronics = held(fire, '005930.KS') / samsungShares;
  const expected = 100 * kccCt * (ctElectronics + ctLife * lifeElectronics + ctLife * lifeFire * fireElectronics);
  assert.equal(exposure.directPct, 0);
  assert.equal(exposure.paths.length, 3);
  assert.deepEqual(exposure.paths.map(p => p.nodeIds.length).sort(), [3, 4, 5]);
  assert.ok(Math.abs(exposure.indirectPct - expected) < 1e-12);
  assert.ok(Math.abs(exposure.totalPct - expected) < 1e-12);
  assert.equal(exposure.complete, true);
});
