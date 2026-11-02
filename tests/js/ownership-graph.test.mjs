import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
const { buildOwnershipGraph, getOwnershipComponent, layoutOwnershipGraph } = require('../../js/ownership-graph.js');

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

test('공시 지분율은 공시 기준일과 수량 불일치를 함께 보존한다', () => {
  const graph = buildOwnershipGraph(chain, [{ id: 'sk', fundamentals: {
    bookValueDate: '2026-06-30', bookValueReport: '2026 반기보고서', subsidiaries: [
      { ticker: '402340.KS', qty: 25, detail: { stakePct: 31.5 } },
    ],
  } }]);
  const edge = graph.edges.find(e => e.target === '402340.KS');
  assert.equal(edge.stakePct, 31.5);
  assert.equal(edge.stakeSource, 'disclosure');
  assert.equal(edge.reportDate, '2026-06-30');
  assert.equal(edge.quantityMismatch, true);
  assert.equal(edge.sharesHeld, 32);
  assert.equal(edge.disclosedShares, 25);
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
