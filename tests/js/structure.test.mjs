// node --test tests/js/ 로 실행. index.html ↔ js/ 모듈 구조 계약을 고정한다.
// (빌드 없는 classic script 아키텍처라 스크립트 로드 순서가 곧 의존성 계약이다.
//  순서 변경·인라인 재비대·모듈 삭제 같은 구조 회귀를 잡는다.)
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import vm from 'node:vm';

const require = createRequire(import.meta.url);
const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const indexHtml = readFileSync(path.join(rootDir, 'index.html'), 'utf-8');

// index.html <script src> 로드 순서 = 의존성 계약 (index.html 주석 참고)
const SCRIPT_ORDER = [
  'js/format.js',
  'js/calc.js',
  'js/dashboard-core.js',
  'js/render.js',
  'js/charts-ui.js',
  'js/live-ui.js',
  'js/app-boot.js',
];

// 인라인 스크립트 총 줄수 상한 — 인라인 재비대(로직이 index.html로 돌아오는 것) 방지.
// 현재는 head 테마/embed 부트 + CURRENT_DATA 초기화 + 데이터 로드/startDashboard 호출 ≈ 30줄.
const MAX_INLINE_SCRIPT_LINES = 300;

function extractScripts(html) {
  const scripts = [];
  const re = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
  let match;
  while ((match = re.exec(html)) !== null) {
    const srcMatch = /src\s*=\s*["']([^"']+)["']/i.exec(match[1]);
    scripts.push({ src: srcMatch ? srcMatch[1] : null, body: match[2] });
  }
  return scripts;
}

test('index.html 스크립트 로드 순서가 의존성 계약과 일치한다', () => {
  const srcs = extractScripts(indexHtml)
    .map(script => script.src)
    .filter(Boolean);
  assert.deepEqual(srcs, SCRIPT_ORDER);
});

test('index.html 인라인 스크립트는 부트 수준으로만 유지된다 (줄수 상한)', () => {
  const inlineBodies = extractScripts(indexHtml)
    .filter(script => !script.src)
    .map(script => script.body);
  assert.ok(inlineBodies.length > 0, '테마/부트 인라인 스크립트는 존재해야 한다');
  const totalLines = inlineBodies.reduce(
    (sum, body) => sum + body.split('\n').filter(line => line.trim() !== '').length,
    0,
  );
  assert.ok(
    totalLines <= MAX_INLINE_SCRIPT_LINES,
    `인라인 스크립트 ${totalLines}줄 > 상한 ${MAX_INLINE_SCRIPT_LINES}줄 — 로직은 js/ 모듈로`,
  );
  // 부트 인라인은 모듈 전역(loadDashboardData/startDashboard)만 호출해야 한다
  const joined = inlineBodies.join('\n');
  assert.ok(joined.includes('loadDashboardData()'), '부트 인라인은 loadDashboardData를 호출해야 한다');
  assert.ok(joined.includes('startDashboard('), '부트 인라인은 startDashboard를 호출해야 한다');
});

test('js/ 모듈이 모두 존재하고 classic script로 파싱된다', () => {
  for (const src of SCRIPT_ORDER) {
    const code = readFileSync(path.join(rootDir, src), 'utf-8');
    assert.ok(code.trim().length > 0, `${src} 비어 있음`);
    // ES module 문법(import/export)이 섞이면 여기서 SyntaxError로 실패한다
    assert.doesNotThrow(() => new vm.Script(code, { filename: src }), `${src} classic script 파싱 실패`);
  }
});

test('모듈별 전역 진입점(UMD-lite export)이 유지된다', () => {
  const renderMod = require('../../js/render.js');
  assert.equal(typeof renderMod.createDashboardRenderers, 'function');

  const chartsMod = require('../../js/charts-ui.js');
  assert.equal(typeof chartsMod.createDashboardCharts, 'function');
  assert.equal(typeof chartsMod.createZoomState, 'function');
  assert.equal(typeof chartsMod.resetZoomState, 'function');
  assert.deepEqual(chartsMod.createZoomState(), { start: 0, end: 1 });

  const liveMod = require('../../js/live-ui.js');
  assert.equal(typeof liveMod.createDashboardLive, 'function');

  const bootMod = require('../../js/app-boot.js');
  assert.equal(typeof bootMod.loadDashboardData, 'function');
  assert.equal(typeof bootMod.startDashboard, 'function');
  assert.equal(bootMod.PINNED_PAIRS_STORAGE_KEY, 'holdingValuePinnedPairIds');
});

test('차트 데이터 줌은 키보드 슬라이더 계약을 제공한다', () => {
  const charts = readFileSync(path.join(rootDir, 'js/charts-ui.js'), 'utf-8');
  assert.match(indexHtml, /data-handle="start" role="slider" tabindex="0"/);
  assert.match(indexHtml, /data-handle="end" role="slider" tabindex="0"/);
  assert.match(charts, /ArrowLeft/);
  assert.match(charts, /PageUp/);
  assert.match(charts, /aria-valuenow/);
  assert.match(charts, /aria-valuetext/);
});

test('팩토리가 만든 함수 묶음이 app 계약(함수 이름)을 유지한다', () => {
  // DOM 없이도 팩토리 호출 자체는 함수 묶음만 만들므로 안전하다 (호출 전까지 DOM 미접근).
  const { createDashboardRenderers } = require('../../js/render.js');
  const { createDashboardCharts } = require('../../js/charts-ui.js');
  const { createDashboardLive } = require('../../js/live-ui.js');
  const app = {};
  const renderers = createDashboardRenderers(app);
  assert.deepEqual(Object.keys(renderers), [
    'updateLastUpdatedText',
    'renderTodayOverview',
    'renderCards',
    'renderTable',
    'renderStats',
  ]);
  const charts = createDashboardCharts(app);
  assert.deepEqual(Object.keys(charts), [
    'renderChart',
    'renderPriceChart',
    'bindPeriodBtns',
    'bindPricePeriodBtns',
    'bindDataZoomControls',
  ]);
  const live = createDashboardLive(app);
  assert.deepEqual(Object.keys(live), [
    'applyCurrentData',
    'ensureHistory',
    'refreshCurrentPrices',
    'bindAutoRefresh',
    'bindRefreshButton',
  ]);
  for (const bundle of [renderers, charts, live]) {
    for (const [name, fn] of Object.entries(bundle)) {
      assert.equal(typeof fn, 'function', `${name}은 함수여야 한다`);
    }
  }
});
