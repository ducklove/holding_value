// node --test tests/js/ 로 실행. Value Compass 생태계 통합 계약을 고정한다.
//  - index.html: vc:theme-boot 마커(인라인 부트), vc-tokens.css → app.css 순서, vc-shell.js(defer),
//    <vc-shell tool="holding_value"> + 허브 링크 폴백, 보유 배지 ?v, 로컬 자산 캐시 버스팅
//  - 벤더링 파일(vc-shell.js / vc-tokens.css)이 저장소 루트(= Pages 루트)에 있다
//  - app-boot.js: 테마 토글 → VCShell.setTheme, 'vc:themechange' → 차트 재렌더,
//    localStorage 차단 환경 방어, 종목 선택 → VCShell.setStock
//  - render.js: 카드 자회사 가격줄에 보유 배지용 티커(data-portfolio-code)가 붙는다
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import vm from 'node:vm';

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = rel => readFileSync(path.join(rootDir, rel), 'utf-8');
const indexHtml = read('index.html');

// 허브 레지스트리(config/ecosystem.json) heldBadges.version — 허브 쪽 값이 바뀌면 함께 올린다.
const HELD_BADGES_VERSION = '20260930-vc';
const APP_SCRIPTS = [
  'js/format.js', 'js/calc.js', 'js/dashboard-core.js', 'js/render.js',
  'js/charts-ui.js', 'js/live-ui.js', 'js/app-boot.js',
];

// ---- index.html 구조 계약 -------------------------------------------------------------

test('head: vc:theme-boot 블록이 모든 스타일시트보다 앞에 한 번만 있고 채워져 있다', () => {
  const blocks = indexHtml.match(/<!-- vc:theme-boot -->[\s\S]*?<!-- \/vc:theme-boot -->/g) || [];
  assert.equal(blocks.length, 1);
  const block = blocks[0];
  assert.match(block, /<script>[\s\S]*vc-theme-boot v1[\s\S]*<\/script>/, 'sync-ecosystem.mjs --write로 채워야 한다');
  const bootAt = indexHtml.indexOf('<!-- vc:theme-boot -->');
  const firstStyle = Math.min(
    ...['<link rel="stylesheet"', '<style'].map(tag => indexHtml.indexOf(tag)).filter(i => i >= 0),
  );
  assert.ok(bootAt < firstStyle, 'theme-boot은 첫 스타일시트보다 앞이어야 한다 (FOUC 방지)');
  // 옛 자체 부트 스크립트는 제거됐다 (중복 부트 금지)
  assert.doesNotMatch(indexHtml, /iframe embed \+ theme/);
});

test('head: vc-tokens.css가 app.css보다 먼저, vc-shell.js는 defer로 로드된다', () => {
  const tokens = indexHtml.indexOf('<link rel="stylesheet" href="./vc-tokens.css?v=');
  const appCss = indexHtml.indexOf('<link rel="stylesheet" href="css/app.css?v=');
  assert.ok(tokens >= 0, 'vc-tokens.css <link> (./ 접두 — 레지스트리 vendor.src)');
  assert.ok(appCss > tokens, 'app.css는 토큰 뒤에 와야 alias가 동작한다');
  assert.match(indexHtml, /<script defer src="\.\/vc-shell\.js\?v=[^"]+"><\/script>/);
});

test('body 최상단에 <vc-shell tool="holding_value">와 허브 링크 폴백이 있다', () => {
  const body = indexHtml.slice(indexHtml.indexOf('<body>') + '<body>'.length).trimStart();
  assert.ok(body.startsWith('<vc-shell tool="holding_value">'), 'vc-shell은 body의 첫 요소');
  assert.match(
    indexHtml,
    /<vc-shell tool="holding_value"><a class="hub-link" href="https:\/\/ducklove\.duckdns\.org:3691" rel="noopener">Value Compass ↗<\/a><\/vc-shell>/,
  );
  // 헤더의 수제 허브 링크는 셸이 대체한다 (폴백 1개만 남는다)
  assert.equal((indexHtml.match(/class="hub-link"/g) || []).length, 1);
  // 자체 테마 토글은 유지된다 (VCShell.setTheme으로 위임)
  assert.match(indexHtml, /id="themeToggle"/);
});

test('보유 배지 스크립트 ?v가 허브 레지스트리 heldBadges.version과 같다', () => {
  const tags = [...indexHtml.matchAll(/portfolio-held-badges\.js\?v=([^"'&\s>]+)/g)].map(m => m[1]);
  assert.deepEqual(tags, [HELD_BADGES_VERSION]);
});

test('로컬 JS/CSS는 모두 캐시 버스팅 ?v=를 가진다', () => {
  for (const src of APP_SCRIPTS) {
    assert.match(indexHtml, new RegExp(`<script src="${src.replace('.', '\\.')}\\?v=[^"]+"></script>`), `${src} ?v= 누락`);
  }
  assert.match(indexHtml, /href="css\/app\.css\?v=[^"]+"/);
});

test('벤더링 파일이 저장소 루트(= Pages 루트)에 있고 허브 사본 헤더를 가진다', () => {
  for (const file of ['vc-shell.js', 'vc-tokens.css', 'pipeline/vc_publish.py']) {
    assert.ok(existsSync(path.join(rootDir, file)), `${file} 없음 — sync-ecosystem.mjs --write`);
  }
  const shell = read('vc-shell.js');
  assert.match(shell, /^\/\* vc-shell\.js v\d+\.\d+\.\d+/);
  assert.doesNotThrow(() => new vm.Script(shell, { filename: 'vc-shell.js' }), 'classic script');
  assert.match(shell, /"id":"holding_value"/, '인라인 레지스트리에 이 도구가 있다');
  assert.match(read('vc-tokens.css'), /--vc-up:/);
  // .nojekyll이 있어야 Pages(legacy 브랜치 빌드)가 파일을 가공 없이 그대로 서빙한다
  assert.ok(existsSync(path.join(rootDir, '.nojekyll')));
});

test('app.css: 상승·하락 색과 본문 폰트만 vc 토큰으로 alias한다 (한국 관례)', () => {
  const css = read('css/app.css');
  assert.match(css, /--up: var\(--vc-up\b/);
  assert.match(css, /--down: var\(--vc-down\b/);
  assert.match(css, /font-family: var\(--vc-font-sans\b/);
  // 표면·강조색은 1단계에서 건드리지 않는다
  assert.doesNotMatch(css, /--(bg|surface|accent): var\(--vc-/);
});

// ---- app-boot.js 행위 (vm + 최소 가짜 DOM) ------------------------------------------------

function makeElement(id) {
  const target = new EventTarget();
  return Object.assign(target, {
    id,
    title: '',
    textContent: '',
    innerHTML: '',
    dataset: {},
    style: {},
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] ?? null; },
    scrollIntoView() {},
    querySelectorAll() { return []; },
    click() { this.dispatchEvent(new Event('click')); },
  });
}

function makeStorage({ blocked = false, initial = {} } = {}) {
  const map = new Map(Object.entries(initial));
  const guard = () => { if (blocked) throw new Error('SecurityError: storage blocked'); };
  return {
    getItem(key) { guard(); return map.has(key) ? map.get(key) : null; },
    setItem(key, value) { guard(); map.set(key, String(value)); },
    removeItem(key) { guard(); map.delete(key); },
    map,
  };
}

// startDashboard를 실제로 실행하되 렌더/차트/라이브 팩토리는 호출 기록만 남기는 스텁으로 바꾼다.
async function bootDashboard({ search = '', theme = 'light', shell = true, storage = makeStorage() } = {}) {
  const elements = new Map();
  const document = new EventTarget();
  Object.assign(document, {
    readyState: 'complete',
    documentElement: { dataset: theme ? { theme } : {} },
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, makeElement(id));
      return elements.get(id);
    },
  });
  const calls = { renderChart: 0, renderPriceChart: 0, setTheme: [], setStock: [] };
  const VCShell = shell ? {
    setTheme(t) {
      calls.setTheme.push(t);
      document.documentElement.dataset.theme = t;
      document.dispatchEvent(new CustomEvent('vc:themechange', { detail: { theme: t } }));
      return t;
    },
    setStock(code, name) { calls.setStock.push([code, name ?? null]); },
    getTheme() { return document.documentElement.dataset.theme; },
  } : undefined;
  const location = { search, href: 'https://ducklove.github.io/holding_value/' + search, hostname: 'ducklove.github.io', protocol: 'https:' };
  const window = { location, addEventListener() {}, VCShell, CURRENT_DATA: null };
  const context = vm.createContext({
    console, URL, URLSearchParams, Date, Promise, Map, Set, Math, JSON, Event, CustomEvent, EventTarget,
    window, document, location, localStorage: storage,
    history: { replaceState(_, __, url) { location.href = url; } },
    requestAnimationFrame: fn => fn(),
    setTimeout, clearTimeout,
    fetch: async () => ({ ok: false, status: 404, json: async () => ({}) }),
  });
  for (const src of APP_SCRIPTS) vm.runInContext(read(src), context, { filename: src });
  let app = null;
  context.createDashboardRenderers = captured => {
    app = captured;
    const noop = () => {};
    return { updateLastUpdatedText: noop, renderTodayOverview: noop, renderCards: noop, renderTable: noop, renderStats: noop };
  };
  context.createDashboardCharts = () => ({
    renderChart() { calls.renderChart += 1; },
    renderPriceChart() { calls.renderPriceChart += 1; },
    bindPeriodBtns() {}, bindPricePeriodBtns() {}, bindDataZoomControls() {},
  });
  context.createDashboardLive = () => ({
    applyCurrentData() { return false; },
    ensureHistory() { return Promise.resolve(); },
    refreshCurrentPrices() { return Promise.resolve(false); },
    bindAutoRefresh() {}, bindRefreshButton() {},
  });
  context.__data = {
    lastUpdated: '2026-09-26 09:21:15',
    pairs: [
      { id: '_average', name: '중앙값', isAverage: true, current: { ratio: 180 }, history: [] },
      { id: 'youngpoong_koreazinc', name: '영풍→고려아연', holdingName: '영풍', holdingTicker: '000670.KS', current: { ratio: 760 }, history: [{ date: '2026-09-26', ratio: 760 }] },
      { id: 'hyosung_heavy', name: '효성→효성중공업', holdingName: '효성', holdingTicker: '004800.KS', current: { ratio: 330 }, history: [{ date: '2026-09-26', ratio: 330 }] },
    ],
  };
  vm.runInContext('startDashboard(__data)', context);
  await new Promise(resolve => setImmediate(resolve)); // Promise.all(ensureHistory) → hasRendered
  return { app, calls, document, storage, toggle: elements.get('themeToggle') };
}

test('테마 토글은 VCShell.setTheme을 호출하고, vc:themechange에 차트를 다시 그린다', async () => {
  const { calls, document, toggle } = await bootDashboard({ theme: 'light' });
  const before = calls.renderChart;
  toggle.click();
  assert.deepEqual(calls.setTheme, ['dark']);
  assert.equal(document.documentElement.dataset.theme, 'dark');
  assert.ok(calls.renderChart > before, '셸 이벤트로 비율 차트 재렌더');
  assert.ok(calls.renderPriceChart > 0);
  assert.equal(toggle.getAttribute('aria-label'), '일반 모드로 전환');
  // 다른 탭/허브 postMessage 등 셸 발 테마 변경도 차트를 다시 그린다
  const mid = calls.renderPriceChart;
  document.documentElement.dataset.theme = 'light';
  document.dispatchEvent(new CustomEvent('vc:themechange', { detail: { theme: 'light' } }));
  assert.ok(calls.renderPriceChart > mid);
  assert.equal(toggle.getAttribute('aria-label'), '다크 모드로 전환');
});

test('셸이 없으면 기존 로직으로 폴백해 공용 theme 키에 저장한다', async () => {
  const storage = makeStorage();
  const { calls, document, toggle } = await bootDashboard({ theme: 'dark', shell: false, storage });
  toggle.click();
  assert.equal(document.documentElement.dataset.theme, 'light');
  assert.equal(storage.map.get('theme'), 'light');
  assert.ok(calls.renderChart >= 2);
});

test('localStorage가 막혀도 부트·토글이 예외 없이 동작한다', async () => {
  const storage = makeStorage({ blocked: true });
  // 부트 블록이 data-theme을 못 정한 극단적 경우까지: 저장소 차단 → 기본 light
  const { document, toggle } = await bootDashboard({ theme: null, shell: false, storage });
  assert.equal(document.documentElement.dataset.theme, 'light');
  assert.doesNotThrow(() => toggle.click());
  assert.equal(document.documentElement.dataset.theme, 'dark'); // 저장 실패해도 현재 페이지엔 적용
});

test('부트 블록이 정한 data-theme(prefers-color-scheme 포함)을 덮어쓰지 않는다', async () => {
  const storage = makeStorage(); // 저장값 없음 → 부트가 OS 설정으로 dark를 골랐다고 가정
  const { document } = await bootDashboard({ theme: 'dark', shell: false, storage });
  assert.equal(document.documentElement.dataset.theme, 'dark');
  assert.equal(storage.map.has('theme'), false, '부트 시 저장하지 않는다');
});

test('종목 선택 → VCShell.setStock(코드, 지주사명), 평균 카드 → setStock(null)', async () => {
  const { app, calls } = await bootDashboard({ search: '?code=000670' });
  assert.deepEqual(calls.setStock.at(-1), ['000670', '영풍']);
  const hyosung = app.pairs.findIndex(pair => pair.id === 'hyosung_heavy');
  app.setSelectedIdx(hyosung, { syncUrl: true });
  assert.deepEqual(calls.setStock.at(-1), ['004800', '효성']);
  const avg = app.pairs.findIndex(pair => pair.isAverage);
  app.setSelectedIdx(avg, { syncUrl: true });
  assert.deepEqual(calls.setStock.at(-1), [null, null]);
});

// ---- render.js: 카드 자회사 보유 배지 티커 -------------------------------------------------

test('카드 자회사 가격줄에 data-portfolio-code 티커가 붙는다 (단일·다중 자회사)', () => {
  const cards = { innerHTML: '', querySelectorAll: () => [] };
  const context = vm.createContext({
    console, URLSearchParams, Date, Map,
    window: { location: { hostname: 'localhost', protocol: 'http:' } },
    document: { getElementById: () => cards },
  });
  for (const src of APP_SCRIPTS) vm.runInContext(read(src), context, { filename: src });
  context.app = {
    selectedIdx: 0,
    holdingCodeById: {},
    isPairPinned: () => false,
    pairs: [
      { id: 'single', name: '영풍→고려아연', holdingName: '영풍', holdingTicker: '000670.KS', subsidiaryName: '고려아연',
        current: { ratio: 760, holdingPrice: 43200, subsidiaryPrice: 1116000, subsidiaryChange: 0.5 },
        fundamentals: { subsidiaries: [{ name: '고려아연', ticker: '010130.KS' }] }, percentileHistory: [], history: [] },
      { id: 'multi', name: '효성→효성중공업', holdingName: '효성', holdingTicker: '004800.KS',
        current: { ratio: 330, holdingPrice: 163900, subsidiaries: [
          { name: '효성중공업', price: 2892000, change: -0.3, ratio: 319 },
          { name: '효성티앤씨', price: 311000, change: -0.6, ratio: 10 },
        ] }, percentileHistory: [], history: [] },
    ],
    // live-ui가 config.json을 받으면 채우는 맵 (다중 자회사는 여기서만 티커를 안다고 가정)
    pairConfigById: new Map([['multi', { subsidiaries: [
      { name: '효성중공업', ticker: '298040.KS' }, { name: '효성티앤씨', ticker: '298020.KS' },
    ] }]]),
  };
  vm.runInContext('Object.assign(app, createDashboardRenderers(app)); app.renderCards()', context);
  const html = cards.innerHTML;
  assert.match(html, /class="price-label" data-portfolio-code="010130\.KS"[^>]*>고려아연</);
  assert.match(html, /class="price-label" data-portfolio-code="298040\.KS"[^>]*>효성중공업</);
  assert.match(html, /class="price-label" data-portfolio-code="298020\.KS"[^>]*>효성티앤씨</);
  assert.match(html, /class="price-label" data-portfolio-code="000670\.KS"[^>]*>영풍</);
  assert.doesNotMatch(html, /undefined/);
});
