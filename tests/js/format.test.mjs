// node --test tests/js/ 로 실행. classic script(js/format.js)를 CommonJS로 로드한다.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const fmt = require('../../js/format.js');

test('escapeHtml: & < > " \' 모두 이스케이프', () => {
  assert.equal(
    fmt.escapeHtml('<a href="x">&\'</a>'),
    '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;',
  );
  assert.equal(fmt.escapeHtml(null), '');
  assert.equal(fmt.escapeHtml(undefined), '');
  assert.equal(fmt.escapeHtml(123), '123');
});

test('formatPrice: 1억 이상은 억 단위 변환, 그 외 천 단위 콤마, 비수치는 -', () => {
  assert.equal(fmt.formatPrice(250000000), '2.50억');
  assert.equal(fmt.formatPrice(100000000), '1.00억');
  assert.equal(fmt.formatPrice(99999999), '99,999,999');
  assert.equal(fmt.formatPrice(50000), '50,000');
  assert.equal(fmt.formatPrice(null), '-');
  assert.equal(fmt.formatPrice(NaN), '-');
  assert.equal(fmt.formatPrice('50000'), '-');
});

test('formatOk: 억 접미사, 비수치는 -', () => {
  assert.equal(fmt.formatOk(1234), '1,234억');
  assert.equal(fmt.formatOk(null), '-');
});

test('formatRatio: 소수 2자리 %, 비수치는 -', () => {
  assert.equal(fmt.formatRatio(12.3), '12.30%');
  assert.equal(fmt.formatRatio(0), '0.00%');
  assert.equal(fmt.formatRatio(null), '-');
  assert.equal(fmt.formatRatio(NaN), '-');
});

test('formatSignedPercent: 부호/0 정규화/비수치', () => {
  assert.equal(fmt.formatSignedPercent(2.5), '+2.50%');
  assert.equal(fmt.formatSignedPercent(-1.234), '-1.23%');
  assert.equal(fmt.formatSignedPercent(0.004), '0.00%'); // |v| < 0.005 → 0 취급
  assert.equal(fmt.formatSignedPercent(NaN), '-');
});

test('formatSignedPoints: 양수 +, 0은 0.00%p, NaN은 -', () => {
  assert.equal(fmt.formatSignedPoints(1.234), '+1.23%p');
  assert.equal(fmt.formatSignedPoints(0), '0.00%p');
  assert.equal(fmt.formatSignedPoints(-0.6), '-0.60%p');
  assert.equal(fmt.formatSignedPoints(NaN), '-');
  assert.equal(fmt.formatSignedPoints(null), '-');
});

test('formatMarketNumber: 지정 소수 자리 + 콤마, 비수치는 -', () => {
  assert.equal(fmt.formatMarketNumber(1234.5, 2), '1,234.50');
  assert.equal(fmt.formatMarketNumber(1234.567, 0), '1,235');
  assert.equal(fmt.formatMarketNumber(null, 2), '-');
});

test('formatPriceChange: 부호 표시, 미세값 0 정규화, 비수치는 빈 문자열', () => {
  assert.equal(fmt.formatPriceChange(1.5), '+1.50%');
  assert.equal(fmt.formatPriceChange(-2.345), '-2.35%');
  assert.equal(fmt.formatPriceChange(0.004), '0.00%');
  assert.equal(fmt.formatPriceChange(NaN), '');
  assert.equal(fmt.formatPriceChange(undefined), '');
});

test('getPriceChangeClass: 부호별 색상 클래스', () => {
  assert.equal(fmt.getPriceChangeClass(1), 'up-color');
  assert.equal(fmt.getPriceChangeClass(-1), 'down-color');
  assert.equal(fmt.getPriceChangeClass(0), 'flat-color');
  assert.equal(fmt.getPriceChangeClass(NaN), 'flat-color');
});

test('getDirectionClass: 0.004는 flat (0.005 미만 데드밴드)', () => {
  assert.equal(fmt.getDirectionClass(0.004), 'flat');
  assert.equal(fmt.getDirectionClass(-0.004), 'flat');
  assert.equal(fmt.getDirectionClass(0.005), 'up');
  assert.equal(fmt.getDirectionClass(1), 'up');
  assert.equal(fmt.getDirectionClass(-1), 'down');
  assert.equal(fmt.getDirectionClass(NaN), 'flat');
  assert.equal(fmt.getDirectionClass(undefined), 'flat');
});

test('formatKstTimestamp: UTC Date를 KST 문자열로', () => {
  assert.equal(
    fmt.formatKstTimestamp(new Date(Date.UTC(2026, 0, 1, 15, 30, 0))),
    '2026-01-02 00:30:00',
  );
  assert.equal(
    fmt.formatKstTimestamp(new Date(Date.UTC(2026, 5, 11, 0, 0, 0))),
    '2026-06-11 09:00:00',
  );
});

test('parseRawNumber: 콤마 제거 파싱, 빈값/비수치는 null', () => {
  assert.equal(fmt.parseRawNumber('1,234.56'), 1234.56);
  assert.equal(fmt.parseRawNumber('-50'), -50);
  assert.equal(fmt.parseRawNumber(12), 12);
  assert.equal(fmt.parseRawNumber(''), null);
  assert.equal(fmt.parseRawNumber(null), null);
  assert.equal(fmt.parseRawNumber(undefined), null);
  assert.equal(fmt.parseRawNumber('abc'), null);
});

test('pickDefined: null/undefined가 아닌 첫 값', () => {
  assert.equal(fmt.pickDefined(null, undefined, 0, 5), 0);
  assert.equal(fmt.pickDefined(undefined, 'a'), 'a');
  assert.equal(fmt.pickDefined(false, 1), false);
  assert.equal(fmt.pickDefined(null, undefined), null);
  assert.equal(fmt.pickDefined(), null);
});

test('getTickerCode: 접미사 제거', () => {
  assert.equal(fmt.getTickerCode('005930.KS'), '005930');
  assert.equal(fmt.getTickerCode('247540.KQ'), '247540');
  assert.equal(fmt.getTickerCode('AAPL'), 'AAPL');
  assert.equal(fmt.getTickerCode(null), '');
});

test('parseSeoulTimestamp: KST 문자열 → UTC ms (KST-9h)', () => {
  assert.equal(
    fmt.parseSeoulTimestamp('2026-06-11 09:30:00'),
    Date.UTC(2026, 5, 11, 0, 30, 0),
  );
  assert.equal(
    fmt.parseSeoulTimestamp('2026-01-01 00:00:00'),
    Date.UTC(2025, 11, 31, 15, 0, 0),
  );
  assert.equal(fmt.parseSeoulTimestamp('2026-06-11'), 0); // 형식 불일치
  assert.equal(fmt.parseSeoulTimestamp(''), 0);
});

test('parseDateKey/formatDateKey: 왕복 변환', () => {
  assert.equal(fmt.formatDateKey(fmt.parseDateKey('2026-06-11')), '2026-06-11');
  assert.equal(fmt.formatDateKey(fmt.parseDateKey('2024-01-05')), '2024-01-05');
  const d = fmt.parseDateKey('2026-02-28');
  assert.equal(d.getFullYear(), 2026);
  assert.equal(d.getMonth(), 1); // 0-indexed
  assert.equal(d.getDate(), 28);
  assert.equal(fmt.formatDateKey(new Date(2026, 0, 9)), '2026-01-09'); // 0 패딩
});
