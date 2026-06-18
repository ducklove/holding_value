import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const calc = require('../../js/calc.js');

test('parseProxyQuote applies falling rf to absolute Naver proxy rate', () => {
  const parsed = calc.parseProxyQuote({
    summary: {
      current_price: 237500,
      previous_close: 243500,
      change: 6000,
      change_pct: 2.46,
    },
    raw: { rf: '5', cv: 6000, cr: 2.46 },
  });

  assert.deepEqual(parsed, {
    price: 237500,
    previousPrice: 243500,
    changePct: -2.46,
  });
});

test('buildProxyMarketMetric applies falling sign to absolute index rate', () => {
  const metric = calc.buildProxyMarketMetric({
    market: 'kosdaq',
    index_code: '1001',
    summary: {
      current_price: 1002.65,
      change: 29.31,
      change_rate: 2.84,
    },
    raw: { prdy_vrss_sign: '5' },
  }, { id: 'KOSDAQ', name: 'KOSDAQ', priceDecimals: 2 });

  assert.equal(metric.change, -29.31);
  assert.equal(metric.changePct, -2.84);
});
