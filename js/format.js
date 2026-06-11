// js/format.js — 순수 포맷/파싱 유틸 (classic script 전역 공유)
// GitHub Pages 정적 서빙 환경이라 ES module 대신 전역 함수로 공유한다.
// 로드 순서: js/format.js → js/calc.js → index.html 본체(startDashboard).

function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatPrice(n) {
  if (typeof n !== 'number' || !Number.isFinite(n)) return '-';
  if (Math.abs(n) >= 100000000) {
    return (n / 100000000).toLocaleString('ko-KR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }) + '억';
  }
  return n.toLocaleString('ko-KR', { maximumFractionDigits: 0 });
}

function formatOk(n) {
  if (typeof n !== 'number' || !Number.isFinite(n)) return '-';
  return n.toLocaleString('ko-KR') + '억';
}

function formatRatio(n) {
  return typeof n === 'number' && Number.isFinite(n) ? n.toFixed(2) + '%' : '-';
}

function formatSignedPercent(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
  const normalized = Math.abs(value) < 0.005 ? 0 : value;
  const sign = normalized > 0 ? '+' : '';
  return sign + normalized.toFixed(2) + '%';
}

function formatSignedPoints(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
  const normalized = Math.abs(value) < 0.005 ? 0 : value;
  const sign = normalized > 0 ? '+' : '';
  return sign + normalized.toFixed(2) + '%p';
}

function formatMarketNumber(value, decimals) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
  return value.toLocaleString('ko-KR', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function formatPriceChange(change) {
  if (typeof change !== 'number' || !Number.isFinite(change)) return '';
  const normalized = Math.abs(change) < 0.005 ? 0 : change;
  const sign = normalized > 0 ? '+' : '';
  return sign + normalized.toFixed(2) + '%';
}

function getPriceChangeClass(change) {
  if (typeof change !== 'number' || !Number.isFinite(change)) return 'flat-color';
  if (change > 0) return 'up-color';
  if (change < 0) return 'down-color';
  return 'flat-color';
}

function getDirectionClass(value) {
  if (typeof value !== 'number' || !Number.isFinite(value) || Math.abs(value) < 0.005) return 'flat';
  return value > 0 ? 'up' : 'down';
}

function formatKstTimestamp(date) {
  const kst = new Date(date.getTime() + 9 * 60 * 60 * 1000);
  return kst.toISOString().slice(0, 19).replace('T', ' ');
}

function parseRawNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = parseFloat(String(value).replace(/,/g, ''));
  return Number.isNaN(parsed) ? null : parsed;
}

function pickDefined() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = arguments[i];
    if (value !== null && value !== undefined) return value;
  }
  return null;
}

function getTickerCode(ticker) {
  return String(ticker || '').split('.')[0];
}

function parseSeoulTimestamp(value) {
  var match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(value);
  if (!match) return 0;
  return Date.UTC(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    Number(match[4]) - 9,
    Number(match[5]),
    Number(match[6])
  );
}

function parseDateKey(dateStr) {
  const parts = dateStr.split('-').map(Number);
  return new Date(parts[0], parts[1] - 1, parts[2]);
}

function formatDateKey(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + d;
}

// UMD-lite: Node(node:test) 환경에서만 CommonJS export. 브라우저에선 전역 함수로 사용.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    escapeHtml,
    formatPrice,
    formatOk,
    formatRatio,
    formatSignedPercent,
    formatSignedPoints,
    formatMarketNumber,
    formatPriceChange,
    getPriceChangeClass,
    getDirectionClass,
    formatKstTimestamp,
    parseRawNumber,
    pickDefined,
    getTickerCode,
    parseSeoulTimestamp,
    parseDateKey,
    formatDateKey,
  };
}
