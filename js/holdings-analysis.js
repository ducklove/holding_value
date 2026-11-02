// 보유 지분·기간 기여·간접 지분가치를 한 화면에서 쓰기 위한 순수 데이터 조립.
// 직접 평가가치(억원)는 기존 계산을 보존하며 간접 가치는 합계에 더하지 않는다.
if (typeof module !== 'undefined' && module.exports) {
  var __holdingsAnalysisCore = require('./dashboard-core.js');
  var buildHoldingsDetailRows = __holdingsAnalysisCore.buildHoldingsDetailRows;
  var buildContributionRows = __holdingsAnalysisCore.buildContributionRows;
  var __holdingsAnalysisGraph = require('./ownership-graph.js');
  var getOwnershipExposure = __holdingsAnalysisGraph.getOwnershipExposure;
  var getOwnershipDescendants = __holdingsAnalysisGraph.getOwnershipDescendants;
}

function buildHoldingsOverview(pair, graph, hist) {
  if (!pair || pair.isAverage) return null;
  var numeric = function(value) { return typeof value === 'number' && Number.isFinite(value) ? value : null; };
  var ticker = function(value) { return typeof value === 'string' ? value.trim().toUpperCase() : ''; };
  var history = Array.isArray(hist) ? hist : [];
  var detail = buildHoldingsDetailRows(pair);
  var current = pair.current || {};
  var graphReady = !!(graph && Array.isArray(graph.nodes) && Array.isArray(graph.edges));
  var nodes = new Map((graphReady ? graph.nodes : []).map(function(node) { return [node.id, node]; }));
  var source = nodes.get(ticker(pair.holdingTicker)) || (graphReady ? graph.nodes.find(function(node) {
    return (node.pairIds || []).includes(pair.id);
  }) : null);
  var directEdges = source ? graph.edges.filter(function(edge) { return edge.source === source.id; }) : [];
  var nameToTickers = new Map();
  function registerName(name, id) {
    if (!name || !id) return;
    if (!nameToTickers.has(name)) nameToTickers.set(name, new Set());
    nameToTickers.get(name).add(id);
  }
  directEdges.forEach(function(edge) {
    var node = nodes.get(edge.target);
    if (node) registerName(node.name, node.id);
  });
  ((pair.fundamentals && pair.fundamentals.subsidiaries) || []).forEach(function(sub) {
    if (sub) registerName(sub.name, ticker(sub.ticker));
  });
  (detail ? detail.rows : []).forEach(function(row) { registerName(row.name, ticker(row.ticker)); });
  // 명시 ticker가 있는 과거 행에서 종목명 변경 이력도 수집한다.
  history.forEach(function(entry) {
    (Array.isArray(entry.subsidiaries) ? entry.subsidiaries : []).forEach(function(sub) {
      if (sub) registerName(sub.name, ticker(sub.ticker));
    });
  });
  function resolveTicker(name, explicit) {
    var id = ticker(explicit);
    if (id) return id;
    var matches = nameToTickers.get(name);
    return matches && matches.size === 1 ? Array.from(matches)[0] : '';
  }

  var directRows = (detail ? detail.rows : []).map(function(row) {
    var id = resolveTicker(row.name, row.ticker);
    var edge = directEdges.find(function(item) { return item.target === id; }) || null;
    var node = nodes.get(id) || null;
    var caveats = [];
    if (row.note) caveats.push(row.note);
    if (row.qtyMismatch || (edge && edge.quantityMismatch)) caveats.push('설정 보유수량과 공시 기말수량이 다릅니다.');
    return Object.assign({}, row, {
      type: 'direct', ticker: id, currency: 'KRW', node: node, edge: edge,
      reportedStakePct: numeric(row.stakePct),
      stakePct: edge ? numeric(edge.stakePct) : numeric(row.stakePct),
      stakeSource: edge ? edge.stakeSource : numeric(row.stakePct) !== null ? 'disclosure' : null,
      contribution: null,
      sources: { value: 'dashboard', priceCurrency: 'KRW', ownership: edge ? edge.stakeSource : 'disclosure' },
      caveats: caveats,
    });
  });

  var canonicalNameByTicker = new Map();
  directRows.forEach(function(row) { if (row.ticker) canonicalNameByTicker.set(row.ticker, row.name); });
  directEdges.forEach(function(edge) {
    var node = nodes.get(edge.target);
    if (node && !canonicalNameByTicker.has(node.id)) canonicalNameByTicker.set(node.id, node.name);
  });
  var canonicalLabels = new Map();
  var labelTickers = new Map();
  canonicalNameByTicker.forEach(function(name, id) {
    if (!labelTickers.has(name)) labelTickers.set(name, []);
    labelTickers.get(name).push(id);
  });
  canonicalNameByTicker.forEach(function(name, id) {
    var key = labelTickers.get(name).length > 1 ? name + ' [' + id + ']' : name;
    canonicalNameByTicker.set(id, key);
    canonicalLabels.set(key, name);
    registerName(key, id);
  });
  var contributionHistory = history.map(function(entry) {
    if (!Array.isArray(entry.subsidiaries)) return entry;
    return Object.assign({}, entry, { subsidiaries: entry.subsidiaries.map(function(sub) {
      var id = resolveTicker(sub.name, sub.ticker);
      return Object.assign({}, sub, { name: canonicalNameByTicker.get(id) || sub.name });
    }) });
  });
  var contribution = history.length >= 2 ? buildContributionRows(contributionHistory) : null;
  // 단일 자회사는 전체 지분가치 비율 자체가 그 자회사의 기여다.
  // 복수 자회사 자료가 일부 누락된 경우에는 전체 변화를 임의 배분하지 않는다.
  var singleContribution = false;
  if (!contribution && directRows.length === 1 && history.length >= 2 && directEdges.length <= 1
      && (!pair.subsidiaryName || String(pair.subsidiaryName).split('+').length <= 1)
      && (!Array.isArray(pair.subsidiaries) || pair.subsidiaries.length <= 1)
      && (!Array.isArray(current.subsidiaries) || current.subsidiaries.length <= 1)
      && history.every(function(entry) { return !Array.isArray(entry.subsidiaries) || entry.subsidiaries.length <= 1; })) {
    var first = numeric(history[0].ratio);
    var last = numeric(history[history.length - 1].ratio);
    if (first !== null && last !== null) {
      var delta = last - first;
      contribution = { rows: [{ name: directRows[0].name, first: first, last: last, delta: delta }], maxAbs: Math.abs(delta) || 1, totalDelta: delta };
      singleContribution = true;
    }
  }
  if (!contribution && history.length >= 2 && directRows.length && contributionHistory.some(function(entry) { return Array.isArray(entry.subsidiaries) && entry.subsidiaries.length; })) {
    contribution = { rows: directRows.map(function(row) { return { name: row.name }; }), maxAbs: 1, totalDelta: null };
  }
  if (contribution) {
    var firstEntry = contributionHistory[0];
    var lastEntry = contributionHistory[contributionHistory.length - 1];
    var boundaryRows = contribution.rows.map(function(row) {
      var firstSub = (firstEntry.subsidiaries || []).find(function(sub) { return sub.name === row.name; });
      var lastSub = (lastEntry.subsidiaries || []).find(function(sub) { return sub.name === row.name; });
      var firstValue = singleContribution ? numeric(firstEntry.ratio) : firstSub ? numeric(firstSub.ratio) : null;
      var lastValue = singleContribution ? numeric(lastEntry.ratio) : lastSub ? numeric(lastSub.ratio) : null;
      var caveats = [];
      if (firstValue === null) caveats.push('선택 구간 시작일의 자회사 기여 비율이 없습니다.');
      if (lastValue === null) caveats.push('선택 구간 종료일의 자회사 기여 비율이 없습니다.');
      return Object.assign({}, row, {
        name: canonicalLabels.get(row.name) || row.name,
        first: firstValue, last: lastValue,
        delta: firstValue !== null && lastValue !== null ? lastValue - firstValue : null,
        ticker: resolveTicker(row.name), caveats: caveats,
      });
    });
    directRows.forEach(function(row) {
      if (!boundaryRows.some(function(item) { return row.ticker && item.ticker ? item.ticker === row.ticker : item.name === row.name; })) {
        var key = canonicalNameByTicker.get(row.ticker) || row.name;
        var firstSub = (firstEntry.subsidiaries || []).find(function(sub) { return sub.name === key; });
        var lastSub = (lastEntry.subsidiaries || []).find(function(sub) { return sub.name === key; });
        var firstValue = firstSub ? numeric(firstSub.ratio) : null;
        var lastValue = lastSub ? numeric(lastSub.ratio) : null;
        var caveats = [];
        if (firstValue === null) caveats.push('선택 구간 시작일의 자회사 기여 비율이 없습니다.');
        if (lastValue === null) caveats.push('선택 구간 종료일의 자회사 기여 비율이 없습니다.');
        boundaryRows.push({ name: row.name, ticker: row.ticker, first: firstValue, last: lastValue,
          delta: firstValue !== null && lastValue !== null ? lastValue - firstValue : null, caveats: caveats });
      }
    });
    var firstTotal = numeric(firstEntry.ratio);
    var lastTotal = numeric(lastEntry.ratio);
    contribution = Object.assign({}, contribution, {
      firstDate: history[0].date || null, lastDate: history[history.length - 1].date || null,
      rows: boundaryRows,
      maxAbs: boundaryRows.reduce(function(max, row) { return row.delta !== null ? Math.max(max, Math.abs(row.delta)) : max; }, 0) || 1,
      totalDelta: firstTotal !== null && lastTotal !== null ? lastTotal - firstTotal : null,
    });
    directRows.forEach(function(row) {
      var match = row.ticker ? contribution.rows.find(function(item) { return item.ticker === row.ticker; }) : null;
      if (!match && (!nameToTickers.has(row.name) || nameToTickers.get(row.name).size <= 1)) {
        var candidates = contribution.rows.filter(function(item) { return item.name === row.name; });
        if (candidates.length === 1) match = candidates[0];
      }
      row.contribution = match ? { first: match.first, last: match.last, delta: match.delta, caveats: match.caveats } : null;
      if (match) row.caveats = row.caveats.concat(match.caveats);
    });
  }

  var indirectRows = [];
  if (source) {
    getOwnershipDescendants(graph, source.id).forEach(function(node) {
      var exposure = getOwnershipExposure(graph, source.id, node.id);
      var paths = exposure.paths.filter(function(path) { return path.edgeIds.length >= 2; });
      if (!paths.length) return;
      var indirectPct = numeric(exposure.indirectPct);
      var marketCap = numeric(node.marketCap);
      var nativeValueRaw = indirectPct !== null && marketCap !== null ? numeric(marketCap * indirectPct / 100) : null;
      var valueOk = node.currency === 'KRW' && nativeValueRaw !== null ? nativeValueRaw / 1e8 : null;
      var parentAdjustedMarketCapOk = numeric(current.marketCap);
      var parentMarketCapRatio = valueOk !== null && parentAdjustedMarketCapOk !== null && parentAdjustedMarketCapOk > 0
        ? valueOk / parentAdjustedMarketCapOk * 100 : null;
      var caveats = exposure.caveats.slice();
      if (marketCap === null) caveats.push('대상 종목 시가총액을 확인할 수 없어 간접 평가가치를 계산하지 않습니다.');
      caveats.push('간접 평가가치는 중간 보유회사에 포함된 가치를 다시 살펴보는 참고값이며 직접 보유 평가가치 합계에 더하지 않습니다.');
      var steps = paths.map(function(path) { return path.edgeIds.length; });
      indirectRows.push({
        type: 'indirect', name: node.name, ticker: node.id, node: node,
        currency: node.currency || null, price: numeric(node.price),
        priceAsOf: node.priceAsOf || null, priceObservedAt: node.priceObservedAt || null, priceSource: node.priceSource || null,
        rawTargetMarketCap: marketCap, marketCapAsOf: node.marketCapAsOf || null,
        marketCapObservedAt: node.marketCapObservedAt || null, marketCapSource: node.marketCapSource || null,
        indirectPct: indirectPct, directPct: exposure.directPct, totalPct: exposure.totalPct,
        nativeValueRaw: nativeValueRaw,
        valueOk: valueOk,
        parentAdjustedMarketCapOk: parentAdjustedMarketCapOk,
        parentMarketCapRatio: parentMarketCapRatio,
        sharesHeld: null, contribution: null,
        exposure: exposure, paths: paths, pathCount: paths.length,
        minSteps: Math.min.apply(null, steps), maxSteps: Math.max.apply(null, steps),
        sources: { shares: node.sharesSource || null, price: node.priceSource || null, sharesAsOf: node.sharesAsOf || null, sharesObservedAt: node.sharesObservedAt || null },
        caveats: caveats,
      });
    });
    indirectRows.sort(function(a, b) {
      if (a.currency === 'KRW' && b.currency === 'KRW') return (b.valueOk === null ? -Infinity : b.valueOk) - (a.valueOk === null ? -Infinity : a.valueOk) || a.ticker.localeCompare(b.ticker);
      return a.currency === 'KRW' ? -1 : b.currency === 'KRW' ? 1 : a.ticker.localeCompare(b.ticker);
    });
  }
  var knownDirect = directRows.filter(function(row) { return numeric(row.valueOk) !== null; });
  var indirectStatus = graph === null ? 'loading' : !graphReady || !source ? 'unavailable'
    : !indirectRows.length ? 'none'
    : indirectRows.some(function(row) { return row.nativeValueRaw === null || !row.exposure.complete; }) ? 'partial' : 'ready';
  return {
    directRows: directRows, indirectRows: indirectRows, contribution: contribution,
    totals: {
      directSum: knownDirect.length ? knownDirect.reduce(function(sum, row) { return sum + row.valueOk; }, 0) : null,
      directCount: directRows.length, directKnownCount: knownDirect.length,
      dashboardHoldingValueOk: numeric(current.holdingValue), indirectExcluded: true,
    },
    report: { label: detail ? detail.reportLabel : null, date: detail ? detail.reportDate : null },
    reportLabel: detail ? detail.reportLabel : null, reportDate: detail ? detail.reportDate : null,
    indirectStatus: indirectStatus,
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { buildHoldingsOverview };
}
