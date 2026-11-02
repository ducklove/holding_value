// 지분관계 그래프의 데이터/배치 순수 로직. classic script와 Node에서 같은 함수를 쓴다.
// 종목명 대신 거래소·주식 종류를 포함한 ticker로 연결한다.
function ownershipTicker(value) {
  return typeof value === 'string' ? value.trim().toUpperCase() : '';
}

function ownershipNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function ownershipStake(value) {
  var number = ownershipNumber(value);
  return number !== null && number >= 0 && number <= 100 ? number : null;
}

function ownershipSort(a, b) {
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

function ownershipTimestamp(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value < 1e12 ? value * 1000 : value;
  if (typeof value !== 'string' || !value) return null;
  var normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(value) ? value.replace(' ', 'T') + '+09:00' : value;
  var timestamp = Date.parse(normalized);
  return Number.isFinite(timestamp) ? timestamp : null;
}

// config가 연결/보유수량의 원본이며 dashboardPairs는 .fundamentals 공시를 보충한다.
// 시가비율(current.ratio)은 지분율이 아니므로 이 모델에서는 사용하지 않는다.
function buildOwnershipGraph(configPairs, dashboardPairs, fundamentals, securities) {
  var configs = (Array.isArray(configPairs) ? configPairs : []).filter(function(pair) {
    return pair && !pair.isAverage && ownershipTicker(pair.holdingTicker);
  }).slice().sort(function(a, b) { return ownershipSort({ id: a.id || '' }, { id: b.id || '' }); });
  var dashboardPayload = dashboardPairs && !Array.isArray(dashboardPairs) ? dashboardPairs : {};
  var dashboardRows = Array.isArray(dashboardPairs) ? dashboardPairs : dashboardPayload.pairs || [];
  var dashboardById = new Map(dashboardRows.filter(Boolean).map(function(pair) {
    return [pair.id, pair];
  }));
  var records = (fundamentals && fundamentals.pairs) || {};
  var securityByTicker = (securities && securities.securities) || {};
  var nodesById = new Map();
  var edgesById = new Map();

  function addNode(ticker, name) {
    var id = ownershipTicker(ticker);
    if (!id) return null;
    if (!nodesById.has(id)) {
      nodesById.set(id, {
        id: id, ticker: id, name: name || id, pairIds: [], totalShares: null,
        sharesAsOf: null, sharesObservedAt: null, sharesSource: null, sharesSourceField: null,
        shareCountBasis: /\.(KS|KQ)$/.test(id) ? 'listedShares' : null,
        currency: /\.(KS|KQ)$/.test(id) ? 'KRW' : null,
        price: null, priceAsOf: null, priceObservedAt: null, priceSource: null, priceSourceField: null,
        marketCap: null, marketCapAsOf: null, marketCapObservedAt: null, marketCapSource: null,
      });
    }
    return nodesById.get(id);
  }

  configs.forEach(function(pair) {
    var node = addNode(pair.holdingTicker, pair.holdingName);
    if (pair.holdingName) node.name = pair.holdingName;
    if (pair.id && !node.pairIds.includes(pair.id)) node.pairIds.push(pair.id);
    var total = ownershipNumber(pair.holdingTotalShares);
    if (total !== null && total > 0) {
      if (node.totalShares !== null && node.totalShares !== total) node.totalSharesConflict = true;
      node.totalShares = total;
      node.sharesSource = 'config';
      node.sharesAsOf = pair.sharesAsOf || null;
    }
    (Array.isArray(pair.subsidiaries) ? pair.subsidiaries : []).forEach(function(sub) {
      if (sub) addNode(sub.ticker, sub.name);
    });
  });
  nodesById.forEach(function(node) {
    if (node.totalSharesConflict) { node.totalShares = null; node.sharesSource = null; }
    var security = securityByTicker[node.id] || {};
    var shares = ownershipNumber(security.totalShares);
    if (shares !== null && shares > 0) {
      node.totalShares = shares;
      node.sharesAsOf = security.sharesAsOf || null;
      node.sharesObservedAt = security.sharesObservedAt || null;
      node.sharesSource = security.sharesSource || 'securities';
      node.sharesSourceField = security.sharesSourceField || security.sourceField || null;
    }
    if (security.currency) node.currency = security.currency;
    if (security.shareCountBasis) node.shareCountBasis = security.shareCountBasis;
    node.pairIds.sort();
  });

  function acceptPrice(node, price, asOf, observedAt, source, sourceField, priority) {
    var value = ownershipNumber(price);
    if (!node || value === null || value <= 0) return;
    var incomingTime = ownershipTimestamp(asOf) || ownershipTimestamp(observedAt);
    var previousTime = ownershipTimestamp(node.priceAsOf) || ownershipTimestamp(node.priceObservedAt);
    var priorityBefore = node._pricePriority || 0;
    if (node.price !== null) {
      if (incomingTime !== null && previousTime !== null && incomingTime < previousTime) return;
      if (incomingTime === null && previousTime !== null) return;
      if (incomingTime === previousTime && priority < priorityBefore) return;
    }
    node.price = value;
    node.priceAsOf = asOf || null;
    node.priceObservedAt = observedAt || null;
    node.priceSource = source || null;
    node.priceSourceField = sourceField || null;
    node._pricePriority = priority;
  }
  nodesById.forEach(function(node) {
    var security = securityByTicker[node.id] || {};
    acceptPrice(node, security.price, security.priceAsOf, security.priceObservedAt, security.priceSource || 'securities', security.priceSourceField, 1);
  });
  configs.forEach(function(pair) {
    var dashboard = dashboardById.get(pair.id);
    if (!dashboard) return;
    var current = dashboard.current || dashboard;
    var asOf = current.priceAsOf || dashboard.priceAsOf || null;
    var observedAt = current.priceObservedAt || dashboard.priceObservedAt || dashboardPayload.lastUpdated || dashboardPayload.generatedAt || null;
    var source = current.quoteSource || dashboard.quoteSource || 'dashboard';
    var holding = nodesById.get(ownershipTicker(pair.holdingTicker));
    if (/\.(KS|KQ)$/.test(holding.id)) acceptPrice(holding, current.holdingPrice, asOf, observedAt, source, 'holdingPrice', 3);
    (Array.isArray(pair.subsidiaries) ? pair.subsidiaries : []).forEach(function(sub) {
      var node = nodesById.get(ownershipTicker(sub.ticker));
      // 기존 대시보드의 해외 자회사 가격은 이미 KRW로 환산되어 있다.
      // 해외 시가총액에는 메타데이터의 원 통화 가격만 사용한다.
      if (!node || !/\.(KS|KQ)$/.test(node.id)) return;
      var quotes = Array.isArray(current.subsidiaries) ? current.subsidiaries : [];
      var quote = quotes.find(function(item) {
        return item && (item.ticker ? ownershipTicker(item.ticker) === node.id : item.name === sub.name);
      });
      var price = quote ? quote.price : pair.subsidiaries.length === 1 ? current.subsidiaryPrice : null;
      acceptPrice(node, price, quote && quote.priceAsOf || asOf, quote && quote.priceObservedAt || observedAt, quote && quote.quoteSource || source, quote ? 'subsidiaries.price' : 'subsidiaryPrice', 2);
    });
  });
  nodesById.forEach(function(node) {
    delete node._pricePriority;
    if (node.totalShares !== null && node.price !== null && node.currency) {
      node.marketCap = node.totalShares * node.price;
      node.marketCapAsOf = node.priceAsOf;
      node.marketCapObservedAt = node.priceObservedAt;
      node.marketCapSource = node.sharesSource + ' × ' + node.priceSource;
    }
  });

  configs.forEach(function(pair) {
    var source = ownershipTicker(pair.holdingTicker);
    var dashboard = dashboardById.get(pair.id);
    var record = (dashboard && dashboard.fundamentals) || records[pair.id] || {};
    (Array.isArray(pair.subsidiaries) ? pair.subsidiaries : []).forEach(function(sub) {
      if (!sub) return;
      var target = ownershipTicker(sub.ticker);
      if (!nodesById.has(target)) return;
      var id = source + '→' + target;
      var disclosed = (Array.isArray(record.subsidiaries) ? record.subsidiaries : []).find(function(item) {
        return item && ownershipTicker(item.ticker) === target;
      });
      var shares = ownershipNumber(sub.sharesHeld);
      if (shares !== null && shares < 0) shares = null;
      var reportedStake = disclosed && disclosed.detail ? ownershipStake(disclosed.detail.stakePct) : null;
      // 예외 매칭 주석에는 중간 법인(YPC)·다른 종류 주식·분할 전 수량이 포함될 수 있다.
      // 해당 주석이 있는 공시의 %를 대상 상장주식의 직접 지분율로 단정하지 않는다.
      if (disclosed && (disclosed.note || disclosed.matchHow === 'bsInvestments' || (disclosed.matchedNames || []).length > 1)) reportedStake = null;
      var stakePct = ownershipStake(sub.stakePct);
      var stakeSource = stakePct !== null ? 'config' : null;
      var reportDate = null;
      var targetNode = nodesById.get(target);
      var targetShares = targetNode.totalShares;
      if (stakePct === null && shares !== null && targetShares !== null) {
        stakePct = ownershipStake(shares / targetShares * 100);
        if (stakePct !== null) stakeSource = 'calculated';
      }
      if (stakePct === null && reportedStake !== null) {
        stakePct = reportedStake;
        stakeSource = 'disclosure';
        reportDate = record.bookValueDate || null;
      }
      var disclosedShares = disclosed ? ownershipNumber(disclosed.qty) : null;
      var quantityMismatch = shares !== null && shares > 0 && disclosedShares !== null && Math.abs(disclosedShares / shares - 1) > 0.02;
      var edge = {
        id: id,
        source: source,
        target: target,
        pairIds: pair.id ? [pair.id] : [],
        sharesHeld: shares,
        stakePct: stakePct,
        stakeSource: stakeSource,
        reportDate: reportDate,
        reportName: stakeSource === 'disclosure' ? record.bookValueReport || null : null,
        totalShares: targetShares,
        sharesAsOf: targetNode.sharesAsOf,
        sharesObservedAt: targetNode.sharesObservedAt,
        sharesSource: targetNode.sharesSource,
        shareCountBasis: targetNode.shareCountBasis,
        holdingAsOf: sub.sharesAsOf || pair.holdingsAsOf || null,
        disclosedStakePct: reportedStake,
        disclosureDate: record.bookValueDate || null,
        disclosedShares: disclosedShares,
        quantityMismatch: quantityMismatch,
        note: disclosed && disclosed.note ? disclosed.note : null,
        relationshipKind: disclosed && disclosed.note && /중간|현물출자|100% 자회사/.test(disclosed.note) ? 'tracked-indirect' : 'tracked',
      };
      if (!edgesById.has(id)) {
        edgesById.set(id, edge);
      } else {
        var existing = edgesById.get(id);
        edge.pairIds.forEach(function(pairId) {
          if (!existing.pairIds.includes(pairId)) existing.pairIds.push(pairId);
        });
        // 중복 페어는 합산하지 않는다. 서로 다른 설정값은 미확인 값으로 남긴다.
        if (existing.sharesHeld !== edge.sharesHeld || existing.stakePct !== edge.stakePct) {
          existing.sharesHeld = null;
          existing.stakePct = null;
          existing.stakeSource = null;
          existing.reportDate = null;
          existing.reportName = null;
          existing.conflictingRecords = true;
        }
      }
    });
  });
  return {
    nodes: Array.from(nodesById.values()).sort(ownershipSort),
    edges: Array.from(edgesById.values()).sort(ownershipSort),
  };
}

// 소유 방향의 위·아래를 모두 따라가 같은 연결망 전체를 반환한다.
function getOwnershipComponent(graph, tickerOrPairId) {
  var nodes = graph && Array.isArray(graph.nodes) ? graph.nodes : [];
  var edges = graph && Array.isArray(graph.edges) ? graph.edges : [];
  var query = ownershipTicker(tickerOrPairId);
  var focus = nodes.find(function(node) { return node.id === query; }) || nodes.find(function(node) {
    return node.pairIds.includes(tickerOrPairId);
  });
  if (!focus && /^\d{6}$/.test(query)) {
    var matches = nodes.filter(function(node) { return node.id.replace(/\.(KS|KQ)$/, '') === query; });
    if (matches.length === 1) focus = matches[0];
  }
  if (!focus) return { nodes: [], edges: [], focusId: null };
  var adjacent = new Map(nodes.map(function(node) { return [node.id, []]; }));
  edges.forEach(function(edge) {
    if (!adjacent.has(edge.source) || !adjacent.has(edge.target)) return;
    adjacent.get(edge.source).push(edge.target);
    adjacent.get(edge.target).push(edge.source);
  });
  var seen = new Set([focus.id]);
  var queue = [focus.id];
  for (var i = 0; i < queue.length; i++) {
    adjacent.get(queue[i]).forEach(function(id) {
      if (!seen.has(id)) { seen.add(id); queue.push(id); }
    });
  }
  return {
    nodes: nodes.filter(function(node) { return seen.has(node.id); }).sort(ownershipSort),
    edges: edges.filter(function(edge) { return seen.has(edge.source) && seen.has(edge.target); }).sort(ownershipSort),
    focusId: focus.id,
  };
}

function ownershipFindNode(graph, queryValue) {
  var nodes = graph && Array.isArray(graph.nodes) ? graph.nodes : [];
  var query = ownershipTicker(queryValue);
  var exact = nodes.find(function(node) { return node.id === query || (node.pairIds || []).includes(queryValue); });
  if (exact) return exact;
  var matches = /^\d{6}$/.test(query) ? nodes.filter(function(node) { return node.id.replace(/\.(KS|KQ)$/, '') === query; }) : [];
  return matches.length === 1 ? matches[0] : null;
}

// 기준 종목에서 소유 방향으로 도달하는 종목. 순환 시 기준 종목 자체는 제외한다.
function getOwnershipDescendants(graph, sourceId) {
  var source = ownershipFindNode(graph, sourceId);
  if (!source) return [];
  var nodes = new Map(graph.nodes.map(function(node) { return [node.id, node]; }));
  var next = new Map(graph.nodes.map(function(node) { return [node.id, []]; }));
  (graph.edges || []).forEach(function(edge) {
    if (next.has(edge.source) && nodes.has(edge.target)) next.get(edge.source).push(edge.target);
  });
  var distance = new Map([[source.id, 0]]);
  var queue = [source.id];
  for (var i = 0; i < queue.length; i++) {
    next.get(queue[i]).sort().forEach(function(id) {
      if (!distance.has(id)) { distance.set(id, distance.get(queue[i]) + 1); queue.push(id); }
    });
  }
  return queue.slice(1).map(function(id) {
    return Object.assign({}, nodes.get(id), { distance: distance.get(id) });
  });
}

// 여러 단계의 지분 노출은 경로별 지분율을 곱한 뒤 더한다. 순환출자에서는
// 단순 경로만 열거하며, 재귀적인 소유 효과를 임의로 합산하지 않는다.
function getOwnershipExposure(graph, sourceId, targetId, options) {
  var source = ownershipFindNode(graph, sourceId);
  var target = ownershipFindNode(graph, targetId);
  var result = {
    sourceId: source ? source.id : null, targetId: target ? target.id : null,
    directPct: null, indirectPct: null, totalPct: null,
    paths: [], complete: false, approximate: false, hasCycle: false, truncated: false, caveats: [],
  };
  if (!source || !target) { result.caveats.push('등록된 기준 종목과 대상 종목을 선택해 주세요.'); return result; }
  if (source.id === target.id) { result.caveats.push('같은 종목에 대한 자기 지분 노출은 계산하지 않습니다.'); return result; }
  var opts = options || {};
  var maxPaths = Math.max(1, Math.min(2000, opts.maxPaths || 200));
  var nodes = new Set(graph.nodes.map(function(node) { return node.id; }));
  var edges = (graph.edges || []).filter(function(edge) { return nodes.has(edge.source) && nodes.has(edge.target); }).slice().sort(ownershipSort);
  var outgoing = new Map(graph.nodes.map(function(node) { return [node.id, []]; }));
  var incoming = new Map(graph.nodes.map(function(node) { return [node.id, []]; }));
  edges.forEach(function(edge) { outgoing.get(edge.source).push(edge); incoming.get(edge.target).push(edge); });
  // 대상에 도달할 수 없는 가지는 경로 탐색과 순환 판정에서 제외한다.
  var canReachTarget = new Set([target.id]);
  var reverseQueue = [target.id];
  for (var r = 0; r < reverseQueue.length; r++) {
    incoming.get(reverseQueue[r]).forEach(function(edge) {
      if (!canReachTarget.has(edge.source)) { canReachTarget.add(edge.source); reverseQueue.push(edge.source); }
    });
  }
  var color = new Map();
  function detectCycle(id) {
    color.set(id, 1);
    outgoing.get(id).forEach(function(edge) {
      if (!canReachTarget.has(edge.target)) return;
      if (color.get(edge.target) === 1) result.hasCycle = true;
      else if (!color.has(edge.target)) detectCycle(edge.target);
    });
    color.set(id, 2);
  }
  if (canReachTarget.has(source.id)) detectCycle(source.id);
  var traversals = 0;
  function walk(id, visited, pathNodes, pathEdges) {
    if (++traversals > 20000) { result.truncated = true; return; }
    if (id === target.id) {
      if (result.paths.length >= maxPaths) { result.truncated = true; return; }
      var missing = pathEdges.filter(function(edge) { return ownershipStake(edge.stakePct) === null; }).map(function(edge) { return edge.id; });
      var pct = missing.length ? null : pathEdges.reduce(function(product, edge) { return product * edge.stakePct / 100; }, 100);
      result.paths.push({
        nodeIds: pathNodes.slice(), edgeIds: pathEdges.map(function(edge) { return edge.id; }),
        stakePcts: pathEdges.map(function(edge) { return ownershipStake(edge.stakePct); }), pct: pct, missing: missing,
        basis: pathEdges.map(function(edge) {
          return {
            edgeId: edge.id, stakeSource: edge.stakeSource, holdingAsOf: edge.holdingAsOf || null,
            sharesAsOf: edge.sharesAsOf || null, sharesObservedAt: edge.sharesObservedAt || null,
            disclosureDate: edge.disclosureDate || edge.reportDate || null,
          };
        }),
        caveats: Array.from(new Set(pathEdges.map(function(edge) { return edge.note; }).filter(Boolean))),
      });
      return;
    }
    outgoing.get(id).forEach(function(edge) {
      if (result.truncated || visited.has(edge.target) || !canReachTarget.has(edge.target)) return;
      visited.add(edge.target);
      pathNodes.push(edge.target); pathEdges.push(edge);
      walk(edge.target, visited, pathNodes, pathEdges);
      pathNodes.pop(); pathEdges.pop(); visited.delete(edge.target);
    });
  }
  if (canReachTarget.has(source.id)) walk(source.id, new Set([source.id]), [source.id], []);
  var direct = edges.find(function(edge) { return edge.source === source.id && edge.target === target.id; });
  result.directPct = direct ? ownershipStake(direct.stakePct) : 0;
  var indirectPaths = result.paths.filter(function(path) { return path.edgeIds.length > 1; });
  var missingIndirect = indirectPaths.some(function(path) { return path.pct === null; });
  result.indirectPct = missingIndirect ? null : indirectPaths.reduce(function(sum, path) { return sum + path.pct; }, 0);
  var pathEdgeIds = new Set(result.paths.flatMap(function(path) { return path.edgeIds; }));
  var usedEdges = edges.filter(function(edge) { return pathEdgeIds.has(edge.id); });
  result.approximate = usedEdges.some(function(edge) { return edge.stakeSource === 'calculated' || edge.note || !edge.reportDate; });
  if (usedEdges.some(function(edge) { return edge.stakeSource === 'calculated'; })) {
    result.caveats.push('설정 보유수량 ÷ 발행주식수로 계산한 지분율을 포함합니다. 보유수량과 주식수의 기준일이 다를 수 있습니다.');
  }
  result.paths.forEach(function(path) { path.caveats.forEach(function(note) { if (!result.caveats.includes(note)) result.caveats.push(note); }); });
  if (result.paths.some(function(path) { return path.missing.length; })) result.caveats.push('확인되지 않은 지분율이 있어 일부 경로를 계산할 수 없습니다.');
  if (result.hasCycle) {
    result.indirectPct = null;
    result.caveats.push('경로에 순환출자가 있습니다. 단순 경로만 표시하고 간접·합산 지분율은 계산하지 않습니다.');
  }
  if (result.truncated) {
    result.indirectPct = null;
    result.caveats.push('표시 가능한 경로 수를 초과하여 전체 지분율을 합산하지 않습니다.');
  }
  if (result.directPct !== null && result.indirectPct !== null) result.totalPct = result.directPct + result.indirectPct;
  if (result.totalPct !== null && result.totalPct > 100 + 1e-8) {
    result.totalPct = null;
    result.indirectPct = null;
    result.caveats.push('경로 합산이 100%를 초과합니다. 중복되거나 서로 다른 시점의 지분 자료를 확인해야 합니다.');
  }
  if (result.totalPct !== null && result.totalPct > 100) result.totalPct = 100; // 부동소수점 오차만 보정
  if (result.paths.length > 1) result.caveats.push('등록된 서로 다른 경로의 지분율 곱을 합산한 경제적 지분 노출이며 의결권·연결재무제표 지분율과 다를 수 있습니다.');
  result.complete = result.totalPct !== null && !result.hasCycle && !result.truncated;
  return result;
}

// 강결합요소를 먼저 묶고 DAG의 최장 깊이를 사용하므로 순환출자도 종료한다.
// x/y는 카드 좌상단. 같은 단계의 카드는 가로, 소유 방향은 위→아래로 배치한다.
function layoutOwnershipGraph(graph, options) {
  var opts = options || {};
  var nodeWidth = opts.nodeWidth || 188;
  var nodeHeight = opts.nodeHeight || 84;
  var siblingGap = opts.siblingGap === undefined ? 28 : opts.siblingGap;
  var levelGap = opts.levelGap === undefined ? 100 : opts.levelGap;
  var padding = opts.padding === undefined ? 40 : opts.padding;
  var nodes = (graph && Array.isArray(graph.nodes) ? graph.nodes : []).slice().sort(ownershipSort);
  var nodeIds = new Set(nodes.map(function(node) { return node.id; }));
  var edges = (graph && Array.isArray(graph.edges) ? graph.edges : []).filter(function(edge) {
    return nodeIds.has(edge.source) && nodeIds.has(edge.target);
  }).slice().sort(ownershipSort);
  if (!nodes.length) return { nodes: [], edges: [], focusId: graph && graph.focusId || null, width: 0, height: 0, levels: [] };
  var outgoing = new Map(nodes.map(function(node) { return [node.id, []]; }));
  edges.forEach(function(edge) { outgoing.get(edge.source).push(edge.target); });
  var sequence = 0;
  var index = new Map();
  var lowlink = new Map();
  var stack = [];
  var stacked = new Set();
  var components = [];
  function visit(id) {
    index.set(id, sequence);
    lowlink.set(id, sequence++);
    stack.push(id);
    stacked.add(id);
    outgoing.get(id).forEach(function(target) {
      if (!index.has(target)) {
        visit(target);
        lowlink.set(id, Math.min(lowlink.get(id), lowlink.get(target)));
      } else if (stacked.has(target)) {
        lowlink.set(id, Math.min(lowlink.get(id), index.get(target)));
      }
    });
    if (lowlink.get(id) === index.get(id)) {
      var component = [];
      var target;
      do { target = stack.pop(); stacked.delete(target); component.push(target); } while (target !== id);
      components.push(component.sort());
    }
  }
  nodes.forEach(function(node) { if (!index.has(node.id)) visit(node.id); });
  components.sort(function(a, b) { return ownershipSort({ id: a[0] }, { id: b[0] }); });
  var componentOf = new Map();
  components.forEach(function(ids, i) { ids.forEach(function(id) { componentOf.set(id, i); }); });
  var next = components.map(function() { return new Set(); });
  var indegree = components.map(function() { return 0; });
  var depths = components.map(function() { return 0; });
  edges.forEach(function(edge) {
    var from = componentOf.get(edge.source);
    var to = componentOf.get(edge.target);
    if (from !== to && !next[from].has(to)) { next[from].add(to); indegree[to]++; }
  });
  var queue = [];
  indegree.forEach(function(degree, i) { if (!degree) queue.push(i); });
  for (var q = 0; q < queue.length; q++) {
    var componentIndex = queue[q];
    next[componentIndex].forEach(function(targetIndex) {
      depths[targetIndex] = Math.max(depths[targetIndex], depths[componentIndex] + 1);
      if (--indegree[targetIndex] === 0) queue.push(targetIndex);
    });
  }
  var levels = [];
  nodes.forEach(function(node) {
    var level = depths[componentOf.get(node.id)];
    if (!levels[level]) levels[level] = [];
    levels[level].push(node.id);
  });
  var widest = Math.max.apply(null, levels.map(function(ids) { return ids.length; }));
  var width = padding * 2 + widest * nodeWidth + (widest - 1) * siblingGap;
  var positions = new Map();
  levels.forEach(function(ids, level) {
    var rowWidth = ids.length * nodeWidth + (ids.length - 1) * siblingGap;
    ids.forEach(function(id, column) {
      positions.set(id, {
        x: (width - rowWidth) / 2 + column * (nodeWidth + siblingGap),
        y: padding + level * (nodeHeight + levelGap),
        width: nodeWidth,
        height: nodeHeight,
        level: level,
        cyclic: components[componentOf.get(id)].length > 1 || outgoing.get(id).includes(id),
      });
    });
  });
  return {
    nodes: nodes.map(function(node) { return Object.assign({}, node, positions.get(node.id)); }),
    edges: edges.map(function(edge) {
      return Object.assign({}, edge, {
        sourceLevel: positions.get(edge.source).level,
        targetLevel: positions.get(edge.target).level,
        cyclic: componentOf.get(edge.source) === componentOf.get(edge.target),
      });
    }),
    focusId: graph && graph.focusId || null,
    width: width,
    height: padding * 2 + levels.length * nodeHeight + (levels.length - 1) * levelGap,
    levels: levels,
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { buildOwnershipGraph, getOwnershipComponent, layoutOwnershipGraph, getOwnershipExposure, getOwnershipDescendants };
}
