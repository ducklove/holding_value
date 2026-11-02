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

// config가 연결/보유수량의 원본이며 dashboardPairs는 .fundamentals 공시를 보충한다.
// 시가비율(current.ratio)은 지분율이 아니므로 이 모델에서는 사용하지 않는다.
function buildOwnershipGraph(configPairs, dashboardPairs, fundamentals) {
  var configs = (Array.isArray(configPairs) ? configPairs : []).filter(function(pair) {
    return pair && !pair.isAverage && ownershipTicker(pair.holdingTicker);
  }).slice().sort(function(a, b) { return ownershipSort({ id: a.id || '' }, { id: b.id || '' }); });
  var dashboardById = new Map((Array.isArray(dashboardPairs) ? dashboardPairs : []).filter(Boolean).map(function(pair) {
    return [pair.id, pair];
  }));
  var records = (fundamentals && fundamentals.pairs) || {};
  var nodesById = new Map();
  var edgesById = new Map();

  function addNode(ticker, name) {
    var id = ownershipTicker(ticker);
    if (!id) return null;
    if (!nodesById.has(id)) {
      nodesById.set(id, { id: id, ticker: id, name: name || id, pairIds: [], totalShares: null });
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
    }
    (Array.isArray(pair.subsidiaries) ? pair.subsidiaries : []).forEach(function(sub) {
      if (sub) addNode(sub.ticker, sub.name);
    });
  });
  nodesById.forEach(function(node) {
    if (node.totalSharesConflict) node.totalShares = null;
    node.pairIds.sort();
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
      if (stakePct === null && reportedStake !== null) {
        stakePct = reportedStake;
        stakeSource = 'disclosure';
        reportDate = record.bookValueDate || null;
      }
      var targetShares = nodesById.get(target).totalShares;
      if (stakePct === null && shares !== null && targetShares !== null) {
        stakePct = ownershipStake(shares / targetShares * 100);
        if (stakePct !== null) stakeSource = 'calculated';
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
        disclosedShares: disclosedShares,
        quantityMismatch: quantityMismatch,
        note: disclosed && disclosed.note ? disclosed.note : null,
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
  module.exports = { buildOwnershipGraph, getOwnershipComponent, layoutOwnershipGraph };
}
