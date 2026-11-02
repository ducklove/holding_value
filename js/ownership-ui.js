// 지분 연결도: config의 직접 보유 관계를 종목 코드로 연결한 탐색용 팝업.
// 계산 대시보드와 선택 상태를 분리하며, 상세 보기 버튼에서만 대시보드로 이동한다.
function createOwnershipGraphView(app) {
  let dialog, graph, graphPromise, component, layout, focusId, scale = 1;
  let opener = null, openerStock = '', openRequest = 0;
  const byId = () => new Map((component ? component.nodes : []).map(node => [node.id, node]));
  const el = id => dialog.querySelector('#' + id);
  const number = value => typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('ko-KR', { maximumFractionDigits: 2 }) : null;

  function ensureGraph() {
    if (graph) return Promise.resolve(graph);
    if (graphPromise) return graphPromise;
    graphPromise = app.loadHoldingConfigMap().then(function(configMap) {
      graph = buildOwnershipGraph(Array.from(configMap.values()), app.pairs);
      return graph;
    }).catch(function(err) {
      graphPromise = null;
      throw err;
    });
    return graphPromise;
  }

  function edgeLabel(edge) {
    const pct = number(edge.stakePct);
    return pct !== null ? pct + '%' : number(edge.sharesHeld) !== null ? number(edge.sharesHeld) + '주' : '보유';
  }

  function edgeDescription(edge) {
    const parts = [];
    if (number(edge.sharesHeld) !== null) parts.push(number(edge.sharesHeld) + '주 · 설정 수량');
    if (number(edge.stakePct) !== null) {
      if (edge.stakeSource === 'disclosure') parts.push('공시 지분율 ' + number(edge.stakePct) + '%' + (edge.reportDate ? ' · ' + edge.reportDate : ''));
      else if (edge.stakeSource === 'calculated') parts.push('설정 수량 / 발행주식수 ' + number(edge.stakePct) + '%');
      else parts.push('설정 지분율 ' + number(edge.stakePct) + '%');
    } else parts.push('지분율 미확인');
    if (edge.quantityMismatch) parts.push('공시 수량 ' + number(edge.disclosedShares) + '주 · 설정 수량과 차이 있음');
    if (edge.conflictingRecords) parts.push('등록 자료 간 수량·지분율 불일치');
    if (edge.note) parts.push(edge.note);
    return parts.join(' · ');
  }

  function createDialog() {
    dialog = document.createElement('dialog');
    dialog.className = 'ownership-dialog';
    dialog.setAttribute('aria-labelledby', 'ownershipTitle');
    dialog.setAttribute('aria-describedby', 'ownershipSubtitle');
    dialog.innerHTML = `
      <header class="ownership-header">
        <div><div class="ownership-kicker">OWNERSHIP MAP</div><h2 id="ownershipTitle">지분 연결도</h2><p id="ownershipSubtitle">연결된 상장 종목의 보유 관계를 살펴보세요.</p></div>
        <button type="button" class="ownership-close" aria-label="지분 연결도 닫기">✕</button>
      </header>
      <div class="ownership-toolbar" hidden>
        <label class="ownership-stock-control"><span>기준 종목</span><select id="ownershipStockSelect"></select></label>
        <div class="ownership-zoom-controls" aria-label="연결도 확대 및 축소">
          <button type="button" data-ownership-zoom="out" aria-label="연결도 축소">−</button><output id="ownershipZoom" aria-live="polite">100%</output><button type="button" data-ownership-zoom="in" aria-label="연결도 확대">+</button><button type="button" data-ownership-zoom="fit">전체 맞춤</button>
        </div>
      </div>
      <div class="ownership-status" id="ownershipStatus" role="status">연결 관계를 불러오는 중…</div>
      <div class="ownership-body" hidden>
        <div class="ownership-viewport" id="ownershipViewport" tabindex="0" role="region" aria-label="지분 관계 그래프. 종목을 선택하거나 스크롤해 탐색하세요."><div class="ownership-canvas" id="ownershipCanvas"><div class="ownership-stage" id="ownershipStage"></div></div></div>
        <aside class="ownership-inspector" id="ownershipInspector" aria-label="선택 종목의 직접 보유 관계" aria-live="polite"></aside>
      </div>
      <footer class="ownership-footnote">화살표는 보유 회사 → 보유 종목입니다. 대시보드에 등록된 지분 연결만 표시하며, 중간 법인 경유 등은 상세 주석을 참고하세요. 공시 지분율은 표시된 기준일 기준으로 설정 보유수량과 다를 수 있습니다.</footer>`;
    document.body.appendChild(dialog);
    dialog.querySelector('.ownership-close').addEventListener('click', () => dialog.close());
    dialog.addEventListener('close', function() {
      openRequest += 1;
      document.body.classList.remove('ownership-modal-open');
      const replacement = Array.from(document.querySelectorAll('[data-ownership-stock]'))
        .find(node => node.dataset.ownershipStock === openerStock);
      const target = opener && opener.isConnected ? opener : replacement;
      if (target) target.focus({ preventScroll: true });
    });
    dialog.addEventListener('click', function(event) {
      // showModal의 backdrop은 dialog 자신을 target으로 전달한다.
      if (event.target === dialog) {
        const rect = dialog.getBoundingClientRect();
        if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close();
      }
      const node = event.target.closest('[data-ownership-node]');
      if (node) {
        const fromInspector = node.classList.contains('ownership-relation');
        selectNode(node.dataset.ownershipNode, true);
        if (fromInspector) {
          const active = Array.from(dialog.querySelectorAll('.ownership-node')).find(button => button.dataset.ownershipNode === focusId);
          if (active) active.focus({ preventScroll: true });
        }
      }
      const zoom = event.target.closest('[data-ownership-zoom]');
      if (zoom) {
        if (zoom.dataset.ownershipZoom === 'fit') fitGraph();
        else setScale(scale * (zoom.dataset.ownershipZoom === 'in' ? 1.25 : 0.8));
      }
      if (event.target.closest('[data-ownership-dashboard]')) {
        const selected = byId().get(focusId);
        if (selected && selected.pairIds.length) {
          dialog.close();
          app.setSelectedPairById(selected.pairIds[0]);
        }
      }
      if (event.target.closest('[data-ownership-retry]')) openOwnershipGraph(openerStock, opener);
    });
    el('ownershipStockSelect').addEventListener('change', event => showComponent(event.target.value));
    window.addEventListener('resize', function() { if (dialog.open && layout) fitGraph(); });
  }

  async function openOwnershipGraph(stock, trigger) {
    if (!dialog) createDialog();
    const request = ++openRequest;
    opener = trigger || document.activeElement;
    openerStock = stock;
    if (!dialog.open) dialog.showModal();
    document.body.classList.add('ownership-modal-open');
    el('ownershipStatus').hidden = false;
    el('ownershipStatus').textContent = '연결 관계를 불러오는 중…';
    dialog.querySelector('.ownership-body').hidden = true;
    dialog.querySelector('.ownership-toolbar').hidden = true;
    try {
      await ensureGraph();
      if (request !== openRequest || !dialog.open) return;
      el('ownershipStockSelect').innerHTML = graph.nodes.slice()
        .sort((a, b) => a.name.localeCompare(b.name, 'ko'))
        .map(node => `<option value="${escapeHtml(node.id)}">${escapeHtml(node.name)} · ${escapeHtml(node.ticker)}</option>`).join('');
      showComponent(stock);
    } catch (err) {
      if (request !== openRequest || !dialog.open) return;
      el('ownershipStatus').innerHTML = '연결 관계를 불러오지 못했습니다. <button type="button" class="ownership-detail-link" data-ownership-retry>다시 시도</button>';
    }
  }

  function showComponent(stock) {
    component = getOwnershipComponent(graph, stock);
    if (!component.nodes.length) {
      el('ownershipStatus').hidden = false;
      el('ownershipStatus').textContent = '등록된 지분 관계가 없습니다.';
      return;
    }
    focusId = component.focusId;
    layout = layoutOwnershipGraph(component);
    el('ownershipStatus').hidden = true;
    dialog.querySelector('.ownership-body').hidden = false;
    dialog.querySelector('.ownership-toolbar').hidden = false;
    renderGraph();
    selectNode(focusId, false);
    requestAnimationFrame(fitGraph);
  }

  function renderGraph() {
    const nodes = new Map(layout.nodes.map(node => [node.id, node]));
    const labelBoxes = [];
    function labelPosition(source, target, sameLevel, text) {
      const halfWidth = Math.max(24, text.length * 3.5 + 6);
      const x1 = source.x + source.width / 2, y1 = source.y + source.height;
      const x2 = target.x + target.width / 2, y2 = target.y;
      const candidates = sameLevel ? [{ x: (x1 + x2) / 2, y: y1 + 26 }] : [0.5, 0.4, 0.6, 0.3, 0.7, 0.2, 0.8].map(function(t) {
        const u = 1 - t, middle = (y1 + y2) / 2;
        return {
          x: u * u * u * x1 + 3 * u * u * t * x1 + 3 * u * t * t * x2 + t * t * t * x2,
          y: u * u * u * y1 + 3 * u * u * t * middle + 3 * u * t * t * middle + t * t * t * (y2 - 6) - 8,
        };
      });
      function boxAt(point) { return { left: point.x - halfWidth, right: point.x + halfWidth, top: point.y - 13, bottom: point.y + 5 }; }
      function overlaps(a, b) { return a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top; }
      const obstacles = layout.nodes.map(node => ({ left: node.x - 5, right: node.x + node.width + 5, top: node.y - 5, bottom: node.y + node.height + 5 }));
      const clearOfNodes = candidates.filter(point => !obstacles.some(box => overlaps(boxAt(point), box)));
      const chosen = clearOfNodes.find(point => !labelBoxes.some(box => overlaps(boxAt(point), box))) || clearOfNodes[0] || candidates[0];
      labelBoxes.push(boxAt(chosen));
      return chosen;
    }
    const paths = layout.edges.map(function(edge) {
      const source = nodes.get(edge.source), target = nodes.get(edge.target);
      const sameLevel = source.level === target.level;
      const x1 = source.x + source.width / 2, y1 = source.y + source.height;
      const x2 = target.x + target.width / 2, y2 = target.y;
      const middle = (y1 + y2) / 2;
      const d = sameLevel
        ? `M ${source.x + source.width} ${source.y + source.height / 2} C ${source.x + source.width + 48} ${source.y + source.height + 52}, ${target.x - 48} ${target.y + target.height + 52}, ${target.x} ${target.y + target.height / 2}`
        : `M ${x1} ${y1} C ${x1} ${middle}, ${x2} ${middle}, ${x2} ${y2 - 6}`;
      const label = labelPosition(source, target, sameLevel, edgeLabel(edge));
      return `<g class="ownership-edge" data-ownership-source="${escapeHtml(edge.source)}" data-ownership-target="${escapeHtml(edge.target)}"><title>${escapeHtml(source.name + ' → ' + target.name + ' · ' + edgeDescription(edge))}</title><path d="${d}" marker-end="url(#ownershipArrow)"></path><text class="ownership-edge-label" x="${label.x}" y="${label.y}" text-anchor="middle">${escapeHtml(edgeLabel(edge))}</text></g>`;
    }).join('');
    const buttons = layout.nodes.map(function(node) {
      const outgoing = component.edges.some(edge => edge.source === node.id);
      const incoming = component.edges.some(edge => edge.target === node.id);
      const role = incoming && outgoing ? '중간 보유 회사' : outgoing ? '보유 회사' : '보유 종목';
      return `<button type="button" class="ownership-node" data-ownership-node="${escapeHtml(node.id)}" style="left:${node.x}px;top:${node.y}px;width:${node.width}px;height:${node.height}px" aria-pressed="false"><span class="ownership-node-role">${role}</span><strong class="ownership-node-name">${escapeHtml(node.name)}</strong><span class="ownership-node-code">${escapeHtml(node.ticker)}</span></button>`;
    }).join('');
    const stage = el('ownershipStage');
    stage.style.width = layout.width + 'px';
    stage.style.height = layout.height + 'px';
    stage.innerHTML = `<svg class="ownership-edges" width="${layout.width}" height="${layout.height}" aria-hidden="true"><defs><marker id="ownershipArrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="var(--accent)"></path></marker></defs>${paths}</svg>${buttons}`;
  }

  function selectNode(id, reveal) {
    const node = byId().get(id);
    if (!node) return;
    focusId = id;
    el('ownershipStockSelect').value = id;
    el('ownershipSubtitle').textContent = node.name + ' · 연결 종목 ' + component.nodes.length + '개 · 등록 보유 관계 ' + component.edges.length + '개';
    dialog.querySelectorAll('.ownership-node').forEach(function(button) {
      const active = button.dataset.ownershipNode === id;
      button.classList.toggle('is-focus', active);
      button.setAttribute('aria-pressed', String(active));
    });
    dialog.querySelectorAll('.ownership-edge').forEach(function(edge) {
      edge.classList.toggle('is-focus', edge.dataset.ownershipSource === id || edge.dataset.ownershipTarget === id);
    });
    renderInspector(node);
    if (reveal) {
      const active = Array.from(dialog.querySelectorAll('.ownership-node')).find(button => button.dataset.ownershipNode === id);
      if (active) active.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  }

  function renderInspector(node) {
    const nodes = byId();
    const incoming = component.edges.filter(edge => edge.target === node.id);
    const outgoing = component.edges.filter(edge => edge.source === node.id);
    function relations(edges, parent) {
      if (!edges.length) return '<p class="ownership-empty">등록된 ' + (parent ? '상위 보유 회사' : '하위 보유 종목') + '가 없습니다.</p>';
      return `<ul class="ownership-relations">${edges.map(function(edge) {
        const other = nodes.get(parent ? edge.source : edge.target);
        return `<li><button type="button" class="ownership-relation" data-ownership-node="${escapeHtml(other.id)}"><span class="ownership-relation-name">${escapeHtml(other.name)} <strong>${escapeHtml(edgeLabel(edge))}</strong></span><span class="ownership-relation-meta">${escapeHtml(edgeDescription(edge))}</span></button></li>`;
      }).join('')}</ul>`;
    }
    el('ownershipInspector').innerHTML = `<h3 class="ownership-focus-name">${escapeHtml(node.name)}</h3><p class="ownership-focus-code">${escapeHtml(node.ticker)}</p>${node.pairIds.length ? '<button type="button" class="ownership-detail-link" data-ownership-dashboard>이 종목 대시보드 보기 ↗</button>' : ''}<section><h3>이 종목을 보유한 회사 <span>${incoming.length}</span></h3>${relations(incoming, true)}</section><section><h3>이 종목이 보유한 종목 <span>${outgoing.length}</span></h3>${relations(outgoing, false)}</section>`;
  }

  function setScale(next) {
    if (!layout) return;
    scale = Math.max(0.35, Math.min(1.6, next));
    el('ownershipStage').style.transform = 'scale(' + scale + ')';
    el('ownershipCanvas').style.width = Math.ceil(layout.width * scale) + 'px';
    el('ownershipCanvas').style.height = Math.ceil(layout.height * scale) + 'px';
    el('ownershipZoom').value = Math.round(scale * 100) + '%';
    el('ownershipZoom').textContent = Math.round(scale * 100) + '%';
  }

  function fitGraph() {
    if (!dialog.open || !layout) return;
    const viewport = el('ownershipViewport');
    // 모바일에서는 카드 글자를 읽을 수 있게 최소 70%로 유지하고 내부 스크롤한다.
    const minimum = window.innerWidth <= 768 ? 0.7 : 0.5;
    setScale(Math.max(minimum, Math.min(1, (viewport.clientWidth - 24) / layout.width, (viewport.clientHeight - 24) / layout.height)));
    const focus = layout.nodes.find(node => node.id === focusId);
    viewport.scrollTop = focus ? Math.max(0, (focus.y + focus.height / 2) * scale - viewport.clientHeight / 2) : 0;
    viewport.scrollLeft = focus ? Math.max(0, (focus.x + focus.width / 2) * scale - viewport.clientWidth / 2) : 0;
  }

  function bindOwnershipGraph() {
    document.addEventListener('click', function(event) {
      const trigger = event.target.closest('[data-ownership-stock]');
      if (!trigger) return;
      event.preventDefault();
      event.stopPropagation(); // 카드 선택보다 먼저 처리해 뒤쪽의 선택 상태를 유지한다.
      openOwnershipGraph(trigger.dataset.ownershipStock, trigger);
    }, true);
    // 시세 갱신과 같은 config 요청을 공유한다. 불러오면 자회사 이름 옆 진입점도 채운다.
    ensureGraph().then(function() {
      app.renderCards();
      app.renderTable();
      app.renderStats();
    }).catch(function() { /* 팝업을 열 때 오류 및 재시도 UI를 제공한다. */ });
  }

  return { bindOwnershipGraph, openOwnershipGraph };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createOwnershipGraphView };
}
