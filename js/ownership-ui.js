// 지분 연결도: config의 직접 보유 관계를 종목 코드로 연결한 탐색용 팝업.
// 계산 대시보드와 선택 상태를 분리하며, 상세 보기 버튼에서만 대시보드로 이동한다.
function createOwnershipGraphView(app) {
  let dialog, graph, graphPromise, graphConfigs, securities, component, layout, baseId, focusId, scale = 1;
  let opener = null, openerStock = '', openRequest = 0;
  const byId = () => new Map((component ? component.nodes : []).map(node => [node.id, node]));
  const el = id => dialog.querySelector('#' + id);
  const number = value => typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('ko-KR', { maximumFractionDigits: 2 }) : null;
  const pct = value => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '미확인';
    if (value > 0 && value < 0.000001) return '<0.000001%';
    const digits = value >= 1 ? 2 : value >= 0.01 ? 3 : value >= 0.0001 ? 4 : 6;
    return value.toLocaleString('ko-KR', { maximumFractionDigits: digits }) + '%';
  };
  const date = value => {
    if (!value) return '';
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
    const stamp = ownershipTimestamp(value);
    return stamp === null ? String(value) : new Intl.DateTimeFormat('sv-SE', {
      timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
    }).format(new Date(stamp)) + ' KST';
  };
  const sourceName = value => ({ kis_proxy: 'KIS 시세', kis_openapi: 'KIS 시세', yahoo_quote: 'Yahoo Finance', config: '등록 정보', disclosure: 'DART 공시', dashboard: '대시보드 시세', naver_browser: '네이버 증권', internal_proxy: '시세 API', mixed: '혼합 시세' })[value] || value || '출처 미확인';
  function money(value, currency) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '미확인';
    if (currency === 'KRW') {
      if (value >= 1e12) return (value / 1e12).toLocaleString('ko-KR', { maximumFractionDigits: 1 }) + '조';
      return (value / 1e8).toLocaleString('ko-KR', { maximumFractionDigits: 0 }) + '억';
    }
    if (currency === 'USD') {
      const unit = value >= 1e12 ? [1e12, 'T'] : value >= 1e9 ? [1e9, 'B'] : [1e6, 'M'];
      return '$' + (value / unit[0]).toLocaleString('ko-KR', { maximumFractionDigits: 2 }) + unit[1];
    }
    return number(value) + ' ' + (currency || '');
  }

  function rebuildGraph() {
    graph = buildOwnershipGraph(graphConfigs, app.stockData, null, securities);
    return graph;
  }

  function ensureGraph() {
    if (graph && securities && Object.keys(securities.securities || {}).length) return Promise.resolve(rebuildGraph());
    if (graphPromise) return graphPromise;
    graphPromise = Promise.all([
      app.loadHoldingConfigMap(),
      fetch('data/ownership-securities.json', { cache: 'no-store' })
        .then(function(response) { if (!response.ok) throw new Error('securities HTTP ' + response.status); return response.json(); })
        .catch(function() { return { securities: {} }; }),
    ]).then(function(results) {
      graphConfigs = Array.from(results[0].values());
      securities = results[1];
      graphPromise = null;
      return rebuildGraph();
    }).catch(function(err) {
      graphPromise = null;
      throw err;
    });
    return graphPromise;
  }

  function edgeLabel(edge) {
    return pct(edge.stakePct);
  }

  function edgeDescription(edge) {
    const parts = [];
    if (number(edge.sharesHeld) !== null) parts.push(number(edge.sharesHeld) + '주 · 설정 수량');
    if (number(edge.stakePct) !== null) {
      if (edge.stakeSource === 'disclosure') parts.push('공시 지분율 ' + number(edge.stakePct) + '%' + (edge.reportDate ? ' · ' + edge.reportDate : ''));
      else if (edge.stakeSource === 'calculated') parts.push('등록 보유수량 ÷ 대상 주식수 ' + number(edge.totalShares) + '주 = ' + pct(edge.stakePct));
      else parts.push('설정 지분율 ' + number(edge.stakePct) + '%');
    } else parts.push('지분율 미확인');
    if (edge.stakeSource === 'calculated') {
      const time = edge.sharesAsOf ? ' 기준 ' + date(edge.sharesAsOf) : edge.sharesObservedAt ? ' 조회 ' + date(edge.sharesObservedAt) : '';
      parts.push('대상 주식수 ' + sourceName(edge.sharesSource) + time);
    }
    if (edge.stakeSource !== 'disclosure' && number(edge.disclosedStakePct) !== null) parts.push('공시 ' + pct(edge.disclosedStakePct) + (edge.disclosureDate ? ' · ' + edge.disclosureDate : ''));
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
      <footer class="ownership-footnote">연결선은 등록된 보유 지분율, 시총은 해당 종류주식의 주식수 × 주가입니다. 간접 비율은 경로별 지분율을 곱해 합산한 참고값이며 의결권 비율과 다릅니다.</footer>`;
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
        if (zoom.dataset.ownershipZoom === 'fit') fitGraph(true);
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
      if (event.target.closest('[data-ownership-rebase]')) showComponent(focusId);
    });
    el('ownershipStockSelect').addEventListener('change', event => showComponent(event.target.value));
    dialog.addEventListener('change', function(event) {
      if (event.target.id !== 'ownershipTargetSelect') return;
      selectNode(event.target.value, true);
      el('ownershipTargetSelect').focus({ preventScroll: true });
    });
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
    baseId = component.focusId;
    focusId = baseId;
    layout = layoutOwnershipGraph(component, { nodeHeight: 100 });
    el('ownershipStatus').hidden = true;
    dialog.querySelector('.ownership-body').hidden = false;
    dialog.querySelector('.ownership-toolbar').hidden = false;
    renderGraph();
    selectNode(focusId, false);
    requestAnimationFrame(function() { fitGraph(); });
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
      return `<button type="button" class="ownership-node${node.id === baseId ? ' is-base' : ''}" data-ownership-node="${escapeHtml(node.id)}" style="left:${node.x}px;top:${node.y}px;width:${node.width}px;height:${node.height}px" aria-pressed="false" title="${escapeHtml(node.name + ' · 시가총액 ' + money(node.marketCap, node.currency))}"><span class="ownership-node-role">${role}${node.id === baseId ? '<span class="ownership-base-dot">기준</span>' : ''}</span><strong class="ownership-node-name">${escapeHtml(node.name)}</strong><span class="ownership-node-bottom"><span class="ownership-node-code">${escapeHtml(node.ticker)}</span><span class="ownership-node-cap">시총 ${escapeHtml(money(node.marketCap, node.currency))}</span></span></button>`;
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
    el('ownershipStockSelect').value = baseId;
    const base = byId().get(baseId);
    el('ownershipSubtitle').textContent = base.name + ' 기준 · ' + (baseId === id ? '' : node.name + ' 선택 · ') + '연결 종목 ' + component.nodes.length + '개';
    const exposure = baseId !== id ? getOwnershipExposure(component, baseId, id) : null;
    const pathEdges = new Set((exposure ? exposure.paths : []).flatMap(path => path.edgeIds));
    const pathNodes = new Set((exposure ? exposure.paths : []).flatMap(path => path.nodeIds));
    el('ownershipStage').classList.toggle('has-exposure', pathEdges.size > 0);
    dialog.querySelectorAll('.ownership-node').forEach(function(button) {
      const active = button.dataset.ownershipNode === id;
      button.classList.toggle('is-focus', active);
      button.classList.toggle('is-path', pathNodes.has(button.dataset.ownershipNode));
      button.setAttribute('aria-pressed', String(active));
    });
    dialog.querySelectorAll('.ownership-edge').forEach(function(edge) {
      const onPath = pathEdges.has(edge.dataset.ownershipSource + '→' + edge.dataset.ownershipTarget);
      edge.classList.toggle('is-path', onPath);
      edge.classList.toggle('is-focus', !exposure && (edge.dataset.ownershipSource === id || edge.dataset.ownershipTarget === id));
    });
    renderInspector(node, exposure);
    if (reveal) {
      revealNode(id);
    }
  }

  function revealNode(id) {
    const node = layout.nodes.find(item => item.id === id);
    if (!node) return;
    const viewport = el('ownershipViewport');
    viewport.scrollTop = Math.max(0, (node.y + node.height / 2) * scale - viewport.clientHeight / 2);
    viewport.scrollLeft = Math.max(0, (node.x + node.width / 2) * scale - viewport.clientWidth / 2);
  }

  function renderInspector(node, exposure) {
    const nodes = byId();
    const base = nodes.get(baseId);
    const incoming = component.edges.filter(edge => edge.target === node.id);
    const outgoing = component.edges.filter(edge => edge.source === node.id);
    const descendants = getOwnershipDescendants(component, baseId);
    const reachable = new Set(descendants.map(item => item.id));
    function relations(edges, parent) {
      if (!edges.length) return '<p class="ownership-empty">등록된 ' + (parent ? '상위 보유 회사' : '하위 보유 종목') + '가 없습니다.</p>';
      return `<ul class="ownership-relations">${edges.map(function(edge) {
        const other = nodes.get(parent ? edge.source : edge.target);
        return `<li><button type="button" class="ownership-relation" data-ownership-node="${escapeHtml(other.id)}" title="${escapeHtml(edgeDescription(edge))}"><span class="ownership-relation-name">${escapeHtml(other.name)} <strong>${escapeHtml(edgeLabel(edge))}</strong></span></button></li>`;
      }).join('')}</ul>`;
    }
    function renderExposure() {
      if (!exposure) return '';
      if (!exposure.paths.length) return `<section class="ownership-exposure"><div class="ownership-exposure-kicker">${escapeHtml(base.name)} 기준</div><p class="ownership-exposure-note">이 종목으로 이어지는 보유 경로가 없습니다. 기준 종목을 바꾸어 살펴보세요.</p></section>`;
      const allIndirect = exposure.paths.every(path => path.edgeIds.length > 1);
      const includesIntermediary = exposure.paths.some(path => path.edgeIds.some(id => component.edges.some(edge => edge.id === id && edge.relationshipKind === 'tracked-indirect')));
      const split = includesIntermediary ? '중간 법인 경유를 포함한 환산 지분율' : allIndirect ? '간접 보유 환산 비율' : '직접 ' + pct(exposure.directPct) + ' · 간접 ' + pct(exposure.indirectPct);
      const pathRows = exposure.paths.map(function(path) {
        const pathTitle = path.nodeIds.map(id => nodes.get(id).name).join(' → ');
        const formula = path.stakePcts.map(value => pct(value)).join(' × ') + ' = ' + pct(path.pct);
        return `<li><div class="ownership-path-title">${escapeHtml(pathTitle)}</div><div class="ownership-path-formula">${escapeHtml(formula)}</div></li>`;
      }).join('');
      return `<section class="ownership-exposure"><div class="ownership-exposure-kicker">${escapeHtml(base.name)} → ${escapeHtml(node.name)}</div><div class="ownership-exposure-value">${pct(exposure.totalPct)}</div><div class="ownership-exposure-split">${split}</div><details class="ownership-calculation" data-ownership-detail="paths"><summary>${exposure.paths.length}개 경로 · 계산 근거</summary><ol class="ownership-path-list">${pathRows}</ol><p class="ownership-exposure-note">경로마다 지분율을 곱한 뒤 합산합니다. 등록 보유수량과 대상 주식수의 시점 차이가 있으며, 등록되지 않은 지분은 포함하지 않습니다.</p>${exposure.caveats.map(note => `<p class="ownership-exposure-note">${escapeHtml(note)}</p>`).join('')}</details></section>`;
    }
    const indirect = descendants.map(function(item) {
      return { node: item, exposure: getOwnershipExposure(component, baseId, item.id) };
    }).filter(item => item.exposure.paths.some(path => path.edgeIds.length > 1))
      .sort((a, b) => (b.exposure.indirectPct || 0) - (a.exposure.indirectPct || 0));
    function indirectList(items) {
      return `<ul class="ownership-relations">${items.map(item => `<li><button type="button" class="ownership-relation" data-ownership-node="${escapeHtml(item.node.id)}"><span class="ownership-relation-name">${escapeHtml(item.node.name)}<strong>${pct(item.exposure.indirectPct)}</strong></span><span class="ownership-relation-meta">${item.exposure.paths.filter(path => path.edgeIds.length > 1).length}개 간접 경로</span></button></li>`).join('')}</ul>`;
    }
    const options = [base].concat(component.nodes.filter(item => item.id !== baseId).sort(function(a, b) {
      return Number(reachable.has(b.id)) - Number(reachable.has(a.id)) || a.name.localeCompare(b.name, 'ko');
    })).map(function(item) {
      return `<option value="${escapeHtml(item.id)}"${item.id === node.id ? ' selected' : ''}>${escapeHtml(item.name)}${item.id === baseId ? ' · 기준' : reachable.has(item.id) ? ' · 보유 대상' : ''}</option>`;
    }).join('');
    const capTime = node.marketCapAsOf ? '시세 기준 ' + date(node.marketCapAsOf) : node.marketCapObservedAt ? '시세 조회 ' + date(node.marketCapObservedAt) : '시세 시점 미확인';
    const shareTime = node.sharesAsOf ? '기준 ' + date(node.sharesAsOf) : node.sharesObservedAt ? '조회 ' + date(node.sharesObservedAt) : '기준일 미확인';
    const sources = incoming.concat(outgoing).map(edge => `<p>${escapeHtml(nodes.get(edge.source).name + ' → ' + nodes.get(edge.target).name + ': ' + edgeDescription(edge))}</p>`).join('');
    el('ownershipInspector').innerHTML = `<label class="ownership-target-control"><span>살펴볼 종목</span><select id="ownershipTargetSelect">${options}</select></label><h3 class="ownership-focus-name">${escapeHtml(node.name)}</h3><p class="ownership-focus-code">${escapeHtml(node.ticker)}</p><div class="ownership-metrics"><div class="ownership-metric-label">시가총액${node.currency === 'USD' ? ' · USD' : ''}</div><div class="ownership-metric-value">${escapeHtml(money(node.marketCap, node.currency))}</div><div class="ownership-metric-meta">${escapeHtml(capTime)}</div></div>${renderExposure()}<div class="ownership-inspector-actions">${node.pairIds.length ? '<button type="button" class="ownership-detail-link" data-ownership-dashboard>대시보드 보기 ↗</button>' : ''}${node.id !== baseId ? '<button type="button" class="ownership-detail-link" data-ownership-rebase>이 종목을 기준으로</button>' : ''}</div>${baseId === node.id && indirect.length ? `<section class="ownership-descendants"><h3>간접 보유 종목 <span>${indirect.length}</span></h3>${indirectList(indirect.slice(0, 4))}${indirect.length > 4 ? `<details class="ownership-connections" data-ownership-detail="more"><summary>나머지 ${indirect.length - 4}종목</summary>${indirectList(indirect.slice(4))}</details>` : ''}</section>` : ''}${outgoing.length ? `<details class="ownership-connections" data-ownership-detail="outgoing"${baseId === node.id ? ' open' : ''}><summary>보유한 종목 ${outgoing.length}</summary>${relations(outgoing, false)}</details>` : ''}${incoming.length ? `<details class="ownership-connections" data-ownership-detail="incoming"${outgoing.length ? '' : ' open'}><summary>보유한 회사 ${incoming.length}</summary>${relations(incoming, true)}</details>` : ''}<details class="ownership-data-details" data-ownership-detail="sources"><summary>시총·지분율 기준</summary><p>해당 종류주식의 주식수 × 주가로 계산합니다. ${node.currency === 'USD' ? '미국 주식수는 자사주를 제외한 outstanding 기준입니다.' : '국내 주식수는 자사주를 포함한 상장주식수로, 대시보드의 조정시총과 다릅니다.'}</p><p>${node.currency === 'USD' ? 'Outstanding' : '상장주식수'} ${number(node.totalShares) === null ? '미확인' : number(node.totalShares) + '주'} · ${escapeHtml(sourceName(node.sharesSource))} · ${escapeHtml(shareTime)}</p><p>주가 ${number(node.price) || '미확인'} ${node.currency === 'USD' ? 'USD' : '원'} · ${escapeHtml(sourceName(node.priceSource))}</p><p>지분율은 등록 보유수량 ÷ 대상 주식수를 우선 사용하며, 계산할 수 없는 경우 공시 지분율을 참고합니다. 수량과 공시 시점이 달라질 수 있습니다.</p>${sources}</details>`;
  }

  function refreshOwnershipGraph() {
    if (!graph || !graphConfigs) return;
    rebuildGraph();
    if (!dialog || !dialog.open || !component) return;
    const active = document.activeElement;
    const activeStock = active && active.dataset.ownershipNode;
    const targetHadFocus = active && active.id === 'ownershipTargetSelect';
    const inspector = el('ownershipInspector');
    const activeDetail = active && active.tagName === 'SUMMARY' ? active.parentElement.dataset.ownershipDetail : '';
    const activeAction = active && inspector.contains(active) ? ['data-ownership-dashboard', 'data-ownership-rebase'].find(attr => active.hasAttribute(attr)) : null;
    const inspectorRelation = activeStock && inspector.contains(active);
    const detailStates = Array.from(inspector.querySelectorAll('details[data-ownership-detail]')).map(detail => [detail.dataset.ownershipDetail, detail.open]);
    component = getOwnershipComponent(graph, baseId);
    layout = layoutOwnershipGraph(component, { nodeHeight: 100 });
    renderGraph();
    setScale(scale);
    selectNode(focusId, false);
    detailStates.forEach(function(entry) {
      const detail = inspector.querySelector('[data-ownership-detail="' + entry[0] + '"]');
      if (detail) detail.open = entry[1];
    });
    if (targetHadFocus) el('ownershipTargetSelect').focus({ preventScroll: true });
    else if (activeDetail) {
      const summary = inspector.querySelector('[data-ownership-detail="' + activeDetail + '"] > summary');
      if (summary) summary.focus({ preventScroll: true });
    } else if (activeAction) {
      const button = inspector.querySelector('[' + activeAction + ']');
      if (button) button.focus({ preventScroll: true });
    }
    else if (activeStock) {
      const root = inspectorRelation ? inspector : el('ownershipStage');
      const button = Array.from(root.querySelectorAll('[data-ownership-node]')).find(item => item.dataset.ownershipNode === activeStock);
      if (button) button.focus({ preventScroll: true });
    }
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

  function fitGraph(full) {
    if (!dialog.open || !layout) return;
    const viewport = el('ownershipViewport');
    // 최초에는 수치를 읽을 수 있는 크기를 유지하고, 전체 맞춤에서만 더 축소한다.
    const minimum = full ? 0.35 : 0.85;
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

  return { bindOwnershipGraph, openOwnershipGraph, refreshOwnershipGraph };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createOwnershipGraphView };
}
