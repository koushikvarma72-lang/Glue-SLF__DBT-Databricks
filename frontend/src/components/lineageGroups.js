/**
 * Collapsible-groups lineage view (sfglue Step 2).
 *
 * Replaces the dense medallion-lanes SVG porthole with a scannable layout: one
 * column per medallion layer present (SOURCE → BRONZE → SILVER → GOLD, preserving
 * left→right flow), each containing **collapsible groups** keyed by schema (Snowflake)
 * / database (Glue) / bucket (source). Collapsed groups show a count; expand to list
 * tables. Glue ETL jobs (which bridge layers, so they don't belong in one column) get
 * their own collapsible section below, each clickable to view its script.
 *
 * Big accounts stay readable: groups auto-collapse past a threshold, and the column
 * grid wraps instead of overflowing horizontally.
 */
const LAYER_ORDER = ['source', 'bronze', 'silver', 'gold'];
const LAYER_TITLE = { source: 'SOURCE', bronze: 'BRONZE', silver: 'SILVER', gold: 'GOLD' };
const LAYER_COLOR = { source: '#2563eb', bronze: '#8b5cf6', silver: '#0f766e', gold: '#ca8a04' };

const esc = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const layerOf = n => (LAYER_ORDER.includes(n.type) ? n.type : 'silver');
const byLabel = (a, b) => String(a.display || a.label).localeCompare(String(b.display || b.label));

/** The collapsible group a node belongs to — its schema / database / bucket. */
export function groupKeyFor(node) {
  const label = String(node.label || '');
  if (node.type === 'source') {
    const m = label.match(/^s3:\/\/([^/]+)/i);
    if (m) return `s3://${m[1]}`;
    const segs = label.split('.').filter(Boolean);
    return segs.length >= 2 ? segs.slice(0, -1).join('.') : 'External';
  }
  const parts = label.split('.').filter(Boolean);
  if (node.system === 'snowflake') {
    if (parts.length >= 3) return parts[parts.length - 2];   // db.schema.table → schema
    if (parts.length === 2) return parts[0];                 // db.table → db
    return '(ungrouped)';
  }
  // Glue: database.table → database
  return parts.length >= 2 ? parts[0] : '(ungrouped)';
}

/** Pure model: layers (present only) → sorted groups → nodes, plus the job list. */
export function groupLineage(lineage) {
  const nodes = (lineage && lineage.nodes) || [];
  const dataNodes = nodes.filter(n => n.type !== 'job');
  const jobs = nodes.filter(n => n.type === 'job').slice().sort(byLabel);

  const layers = LAYER_ORDER.map(layer => {
    const items = dataNodes.filter(n => layerOf(n) === layer);
    if (!items.length) return null;
    const groups = new Map();
    items.forEach(n => {
      const k = groupKeyFor(n);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(n);
    });
    const groupList = [...groups.entries()]
      .map(([name, ns]) => ({ name, count: ns.length, nodes: ns.slice().sort(byLabel) }))
      .sort((a, b) => a.name.localeCompare(b.name));
    return { layer, title: LAYER_TITLE[layer], color: LAYER_COLOR[layer], count: items.length, groups: groupList };
  }).filter(Boolean);

  return { layers, jobs, total: dataNodes.length };
}

const STYLE = `
<style>
  .lin-wrap { font-size:13px; }
  .lin-cols { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:14px; }
  .lin-col { border:1px solid var(--border); border-radius:10px; background:var(--bg-primary); padding:0 0 6px; overflow:hidden; }
  .lin-col-head { font-size:11px; font-weight:700; letter-spacing:1px; padding:8px 12px; border-bottom:1px solid var(--border); }
  .lin-col-head .c { color:var(--text-muted); font-weight:600; letter-spacing:0; }
  .lin-group { border-bottom:1px solid var(--border); }
  .lin-group:last-child { border-bottom:0; }
  .lin-ghead { width:100%; display:flex; align-items:center; gap:8px; background:none; border:0; cursor:pointer;
               padding:7px 12px; text-align:left; color:var(--text-primary,#0f172a); font-size:12px; }
  .lin-ghead:hover { background:var(--bg-secondary); }
  .lin-caret { width:10px; color:var(--text-muted); flex:none; }
  .lin-gname { flex:1; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .lin-gcount { font-size:11px; color:var(--text-muted); background:var(--bg-secondary); border-radius:10px; padding:1px 7px; }
  .lin-gbody { padding:2px 0 6px; }
  .lin-item { display:flex; align-items:center; gap:7px; padding:3px 12px 3px 26px; font-size:12px; }
  .lin-dot { width:7px; height:7px; border-radius:50%; flex:none; }
  .lin-item code { flex:1; font-family:monospace; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .lin-item .n { color:var(--text-muted); font-size:11px; }
  .lin-jobs { margin-top:14px; border:1px solid var(--border); border-radius:10px; background:var(--bg-primary); overflow:hidden; }
  .lin-jobgrid { display:flex; flex-wrap:wrap; gap:6px; padding:8px 12px; }
  .lin-job { display:inline-flex; align-items:center; gap:6px; cursor:pointer; font-size:12px;
             background:var(--bg-secondary); border:1px solid var(--border); border-radius:8px; padding:4px 9px; color:var(--text-primary,#0f172a); }
  .lin-job:hover { border-color:#ea580c; }
  .lin-job code { font-family:monospace; }
  .lin-jtype { color:var(--text-muted); font-size:11px; }
  .lin-empty { padding:14px; color:var(--text-muted); font-size:13px; }
</style>`;

function groupHtml(layer, g, color, expanded) {
  const items = g.nodes.map(n =>
    `<div class="lin-item" title="${esc(n.label)}${n.column_count != null ? ` (${n.column_count} cols)` : ''}">
       <span class="lin-dot" style="background:${color}"></span>
       <code>${esc(n.display || n.label)}</code>
       ${n.column_count != null ? `<span class="n">${n.column_count}</span>` : ''}
     </div>`).join('');
  return `<div class="lin-group">
    <button type="button" class="lin-ghead" data-gid="${esc(layer)}::${esc(g.name)}" aria-expanded="${expanded}">
      <span class="lin-caret" aria-hidden="true">${expanded ? '▾' : '▸'}</span>
      <span class="lin-gname">${esc(g.name)}</span>
      <span class="lin-gcount">${g.count}</span>
    </button>
    <div class="lin-gbody" style="display:${expanded ? 'block' : 'none'}">${items}</div>
  </div>`;
}

export function renderLineageGroups(container, lineage, opts = {}) {
  if (!container) return;
  const model = groupLineage(lineage);
  if (!model.layers.length && !model.jobs.length) {
    container.innerHTML = `${STYLE}<div class="lin-empty">Nothing to display.</div>`;
    return;
  }

  // Auto-collapse for big accounts; keep small ones open so nothing's hidden by default.
  const big = model.total > 40;
  const expandedByDefault = g => !big && g.count <= 8;

  const cols = model.layers.map(L => `
    <div class="lin-col" style="border-top:3px solid ${L.color}">
      <div class="lin-col-head" style="color:${L.color}">${L.title} <span class="c">(${L.count})</span></div>
      ${L.groups.map(g => groupHtml(L.layer, g, L.color, expandedByDefault(g))).join('')}
    </div>`).join('');

  const jobsExpanded = !big && model.jobs.length <= 12;
  const jobsHtml = model.jobs.length ? `
    <div class="lin-jobs">
      <button type="button" class="lin-ghead" data-gid="__jobs__" aria-expanded="${jobsExpanded}">
        <span class="lin-caret" aria-hidden="true">${jobsExpanded ? '▾' : '▸'}</span>
        <span class="lin-gname"><span aria-hidden="true">⚙</span> Glue ETL jobs</span>
        <span class="lin-gcount">${model.jobs.length}</span>
      </button>
      <div class="lin-gbody lin-jobgrid" style="display:${jobsExpanded ? 'flex' : 'none'}">
        ${model.jobs.map(j => `<button type="button" class="lin-job" data-job="${esc(j.label)}" aria-label="View script for ${esc(j.display || j.label)}">
            <span aria-hidden="true">⚙</span> <code>${esc(j.display || j.label)}</code>${j.job_type ? `<span class="lin-jtype">${esc(j.job_type)}</span>` : ''}
          </button>`).join('')}
      </div>
    </div>` : '';

  container.innerHTML = `${STYLE}<div class="lin-wrap"><div class="lin-cols">${cols}</div>${jobsHtml}</div>`;

  container.querySelectorAll('.lin-ghead').forEach(btn => {
    btn.addEventListener('click', () => {
      const body = btn.nextElementSibling;
      if (!body) return;
      const open = body.style.display !== 'none';
      body.style.display = open ? 'none' : (body.classList.contains('lin-jobgrid') ? 'flex' : 'block');
      btn.setAttribute('aria-expanded', String(!open));
      const caret = btn.querySelector('.lin-caret');
      if (caret) caret.textContent = open ? '▸' : '▾';
    });
  });
  container.querySelectorAll('.lin-job').forEach(btn => {
    btn.addEventListener('click', () => opts.onJobClick && opts.onJobClick(btn.dataset.job));
  });
}

/** Expand or collapse every group inside a rendered lineage-groups container. */
export function setAllGroups(container, expand) {
  if (!container) return;
  container.querySelectorAll('.lin-ghead').forEach(btn => {
    const body = btn.nextElementSibling;
    if (!body) return;
    body.style.display = expand ? (body.classList.contains('lin-jobgrid') ? 'flex' : 'block') : 'none';
    btn.setAttribute('aria-expanded', String(expand));
    const caret = btn.querySelector('.lin-caret');
    if (caret) caret.textContent = expand ? '▾' : '▸';
  });
}
