/**
 * Operational-lineage renderer — the fused view: RDS control plane on top, the Glue
 * job execution chain, medallion data columns (source → bronze → silver → gold →
 * Snowflake), and a source-health panel. Clicking a job highlights its edges and opens
 * a drawer with reads/writes, control tables, extracted config SQL, and review flags.
 *
 * Pure DOM, no deps. Everything is data-driven from build_operational_lineage() output;
 * nothing about any specific pipeline is assumed.
 */

const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

const baseName = (s) => String(s || '').split('.').pop().toLowerCase()
  .replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');

/** Topological order (Kahn) of job ids over execution edges. */
function topoJobs(jobs, edges) {
  const exec = edges.filter((e) => e.kind === 'execution');
  const ids = jobs.map((j) => j.id);
  const set = new Set(ids);
  const indeg = {}; const adj = {};
  ids.forEach((id) => { indeg[id] = 0; adj[id] = []; });
  exec.forEach((e) => { if (set.has(e.from) && set.has(e.to)) { adj[e.from].push(e.to); indeg[e.to]++; } });
  const order = []; const ready = ids.filter((id) => indeg[id] === 0);
  while (ready.length) {
    const id = ready.shift(); order.push(id);
    (adj[id] || []).forEach((n) => { if (--indeg[n] === 0) ready.push(n); });
  }
  ids.forEach((id) => { if (!order.includes(id)) order.push(id); });
  const byId = Object.fromEntries(jobs.map((j) => [j.id, j]));
  return order.map((id) => byId[id]).filter(Boolean);
}

/**
 * Compact header for the FLOW view: the RDS control-plane strip + the Glue job
 * execution chain (true trigger order). Job chips call opts.onJobClick(label).
 */
export function renderOpsHeader(el, graph, opts = {}) {
  const g = graph || {};
  const control = (g.nodes || []).filter((n) => n.type === 'control');
  const chain = topoJobs(g.jobs || [], g.edges || []);
  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px">
      <div style="border:1px dashed var(--border,#cbd5e1);border-radius:8px;padding:8px 12px;background:var(--bg-inset,#f8fafc)">
        <span style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#475569;text-transform:uppercase;margin-right:8px">Control plane · RDS (${control.length})</span>
        ${control.map((c) => `<span style="font-size:11px;border:1px solid #cbd5e1;border-radius:12px;padding:2px 9px;background:#fff;margin-right:4px;display:inline-block;margin-top:3px">${esc(c.label)}${c.rows ? `<span style="color:#94a3b8"> ·${c.rows}</span>` : ''}</span>`).join('') || '<span style="font-size:11px;color:#94a3b8">none detected</span>'}
      </div>
      <div style="border:1px solid var(--border,#e2e8f0);border-radius:8px;padding:8px 12px;background:var(--bg-surface,#fff)">
        <span style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#334155;text-transform:uppercase;margin-right:8px">Glue job execution chain (${chain.length})</span>
        ${chain.map((j, i) => `${i ? '<span style="color:#94a3b8;margin:0 3px">→</span>' : ''}<button class="opsh-job" data-label="${esc(j.label)}" style="font-size:11px;font-weight:600;border:1px solid #0e7490;color:#0e7490;background:#fff;border-radius:6px;padding:3px 9px;cursor:pointer;margin-top:3px">${esc(j.label)}${(j.flags || []).length ? ` <span style="color:#dc2626">⚑${j.flags.length}</span>` : ''}</button>`).join('') || '<span style="font-size:11px;color:#94a3b8">no jobs</span>'}
      </div>
    </div>`;
  el.querySelectorAll('.opsh-job').forEach((b) => b.addEventListener('click', () =>
    opts.onJobClick && opts.onJobClick(b.getAttribute('data-label'))));
}

/**
 * Merge the config-derived (ops) data edges into a script-derived lineage graph, so
 * the Flow view connects silver→gold through the jobs even when the job scripts are
 * generic engines. Evidence-based: endpoints resolve by base name; missing endpoints
 * are added using the ops node's own type/label (e.g. raw_* → bronze). Pure.
 */
export function augmentLineageWithOps(lineage, ops) {
  if (!lineage || !ops) return lineage;
  const nodes = (lineage.nodes || []).map((n) => ({ ...n }));
  const edges = (lineage.edges || []).map((e) => ({ ...e }));
  const byBase = {};
  nodes.forEach((n) => { (byBase[baseName(n.label)] ||= []).push(n.id); });
  const opsById = Object.fromEntries((ops.nodes || []).map((n) => [n.id, n]));

  const ensure = (opsNode) => {
    const b = baseName(opsNode.label);
    const cands = byBase[b] || [];
    // prefer a same-role node: jobs match jobs, tables match tables
    const wantJob = opsNode.type === 'job';
    for (const id of cands) {
      const n = nodes.find((x) => x.id === id);
      if (n && ((n.type === 'job') === wantJob)) return id;
    }
    const id = `${wantJob ? 'job' : 'ops'}:${b}`;
    if (!nodes.some((n) => n.id === id)) {
      nodes.push({ id, label: opsNode.label, display: opsNode.label, type: opsNode.type,
                   system: opsNode.system || 'config', inferred: !!opsNode.inferred });
      (byBase[b] ||= []).push(id);
    }
    return id;
  };

  (ops.edges || []).forEach((e) => {
    if (e.kind !== 'data' && e.kind !== 'replicate') return;
    const f = opsById[e.from]; const t = opsById[e.to];
    if (!f || !t || f.type === 'control' || t.type === 'control') return;
    const fid = ensure(f); const tid = ensure(t);
    if (fid !== tid) edges.push({ from: fid, to: tid, label: e.kind === 'replicate' ? 'load' : '' });
  });
  const uniq = {};
  edges.forEach((e) => { uniq[`${e.from}|${e.to}|${e.label}`] = e; });
  return { ...lineage, nodes, edges: Object.values(uniq) };
}

const LAYER = {
  source: { title: 'Source', color: '#2563eb' },
  bronze: { title: 'Bronze · raw/landing', color: '#7c3aed' },
  silver: { title: 'Silver · curated', color: '#0e7490' },
  gold: { title: 'Gold · marts/publish', color: '#b45309' },
};

export function renderLineageOps(el, graph, opts = {}) {
  const g = graph || {};
  const nodes = g.nodes || [];
  const edges = g.edges || [];
  const jobs = g.jobs || [];
  const health = g.health || [];
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));

  const dataNodes = nodes.filter((n) => ['source', 'bronze', 'silver', 'gold'].includes(n.type));
  const control = nodes.filter((n) => n.type === 'control');
  const sfNodes = nodes.filter((n) => n.system === 'snowflake');

  // execution order: real topological sort over the execution edges.
  const chain = topoJobs(jobs, edges);

  const layerCol = (layer) => {
    const items = dataNodes.filter((n) => n.type === layer && n.system !== 'snowflake');
    return `
      <div class="ops-col" data-layer="${layer}" style="flex:1;min-width:150px">
        <div style="font-size:10px;font-weight:700;letter-spacing:.5px;color:${LAYER[layer].color};text-transform:uppercase;margin-bottom:6px">${LAYER[layer].title} · ${items.length}</div>
        ${items.map((n) => `<div class="ops-tbl" data-id="${esc(n.id)}" style="border:1px solid var(--border,#e2e8f0);border-left:3px solid ${LAYER[layer].color};border-radius:6px;padding:5px 8px;margin-bottom:5px;font-size:11px;background:var(--bg-surface,#fff)">${esc(n.label)}${n.columns ? `<span style="color:var(--text-muted,#94a3b8)"> · ${n.columns} cols</span>` : ''}${n.inferred ? '<span style="color:#b45309" title="referenced in config, inferred"> ⚙</span>' : ''}</div>`).join('') || '<div style="font-size:11px;color:var(--text-muted,#94a3b8)">—</div>'}
      </div>`;
  };

  const healthColor = { warn: '#dc2626', info: '#64748b' };

  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:14px;font-family:inherit">
      <!-- control plane -->
      <div style="border:1px dashed var(--border,#cbd5e1);border-radius:8px;padding:10px 12px;background:var(--bg-inset,#f8fafc)">
        <div style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#475569;text-transform:uppercase;margin-bottom:6px">Control plane · RDS (${control.length})</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px">
          ${control.map((c) => `<span class="ops-ctl" data-id="${esc(c.id)}" style="font-size:11px;border:1px solid #cbd5e1;border-radius:12px;padding:3px 10px;background:#fff">${esc(c.canonical || c.label)}${c.rows ? `<span style="color:#94a3b8"> ·${c.rows}</span>` : ''}</span>`).join('') || '<span style="font-size:11px;color:#94a3b8">no control tables detected</span>'}
        </div>
      </div>

      <!-- execution chain -->
      <div style="border:1px solid var(--border,#e2e8f0);border-radius:8px;padding:10px 12px">
        <div style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#334155;text-transform:uppercase;margin-bottom:8px">Glue job execution chain (${jobs.length})</div>
        <div style="display:flex;flex-wrap:wrap;align-items:center;gap:4px">
          ${chain.map((j, i) => `${i ? '<span style="color:#94a3b8">→</span>' : ''}<button class="ops-job" data-id="${esc(j.id)}" style="font-size:11px;font-weight:600;border:1px solid #0e7490;color:#0e7490;background:#fff;border-radius:6px;padding:5px 10px;cursor:pointer">${esc(j.label)}${(j.flags || []).length ? ` <span style="color:#dc2626" title="${(j.flags || []).length} review flag(s)">⚑${j.flags.length}</span>` : ''}</button>`).join('') || '<span style="font-size:11px;color:#94a3b8">no jobs</span>'}
        </div>
      </div>

      <!-- medallion data columns -->
      <div style="display:flex;gap:10px;align-items:flex-start">
        ${layerCol('source')}${layerCol('bronze')}${layerCol('silver')}${layerCol('gold')}
        <div class="ops-col" style="flex:1;min-width:150px">
          <div style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#0369a1;text-transform:uppercase;margin-bottom:6px">Snowflake · publish · ${sfNodes.length}</div>
          ${sfNodes.map((n) => `<div class="ops-tbl" data-id="${esc(n.id)}" style="border:1px solid #bae6fd;border-radius:6px;padding:5px 8px;margin-bottom:5px;font-size:11px;background:#f0f9ff">${esc(n.label)}</div>`).join('') || '<div style="font-size:11px;color:#94a3b8">—</div>'}
        </div>
      </div>

      <!-- source health -->
      ${health.length ? `
      <div style="border:1px solid var(--border,#e2e8f0);border-radius:8px;padding:10px 12px">
        <div style="font-size:10px;font-weight:700;letter-spacing:.5px;color:#334155;text-transform:uppercase;margin-bottom:6px">Source health · ${health.length} finding(s)</div>
        <div style="display:flex;flex-direction:column;gap:3px;max-height:150px;overflow:auto">
          ${health.map((h) => `<div style="font-size:11px;color:${healthColor[h.severity] || '#64748b'}">• ${esc(h.detail)}</div>`).join('')}
        </div>
      </div>` : ''}

      <div id="ops-drawer"></div>
    </div>`;

  // interactions: click a job → drawer + highlight its data neighbours
  const clearHi = () => el.querySelectorAll('.ops-tbl,.ops-ctl').forEach((n) => { n.style.outline = ''; });
  const highlight = (ids, color) => ids.forEach((id) => {
    const n = el.querySelector(`[data-id="${CSS.escape(id)}"]`);
    if (n) n.style.outline = `2px solid ${color}`;
  });

  el.querySelectorAll('.ops-job').forEach((btn) => btn.addEventListener('click', () => {
    const jid = btn.getAttribute('data-id');
    const job = jobs.find((j) => j.id === jid) || {};
    clearHi();
    const reads = edges.filter((e) => e.kind === 'data' && e.to === jid).map((e) => e.from);
    const writes = edges.filter((e) => e.kind === 'data' && e.from === jid).map((e) => e.to);
    const ctls = edges.filter((e) => e.kind === 'control' && (e.from === jid || e.to === jid))
      .map((e) => (e.from === jid ? e.to : e.from));
    highlight(reads, '#0e7490'); highlight(writes, '#b45309'); highlight(ctls, '#64748b');
    if (opts.onJobClick) opts.onJobClick(job.label);

    const drawer = el.querySelector('#ops-drawer');
    drawer.innerHTML = `
      <div style="border:1px solid #0e7490;border-radius:8px;padding:12px 14px;background:var(--bg-surface,#fff)">
        <div style="font-size:14px;font-weight:700;color:#0e7490;margin-bottom:8px">${esc(job.label)}</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:12px">
          <div><strong>Reads</strong><br>${(job.reads || []).map(esc).join('<br>') || '<span style="color:#94a3b8">—</span>'}</div>
          <div><strong>Writes</strong><br>${(job.writes || []).map(esc).join('<br>') || '<span style="color:#94a3b8">—</span>'}</div>
          <div><strong>Control tables</strong><br>${(job.control_tables || []).map(esc).join('<br>') || '<span style="color:#94a3b8">—</span>'}</div>
          <div><strong>Review flags</strong><br>${(job.flags || []).length ? job.flags.map((f) => `<span style="color:#dc2626">⚑ ${esc(f)}</span>`).join('<br>') : '<span style="color:#94a3b8">none</span>'}</div>
        </div>
        ${(job.config_samples || []).length ? `
        <div style="margin-top:10px"><strong style="font-size:12px">Extracted logic (from RDS config)</strong>
          ${job.config_samples.map((s) => `<div style="margin-top:6px;font-size:11px"><span style="color:#64748b">${esc(s.source)} → ${esc(s.target)}</span><pre style="margin:2px 0 0;padding:6px 8px;background:var(--bg-inset,#f1f5f9);border-radius:5px;overflow:auto;font-size:10.5px">${esc(s.sql)}</pre></div>`).join('')}
        </div>` : ''}
      </div>`;
    drawer.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }));
}
