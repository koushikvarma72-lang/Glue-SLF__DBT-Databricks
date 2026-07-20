/**
 * Operational-lineage header strip for the Flow view: the RDS control-plane summary
 * plus the Glue job execution chain (true trigger order). Also merges the config-derived
 * data edges into the script-derived lineage graph so Flow connects silver→gold even
 * when job scripts are generic engines.
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

