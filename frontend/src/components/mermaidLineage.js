/**
 * Mermaid-based lineage renderer for the Snowflake/Glue flow.
 *
 * Renders the {nodes, edges} lineage as a left→right Mermaid flowchart with one
 * subgraph per medallion layer (SOURCE → BRONZE → SILVER → GOLD), so the flow from
 * source to destination reads as a clean hierarchy.
 *
 * Crucially it MERGES the same table across systems: when the same logical table
 * exists in both Snowflake and Glue, the two copies collapse into a single node
 * (preferring the Snowflake "source of truth"), with a "merged" badge — so each table
 * appears once, not twice. The per-copy detail still lives in the "Tables in both
 * systems" panel.
 */
import mermaid from 'mermaid';
import { enableSvgPanZoom, resetSvgPanZoom } from './svgPanZoom.js';
import { notify } from './notify.js';

let _initialized = false;
let _renderSeq = 0;

function ensureInit() {
  if (_initialized) return;
  mermaid.initialize({
    startOnLoad: false,
    theme: 'neutral',
    securityLevel: 'loose',
    // htmlLabels:false → labels are SVG <text> (not foreignObject), so the diagram
    // rasterizes correctly when exported to PNG.
    flowchart: { htmlLabels: false, curve: 'basis', nodeSpacing: 28, rankSpacing: 70, padding: 8 },
  });
  _initialized = true;
}

const LAYER_ORDER = ['source', 'bronze', 'silver', 'gold'];
const LAYER_TITLE = { source: 'SOURCE', bronze: 'BRONZE', silver: 'SILVER', gold: 'GOLD' };
const LAYER_COLOR = { source: '#2563eb', bronze: '#8b5cf6', silver: '#0f766e', gold: '#ca8a04' };

const escMer = s => String(s == null ? '' : s).replace(/"/g, '&quot;').replace(/[\r\n]+/g, ' ');
const sid = id => 'n' + String(id).replace(/[^a-zA-Z0-9_]/g, '_');
// Reconstruct a node id from a duplicate-group member (matches the backend's
// `sf:`/`glue:` + normalized full_name scheme).
const idForMember = m => (m.system === 'snowflake' ? 'sf:' : 'glue:') + String(m.full_name || '').toLowerCase();

/** Build a Mermaid `graph LR` definition from the lineage, merging duplicates. */
export function buildMermaidLineage(lineage, { duplicates = [], prefer = 'snowflake' } = {}) {
  const nodes = (lineage && lineage.nodes) || [];
  const edges = (lineage && lineage.edges) || [];
  const byId = new Map(nodes.map(n => [n.id, n]));

  // Map every duplicate member → one canonical node id (prefer the Snowflake copy).
  const canonical = new Map();
  const mergeMeta = new Map(); // canonicalId -> { systems: Set }
  (duplicates || []).forEach(g => {
    const memberIds = (g.members || []).map(idForMember).filter(id => byId.has(id));
    if (memberIds.length < 2) return;
    const canon = memberIds.find(id => byId.get(id).system === prefer) || memberIds[0];
    memberIds.forEach(id => canonical.set(id, canon));
    mergeMeta.set(canon, { systems: new Set(memberIds.map(id => byId.get(id).system)) });
  });
  const canonOf = id => canonical.get(id) || id;

  // Deduped node set (keyed by canonical id).
  const keep = new Map();
  nodes.forEach(n => {
    const c = canonOf(n.id);
    if (!keep.has(c)) keep.set(c, byId.get(c) || n);
  });

  // Deduped, remapped edges. Self-loops (collapsed same-table links) and explicit
  // 'copy' links are dropped — the merge already conveys "same table in both systems".
  const seen = new Set();
  const outEdges = [];
  edges.forEach(e => {
    if (e.label === 'copy') return;
    const f = canonOf(e.from), t = canonOf(e.to);
    if (f === t) return;
    const key = `${f}>${t}>${e.label || ''}`;
    if (seen.has(key)) return;
    seen.add(key);
    outEdges.push({ from: f, to: t, label: e.label || '' });
  });

  // Bucket kept nodes: medallion layers go in subgraphs; Glue jobs render as
  // free, clickable hexagon nodes between their inputs and outputs.
  const byLayer = { source: [], bronze: [], silver: [], gold: [] };
  const jobNodes = [];
  keep.forEach((n, id) => {
    if (n.type === 'job') { jobNodes.push({ id, n }); return; }
    const layer = LAYER_ORDER.includes(n.type) ? n.type : 'silver';
    byLayer[layer].push({ id, n });
  });

  const lines = ['graph LR'];
  LAYER_ORDER.forEach(layer => {
    const items = byLayer[layer];
    if (!items.length) return;
    lines.push(`  subgraph sg_${layer}["${LAYER_TITLE[layer]}"]`);
    lines.push('    direction TB');
    items.sort((a, b) => String(a.n.display || a.n.label).localeCompare(String(b.n.display || b.n.label)));
    items.forEach(({ id, n }) => {
      let label = escMer(n.display || n.label || id);
      const meta = mergeMeta.get(id);
      if (meta) {
        const sys = [...meta.systems].map(s => (s === 'snowflake' ? 'SF' : 'Glue')).join(' + ');
        label += `<br/>⊕ ${sys}`;   // mermaid splits <br/> into multiline SVG text
      }
      lines.push(`    ${sid(id)}["${label}"]`);
    });
    lines.push('  end');
  });

  // Glue job nodes (hexagons).
  jobNodes.sort((a, b) => String(a.n.label).localeCompare(String(b.n.label)));
  jobNodes.forEach(({ id, n }) => {
    lines.push(`  ${sid(id)}{{"⚙ ${escMer(n.display || n.label || id)}"}}`);
  });

  outEdges.forEach(e => {
    const arrow = e.label === 'fk' ? '-.->' : '-->';
    const lbl = e.label ? `|"${escMer(e.label)}"|` : '';
    lines.push(`  ${sid(e.from)} ${arrow}${lbl} ${sid(e.to)}`);
  });

  LAYER_ORDER.forEach(layer => {
    const items = byLayer[layer];
    if (!items.length) return;
    const c = LAYER_COLOR[layer];
    lines.push(`  classDef cls_${layer} fill:${c}1f,stroke:${c},stroke-width:1px,color:#0f172a;`);
    lines.push(`  class ${items.map(({ id }) => sid(id)).join(',')} cls_${layer};`);
  });
  if (jobNodes.length) {
    lines.push('  classDef cls_job fill:#ea580c22,stroke:#ea580c,stroke-width:1.5px,color:#7c2d12;');
    lines.push(`  class ${jobNodes.map(({ id }) => sid(id)).join(',')} cls_job;`);
    // Click a job → show its details. __mlinClick is registered by renderMermaidLineage.
    jobNodes.forEach(({ id, n }) => {
      lines.push(`  click ${sid(id)} call __mlinClick("${escMer(n.label || id)}")`);
    });
  }

  return lines.join('\n');
}

/**
 * Render the lineage into ``container`` using Mermaid. Async (Mermaid renders to a
 * promise); fire-and-forget is fine — it fills the container when ready.
 */
export async function renderMermaidLineage(container, lineage, opts = {}) {
  if (!container) return;
  ensureInit();
  // Large-model guard (mirrors graph.js / mermaidGraph.js): dagre layout of a huge
  // graph runs multi-second on the main thread and can hang the tab. Skip it and
  // steer the user to the Groups view, which scales via auto-collapse.
  const nNodes = ((lineage && lineage.nodes) || []).length;
  const nEdges = ((lineage && lineage.edges) || []).length;
  const MAX_NODES = 400, MAX_EDGES = 900;
  if (nNodes > MAX_NODES || nEdges > MAX_EDGES) {
    container.innerHTML =
      `<div role="status" aria-live="polite" style="padding:24px;text-align:center;color:var(--text-secondary,#64748b);font-size:13px">` +
        `<div style="font-size:1.6rem;opacity:.5;margin-bottom:8px">🗺️</div>` +
        `This lineage is too large to render as a diagram (${nNodes.toLocaleString()} nodes / ${nEdges.toLocaleString()} edges).<br/>` +
        `Switch to the <strong>Groups</strong> view to explore it by medallion layer.` +
      `</div>`;
    return;
  }
  // Register the job-click callback Mermaid's `click … call __mlinClick()` invokes.
  if (typeof window !== 'undefined') {
    window.__mlinClick = (name) => { if (typeof opts.onJobClick === 'function') opts.onJobClick(name); };
  }
  const def = buildMermaidLineage(lineage, opts);
  // A newer render (myGen !== _renderSeq) or a view switch (opts.isCurrent → false)
  // while we await must NOT let this render's deferred write clobber what's now shown.
  const myGen = ++_renderSeq;
  const superseded = () => myGen !== _renderSeq || (typeof opts.isCurrent === 'function' && !opts.isCurrent());
  container.innerHTML = '<div role="status" aria-live="polite" style="padding:14px;color:#64748b;font-size:13px">Rendering diagram…</div>';
  try {
    const { svg, bindFunctions } = await mermaid.render(`mlin_${myGen}`, def);
    if (superseded()) return def;
    // overflow:hidden — pan/zoom (drag + scroll) replaces native scrollbars.
    container.innerHTML = `<div class="mermaid-scroll" style="width:100%;height:100%;overflow:hidden">${svg}</div>`;
    const scroll = container.querySelector('.mermaid-scroll');
    if (bindFunctions && scroll) bindFunctions(scroll);
    // Fit the diagram to the box, then enable drag-to-pan / scroll-to-zoom.
    enableSvgPanZoom(container);
  } catch (err) {
    // Mermaid parse errors are developer-oriented; show a recoverable message and
    // tuck the raw text behind a <details> / the console instead of dead-ending.
    console.error('Mermaid lineage render failed:', err);
    if (superseded()) return def;
    container.innerHTML =
      '<div role="status" aria-live="polite" style="padding:14px;font-size:13px;color:var(--text-primary,#111827)">' +
        '<p style="margin:0 0 8px">Could not render this diagram. Try the <strong>Groups</strong> view, or retry.</p>' +
        '<button type="button" data-mlin-retry style="cursor:pointer;font-size:12px;padding:5px 11px;border:1px solid var(--border,#e5e7eb);border-radius:6px;background:var(--bg-surface,#fff);color:var(--text-primary,#111827)">Retry diagram</button>' +
        `<details style="margin-top:10px"><summary style="cursor:pointer;color:var(--text-muted,#64748b);font-size:12px">Technical details</summary>` +
          `<pre style="white-space:pre-wrap;font-size:11px;color:var(--danger,#dc2626);margin:6px 0 0">${escMer(err && err.message)}</pre>` +
        '</details>' +
      '</div>';
    const retry = container.querySelector('[data-mlin-retry]');
    if (retry) retry.addEventListener('click', () => { renderMermaidLineage(container, lineage, opts); });
    notify('Could not render the lineage diagram. Try the Groups view.', { kind: 'error', title: 'Diagram failed' });
  }
  return def;
}

/** Reset pan/zoom so the whole diagram fits the box again. */
export function toggleMermaidFit(container) {
  resetSvgPanZoom(container);
}

function _triggerDownload(url, name) {
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

/**
 * Export the rendered graph SVG inside ``container`` as a downloadable image.
 * format 'png' (default) rasterizes to a white-background PNG at ``scale``× ; 'svg'
 * downloads the vector directly. Works for both the Mermaid and the D3 (Force) svg.
 */
export function downloadGraphImage(container, { filename = 'lineage', format = 'png', scale = 2 } = {}) {
  const svg = container && container.querySelector('svg');
  if (!svg) return;

  const vb = String(svg.getAttribute('viewBox') || '').split(/\s+/).map(Number);
  const box = svg.getBoundingClientRect();
  const w = Math.max(1, Math.ceil(vb[2] > 0 ? vb[2] : box.width || 1200));
  const h = Math.max(1, Math.ceil(vb[3] > 0 ? vb[3] : box.height || 800));

  const clone = svg.cloneNode(true);
  clone.setAttribute('width', String(w));
  clone.setAttribute('height', String(h));
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
  clone.style.maxWidth = 'none';
  clone.style.height = 'auto';
  // Drop any live pan/zoom transform so the export captures the full diagram,
  // not the currently zoomed-in viewport.
  const pzLayer = clone.querySelector('g.__pz_layer');
  if (pzLayer) pzLayer.removeAttribute('transform');
  const serialized = `<?xml version="1.0" encoding="UTF-8"?>\n${new XMLSerializer().serializeToString(clone)}`;
  const svgUrl = URL.createObjectURL(new Blob([serialized], { type: 'image/svg+xml;charset=utf-8' }));

  if (format === 'svg') {
    _triggerDownload(svgUrl, `${filename}.svg`);
    setTimeout(() => URL.revokeObjectURL(svgUrl), 1500);
    return;
  }

  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement('canvas');
    canvas.width = w * scale;
    canvas.height = h * scale;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.scale(scale, scale);
    ctx.drawImage(img, 0, 0, w, h);
    URL.revokeObjectURL(svgUrl);
    canvas.toBlob((blob) => {
      if (!blob) return;
      const pngUrl = URL.createObjectURL(blob);
      _triggerDownload(pngUrl, `${filename}.png`);
      setTimeout(() => URL.revokeObjectURL(pngUrl), 1500);
    }, 'image/png');
  };
  img.onerror = () => URL.revokeObjectURL(svgUrl);
  img.src = svgUrl;
}
