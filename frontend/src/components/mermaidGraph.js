/**
 * Mermaid-based renderer for the Qlik data-model graph on the Upload page.
 *
 * The Upload page models the QVF as {nodes: tables, edges: associations}. This
 * renders that as a Mermaid flowchart (one box per table, association links
 * labelled by the join field), as an alternative to the draggable D3 force view.
 * Tables are coloured by role (fact vs dimension) so the star shape is obvious.
 */
import mermaid from 'mermaid';
import { enableSvgPanZoom, resetSvgPanZoom } from './svgPanZoom.js';

let _initialized = false;
let _renderSeq = 0;

function ensureInit() {
  if (_initialized) return;
  mermaid.initialize({
    startOnLoad: false,
    theme: 'neutral',
    securityLevel: 'loose',
    // htmlLabels:false → SVG <text> labels so PNG export rasterizes correctly.
    flowchart: { htmlLabels: false, curve: 'basis', nodeSpacing: 40, rankSpacing: 80, padding: 10 },
  });
  _initialized = true;
}

const esc = s => String(s == null ? '' : s).replace(/"/g, '&quot;').replace(/[\r\n]+/g, ' ');
const sid = id => 'n' + String(id).replace(/[^a-zA-Z0-9_]/g, '_');

/** Build a Mermaid `graph LR` definition from a {nodes, edges} data model. */
export function buildMermaidGraph(graph) {
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  const ids = new Set(nodes.map(n => n.id));

  const lines = ['graph LR'];

  nodes.forEach(n => {
    const meta = [];
    const cols = (n.fields || []).length;
    if (cols) meta.push(`${cols} cols`);
    if (n.rows) meta.push(`${Number(n.rows).toLocaleString()} rows`);
    let label = esc(n.name || n.id);
    if (meta.length) label += `<br/>${meta.join(' · ')}`;
    lines.push(`  ${sid(n.id)}["${label}"]`);
  });

  // Associations are undirected joins; label with the shared field when known.
  const seen = new Set();
  edges.forEach(e => {
    const f = e.source, t = e.target;
    if (f == null || t == null || !ids.has(f) || !ids.has(t) || f === t) return;
    const key = f < t ? `${f}|${t}` : `${t}|${f}`;
    if (seen.has(key)) return;
    seen.add(key);
    const field = esc(String(e.sourceField || e.field || '').replace(/%/g, ''));
    const lbl = field ? `|"${field}"|` : '';
    lines.push(`  ${sid(f)} ---${lbl} ${sid(t)}`);
  });

  const facts = nodes.filter(n => n.type === 'fact');
  const dims = nodes.filter(n => n.type && n.type !== 'fact');
  const rest = nodes.filter(n => !n.type);
  if (facts.length) {
    lines.push('  classDef cls_fact fill:#2f7d5b22,stroke:#2f7d5b,stroke-width:1.5px,color:#0f172a;');
    lines.push(`  class ${facts.map(n => sid(n.id)).join(',')} cls_fact;`);
  }
  if (dims.length) {
    lines.push('  classDef cls_dim fill:#4f8fbf22,stroke:#4f8fbf,stroke-width:1px,color:#0f172a;');
    lines.push(`  class ${dims.map(n => sid(n.id)).join(',')} cls_dim;`);
  }
  if (rest.length) {
    lines.push('  classDef cls_other fill:#94a3b822,stroke:#64748b,stroke-width:1px,color:#0f172a;');
    lines.push(`  class ${rest.map(n => sid(n.id)).join(',')} cls_other;`);
  }

  return lines.join('\n');
}

/** Render the data-model graph into ``container`` with Mermaid (natural size, scrollable). */
export async function renderMermaidGraph(container, graph) {
  if (!container) return;
  ensureInit();
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  if (!nodes.length) {
    container.innerHTML = '<div style="padding:16px;color:#64748b;font-size:13px">No tables to diagram.</div>';
    return;
  }
  // Safety cap: Mermaid's dagre layout on thousands of nodes/edges builds a giant
  // SVG that can freeze the browser (and the machine). Above these limits, show a
  // note instead — the list/table views cover exploration of large models.
  const MAX_NODES = 400, MAX_EDGES = 900;
  if (nodes.length > MAX_NODES || edges.length > MAX_EDGES) {
    container.innerHTML = `<div style="padding:24px;text-align:center;color:var(--text-muted);font-size:13px;line-height:1.6">
      <div style="font-size:15px;font-weight:600;color:var(--text-secondary);margin-bottom:4px">Model too large to diagram</div>
      ${nodes.length.toLocaleString()} tables · ${edges.length.toLocaleString()} links — use the list views to explore.</div>`;
    return;
  }
  const def = buildMermaidGraph(graph);
  container.innerHTML = '<div style="padding:14px;color:#64748b;font-size:13px">Rendering diagram…</div>';
  try {
    const { svg } = await mermaid.render(`mgraph_${++_renderSeq}`, def);
    // overflow:hidden — pan/zoom (drag + scroll) replaces native scrollbars.
    container.innerHTML = `<div class="mermaid-scroll" style="width:100%;height:100%;overflow:hidden">${svg}</div>`;
    // Fit the diagram to the box, then enable drag-to-pan / scroll-to-zoom.
    enableSvgPanZoom(container);
  } catch (err) {
    container.innerHTML = `<div style="padding:14px;color:var(--danger,#dc2626);font-size:12px">Diagram render failed: ${esc(err && err.message)}</div>`;
  }
  return def;
}

/** Reset pan/zoom so the whole diagram fits the box again. */
export function toggleMermaidGraphFit(container) {
  resetSvgPanZoom(container);
}
