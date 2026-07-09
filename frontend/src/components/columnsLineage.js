/**
 * Medallion-lanes lineage renderer.
 *
 * Lays the dataflow out in fixed vertical columns by medallion layer
 * (SOURCE → BRONZE → SILVER → GOLD), with colored lane bands behind each column,
 * so the left→right flow of this exact pipeline shape reads at a glance. Glue jobs
 * float between the lanes they connect. A light barycenter sweep orders nodes within
 * each lane to reduce edge crossings.
 *
 * A lane with many tables (e.g. a Snowflake-heavy model where dozens of base tables
 * all land in SILVER) WRAPS sideways into multiple sub-columns instead of forming one
 * enormous single column — otherwise the diagram degenerates into an illegible
 * thousands-of-pixels-tall stripe.
 *
 * Emits a viewBox'd <svg>, then hands off to the shared enableSvgPanZoom so the
 * "Fit" button and PNG/SVG export work the same as the Mermaid diagram.
 */
import { select } from 'd3-selection';
import { enableSvgPanZoom } from './svgPanZoom.js';

const LAYER_ORDER = ['source', 'bronze', 'silver', 'gold'];
const LAYER_TITLE = { source: 'SOURCE', bronze: 'BRONZE', silver: 'SILVER', gold: 'GOLD' };
const LAYER_COLOR = { source: '#2563eb', bronze: '#8b5cf6', silver: '#0f766e', gold: '#ca8a04', job: '#ea580c' };

const NODE_W = 188;         // data-node box width
const ROW_H = 38;           // vertical pitch between stacked nodes
const NODE_H = 26;
const PAD_TOP = 54;         // room for lane titles
const PAD_X = 40;
const MAX_ROWS = 22;        // beyond this a lane wraps into extra sub-columns
const SUBCOL_W = NODE_W + 28; // horizontal pitch between sub-columns within a lane
const LANE_GAP = 74;        // horizontal gap between medallion lanes
// Pathological guard: past this the SVG is too big to be useful even wrapped.
const MAX_NODES = 2000, MAX_EDGES = 4000;

const trunc = (s, n = 26) => (s && s.length > n ? s.slice(0, n - 1) + '…' : (s || ''));
const layerOf = n => (LAYER_ORDER.includes(n.type) ? n.type : (n.type === 'job' ? 'job' : 'silver'));
const esc = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** Assign each node an (x, y) with lanes wrapped into sub-columns; return draw model. */
function layout(lineage) {
  const nodes = (lineage && lineage.nodes) || [];
  const edges = (lineage && lineage.edges) || [];
  const byId = new Map(nodes.map(n => [n.id, n]));
  const isJob = n => n.type === 'job';
  const laneIdxOf = n => LAYER_ORDER.indexOf(layerOf(n));

  // Bucket data nodes per lane; jobs handled separately (they sit between lanes).
  const dataByLane = LAYER_ORDER.map(() => []);
  nodes.forEach(n => { if (!isJob(n)) dataByLane[laneIdxOf(n)].push(n); });
  const jobs = nodes.filter(isJob);

  // Linear order within each lane (alpha seed) + a global order map used by the
  // barycenter sweeps (jobs ordered among themselves).
  const order = new Map();
  dataByLane.forEach(arr => {
    arr.sort((a, b) => String(a.label).localeCompare(String(b.label)))
      .forEach((n, i) => order.set(n.id, i));
  });
  jobs.sort((a, b) => String(a.label).localeCompare(String(b.label)))
    .forEach((n, i) => order.set(n.id, i));

  // Barycenter sweeps: order each lane by the mean row of connected neighbors.
  for (let pass = 0; pass < 4; pass++) {
    dataByLane.forEach(arr => {
      const bary = new Map();
      arr.forEach(n => {
        const rows = [];
        edges.forEach(e => {
          if (e.from === n.id && order.has(e.to)) rows.push(order.get(e.to));
          if (e.to === n.id && order.has(e.from)) rows.push(order.get(e.from));
        });
        bary.set(n.id, rows.length ? rows.reduce((s, v) => s + v, 0) / rows.length : order.get(n.id));
      });
      arr.sort((a, b) => bary.get(a.id) - bary.get(b.id)).forEach((n, i) => order.set(n.id, i));
    });
  }

  // Sub-column count per lane + rows per sub-column (balanced).
  const laneSubCols = dataByLane.map(arr => Math.max(1, Math.ceil(arr.length / MAX_ROWS)));
  const rowsPerSub  = dataByLane.map((arr, i) => Math.max(1, Math.ceil(arr.length / laneSubCols[i])));
  const laneWidth   = laneSubCols.map(sc => sc * SUBCOL_W);

  // Variable-width lane geometry (cumulative x so wide lanes don't overlap).
  const laneStartX = [];
  let cursor = PAD_X;
  LAYER_ORDER.forEach((_, i) => { laneStartX[i] = cursor; cursor += laneWidth[i] + LANE_GAP; });
  const laneCenterX = LAYER_ORDER.map((_, i) => laneStartX[i] + laneWidth[i] / 2);

  const gridRows = Math.max(1, ...dataByLane.map((arr, i) => Math.min(arr.length, rowsPerSub[i])), jobs.length);
  const yMid = PAD_TOP + (gridRows * ROW_H) / 2;

  const placed = [];
  dataByLane.forEach((arr, li) => {
    const rps = rowsPerSub[li];
    arr.forEach(n => {
      const k = order.get(n.id);
      const sub = Math.floor(k / rps);
      const row = k % rps;
      const colCount = Math.min(rps, arr.length - sub * rps);
      const x = laneStartX[li] + sub * SUBCOL_W + NODE_W / 2;
      const y = yMid + (row - (colCount - 1) / 2) * ROW_H;
      placed.push({ node: n, x, y, isJob: false });
    });
  });

  // Jobs: place in the gap between the neighbor lanes they connect, stacked centrally.
  jobs.forEach((n, i) => {
    const nbr = [];
    edges.forEach(e => {
      if (e.from === n.id) { const t = byId.get(e.to); if (t && !isJob(t)) nbr.push(laneIdxOf(t)); }
      if (e.to === n.id) { const f = byId.get(e.from); if (f && !isJob(f)) nbr.push(laneIdxOf(f)); }
    });
    const lo = nbr.length ? Math.min(...nbr) : 1;
    const hi = nbr.length ? Math.max(...nbr) : 2;
    const x = hi > lo
      ? (laneStartX[lo] + laneWidth[lo] + laneStartX[hi]) / 2
      : laneStartX[lo] + laneWidth[lo] + LANE_GAP / 2;
    const y = yMid + (i - (jobs.length - 1) / 2) * ROW_H;
    placed.push({ node: n, x, y, isJob: true });
  });

  const posById = new Map(placed.map(p => [p.node.id, p]));
  const laneBands = LAYER_ORDER.map((layer, i) => ({ layer, x: laneStartX[i], width: laneWidth[i], centerX: laneCenterX[i] }));
  const lastX = laneStartX[LAYER_ORDER.length - 1] + laneWidth[LAYER_ORDER.length - 1];
  const width = lastX + PAD_X;
  const height = PAD_TOP + gridRows * ROW_H + 30;
  return { placed, posById, edges, byId, width, height, laneBands };
}

export function renderColumnsLineage(container, lineage, opts = {}) {
  if (!container) return;

  const nNodes = ((lineage && lineage.nodes) || []).length;
  const nEdges = ((lineage && lineage.edges) || []).length;
  if (nNodes > MAX_NODES || nEdges > MAX_EDGES) {
    container.innerHTML =
      `<div style="padding:24px;text-align:center;color:#64748b;font-size:13px">` +
        `<div style="font-size:1.6rem;opacity:.5;margin-bottom:8px">🗺️</div>` +
        `This dependency map is too large to draw (${nNodes.toLocaleString()} nodes / ${nEdges.toLocaleString()} edges).<br/>` +
        `Use the lineage list / duplicate findings above to review it.` +
      `</div>`;
    return;
  }

  const { placed, posById, edges, width, height, laneBands } = layout(lineage);
  if (!placed.length) {
    container.innerHTML = '<div style="padding:14px;color:#64748b;font-size:13px">No lineage generated yet — run the reconcile step to build it.</div>';
    return;
  }

  const NS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('width', '100%');
  const root = select(svg);

  // Lane bands + titles (one per medallion layer; width tracks its sub-columns).
  const lanes = root.append('g');
  laneBands.forEach(b => {
    lanes.append('rect')
      .attr('x', b.x - 16).attr('y', 28)
      .attr('width', b.width + 32).attr('height', height - 40)
      .attr('rx', 12).attr('fill', LAYER_COLOR[b.layer]).attr('fill-opacity', 0.05)
      .attr('stroke', LAYER_COLOR[b.layer]).attr('stroke-opacity', 0.18);
    lanes.append('text')
      .attr('x', b.centerX).attr('y', 20).attr('text-anchor', 'middle')
      .attr('font-size', 12).attr('font-weight', 700).attr('letter-spacing', '1px')
      .attr('fill', LAYER_COLOR[b.layer]).text(LAYER_TITLE[b.layer]);
  });

  // Edges (drawn before nodes so nodes sit on top). 'copy'/'fk' = association links.
  const edgeG = root.append('g').attr('fill', 'none');
  edges.forEach(e => {
    const a = posById.get(e.from), b = posById.get(e.to);
    if (!a || !b) return;
    const x1 = a.x + (a.isJob ? 12 : NODE_W / 2), y1 = a.y;
    const x2 = b.x - (b.isJob ? 12 : NODE_W / 2), y2 = b.y;
    const mx = (x1 + x2) / 2;
    const assoc = e.label === 'copy' || e.label === 'fk';
    edgeG.append('path')
      .attr('d', `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`)
      .attr('stroke', assoc ? '#cbd5e1' : '#94a3b8')
      .attr('stroke-width', 1.4)
      .attr('stroke-dasharray', assoc ? '4 3' : null)
      .attr('stroke-opacity', 0.85);
    if (e.label && !assoc) {
      edgeG.append('text')
        .attr('x', mx).attr('y', (y1 + y2) / 2 - 3).attr('text-anchor', 'middle')
        .attr('font-size', 9).attr('fill', '#64748b').text(esc(e.label));
    }
  });

  // Nodes: data = rounded rect in its layer color; jobs = orange clickable diamond.
  const nodeG = root.append('g');
  placed.forEach(p => {
    const n = p.node;
    const g = nodeG.append('g').attr('transform', `translate(${p.x},${p.y})`);
    if (p.isJob) {
      const r = 13;
      g.append('path')
        .attr('d', `M0,${-r} L${r},0 L0,${r} L${-r},0 Z`)
        .attr('fill', LAYER_COLOR.job).attr('fill-opacity', 0.16)
        .attr('stroke', LAYER_COLOR.job).attr('stroke-width', 1.5)
        .style('cursor', 'pointer');
      g.append('text')
        .attr('x', 0).attr('y', r + 12).attr('text-anchor', 'middle')
        .attr('font-size', 10).attr('fill', LAYER_COLOR.job).attr('font-weight', 600)
        .text('⚙ ' + trunc(n.display || n.label, 22)).style('cursor', 'pointer');
      g.on('click', () => opts.onJobClick && opts.onJobClick(n.label));
      g.append('title').text(n.label);
    } else {
      const color = LAYER_COLOR[layerOf(n)] || '#64748b';
      g.append('rect')
        .attr('x', -NODE_W / 2).attr('y', -NODE_H / 2)
        .attr('width', NODE_W).attr('height', NODE_H).attr('rx', 6)
        .attr('fill', color).attr('fill-opacity', 0.12)
        .attr('stroke', color).attr('stroke-width', 1.3);
      g.append('text')
        .attr('x', 0).attr('y', 4).attr('text-anchor', 'middle')
        .attr('font-size', 11).attr('fill', 'var(--text-primary,#0f172a)')
        .attr('font-family', 'monospace')
        .text(trunc(n.display || n.label));
      g.append('title').text(n.label + (n.column_count != null ? ` (${n.column_count} cols)` : ''));
    }
  });

  container.innerHTML = '';
  container.appendChild(svg);
  enableSvgPanZoom(container);
}
