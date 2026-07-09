/**
 * NeuralChart — a simple "neural network" style layered node-link chart.
 *
 * Nodes are drawn as neurons (circles) arranged in vertical columns by a caller-
 * supplied rank, connected by thin curved edges — the clean, readable layered look
 * of a neural-network diagram. Generic over node types: the caller provides
 * ``rankOf`` (which column a node sits in), ``colorOf``, ``labelOf`` and optional
 * per-column ``rankLabels`` headers. Supports pan/zoom and fit-to-view.
 *
 * Used for the migration lineage views (Tableau data model + field lineage, and the
 * Snowflake/Glue medallion dataflow) in place of the cramped, shrink-to-fit Mermaid
 * diagram.
 */
import { select } from 'd3-selection';
import { zoom, zoomIdentity } from 'd3-zoom';
import { drag } from 'd3-drag';

const edgeSource = e => e.from || e.source;
const edgeTarget = e => e.to || e.target;

/**
 * Column index per node from the dataflow itself: longest path from root nodes
 * (no incoming edges), following edge direction. For a dataflow-directed graph
 * (source → job → bronze → job → silver → job → gold) this yields the true
 * upstream→downstream order, interleaving transform nodes between data layers.
 * Cycle nodes (rare; e.g. FK loops) keep rank 0.
 */
function computeTopoRanks(nodes, edges) {
  const adj = new Map(), indeg = new Map();
  nodes.forEach(n => { adj.set(n.id, []); indeg.set(n.id, 0); });
  edges.forEach(e => {
    if (!adj.has(e.from) || !indeg.has(e.to)) return;
    adj.get(e.from).push(e.to);
    indeg.set(e.to, indeg.get(e.to) + 1);
  });
  const rank = new Map(nodes.map(n => [n.id, 0]));
  const deg = new Map(indeg);
  const q = nodes.filter(n => deg.get(n.id) === 0).map(n => n.id);
  let head = 0;
  while (head < q.length) {
    const u = q[head++];
    for (const v of adj.get(u) || []) {
      if (rank.get(v) < rank.get(u) + 1) rank.set(v, rank.get(u) + 1);
      deg.set(v, deg.get(v) - 1);
      if (deg.get(v) === 0) q.push(v);
    }
  }
  return rank;
}

export class NeuralChart {
  constructor(container, graph, options = {}) {
    this.container = container;
    this.graph = graph || { nodes: [], edges: [] };
    this.options = {
      rankOf: () => 0,
      colorOf: () => '#475569',
      labelOf: n => n.display || n.label || n.id,
      rankLabels: {},
      onNodeClick: null,
      ...options,
    };
    this.svg = null;
    this.g = null;
    this.zoomBehavior = null;
    this.render();
  }

  destroy() {
    if (this.svg) this.svg.on('.zoom', null);
    this.container.innerHTML = '';
  }

  render() {
    const { rankOf, colorOf, labelOf, rankLabels, typeLabels, topo, onNodeClick } = this.options;
    const nodes = (this.graph.nodes || []).map(n => ({ ...n }));
    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const edges = (this.graph.edges || [])
      .map(e => ({ from: edgeSource(e), to: edgeTarget(e), label: e.label || '' }))
      .filter(e => nodeMap.has(e.from) && nodeMap.has(e.to) && e.from !== e.to);
    // Assign each node its column. ``topo`` derives it from the dataflow (longest
    // path from roots) so transform nodes (e.g. Glue jobs) and data layers fall into
    // true upstream→downstream order; otherwise the caller's ``rankOf`` fixes the
    // column by node type.
    if (topo) {
      const rankMap = computeTopoRanks(nodes, edges);
      nodes.forEach(n => { n._rank = rankMap.get(n.id) || 0; });
    } else {
      nodes.forEach(n => { n._rank = Number(rankOf(n)) || 0; });
    }

    this.container.innerHTML = '';
    const rect = this.container.getBoundingClientRect();
    const width = rect.width || 900;
    const height = rect.height || 460;
    this.svg = select(this.container).append('svg')
      .attr('width', '100%').attr('height', '100%')
      .attr('viewBox', `0 0 ${width} ${height}`);
    this.g = this.svg.append('g');
    this.zoomBehavior = zoom().scaleExtent([0.2, 3]).on('zoom', e => this.g.attr('transform', e.transform));
    this.svg.call(this.zoomBehavior);

    if (!nodes.length) {
      this.g.append('text').attr('x', 24).attr('y', 32).attr('fill', '#64748b').attr('font-size', 13)
        .text('No nodes to display.');
      return;
    }

    // Bucket into columns by rank, place each column and vertically center it.
    const COL_GAP = 240, ROW_GAP = 72, R = 16;
    const byRank = new Map();
    nodes.forEach(n => { if (!byRank.has(n._rank)) byRank.set(n._rank, []); byRank.get(n._rank).push(n); });
    const ranks = [...byRank.keys()].sort((a, b) => a - b);
    const maxCount = Math.max(1, ...ranks.map(r => byRank.get(r).length));
    ranks.forEach((r, colIdx) => {
      const col = byRank.get(r).sort((a, b) => String(labelOf(a)).localeCompare(String(labelOf(b))));
      const colH = (col.length - 1) * ROW_GAP;
      col.forEach((n, i) => { n.x = colIdx * COL_GAP; n.y = i * ROW_GAP - colH / 2; });
    });

    // Curved edges (neuron connections) — drawn first so nodes sit on top.
    const linkPath = (d) => {
      const s = nodeMap.get(d.from), t = nodeMap.get(d.to);
      const x1 = s.x, y1 = s.y, x2 = t.x, y2 = t.y;
      const mx = (x1 + x2) / 2;
      return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
    };
    this.g.append('g').selectAll('path').data(edges).enter().append('path')
      .attr('d', linkPath).attr('fill', 'none')
      .attr('stroke', '#94a3b8').attr('stroke-width', 1).attr('stroke-opacity', 0.5);

    // Column headers — explicit rankLabels, else (topo) the column's dominant type.
    const headerY = -((maxCount - 1) * ROW_GAP) / 2 - 34;
    ranks.forEach((r, colIdx) => {
      let title = rankLabels[r];
      if (!title && typeLabels) {
        const counts = {};
        byRank.get(r).forEach(n => { counts[n.type] = (counts[n.type] || 0) + 1; });
        const domType = Object.keys(counts).sort((a, b) => counts[b] - counts[a])[0];
        title = typeLabels[domType];
      }
      if (!title) return;
      this.g.append('text').attr('x', colIdx * COL_GAP).attr('y', headerY)
        .attr('text-anchor', 'middle').attr('font-size', 11).attr('font-weight', 800)
        .attr('letter-spacing', '1px').attr('fill', '#64748b').text(title);
    });

    // Neurons.
    const node = this.g.append('g').selectAll('g').data(nodes).enter().append('g')
      .attr('transform', d => `translate(${d.x},${d.y})`)
      .style('cursor', onNodeClick ? 'pointer' : 'default')
      .call(drag()
        .on('start', (e, d) => { d._dragged = true; })
        .on('drag', (e, d) => { d.x = e.x; d.y = e.y; redraw(); })
        .on('end', () => {}))
      .on('click', (_, d) => { if (onNodeClick) onNodeClick(d); });

    node.append('circle').attr('r', R)
      .attr('fill', d => colorOf(d)).attr('fill-opacity', 0.16)
      .attr('stroke', d => colorOf(d)).attr('stroke-width', 2);
    node.append('title').text(d => String(d.label || d.id));
    // Label below the neuron, truncated; full name on hover via <title>.
    node.append('text').attr('text-anchor', 'middle').attr('y', R + 14)
      .attr('font-size', 11).attr('font-weight', 600).attr('fill', '#0f172a')
      .text(d => { const s = String(labelOf(d)); return s.length > 22 ? s.slice(0, 21) + '…' : s; });

    const links = this.g.select('g').selectAll('path');
    const redraw = () => {
      links.attr('d', linkPath);
      node.attr('transform', d => `translate(${d.x},${d.y})`);
    };

    setTimeout(() => this.fitView(), 80);
  }

  fitView() {
    if (!this.svg || !this.g || !this.zoomBehavior) return;
    const bounds = this.g.node()?.getBBox?.();
    if (!bounds || !bounds.width || !bounds.height) return;
    const vb = String(this.svg.attr('viewBox') || '0 0 900 460').split(/\s+/).map(Number);
    const width = vb[2] || 900, height = vb[3] || 460;
    const pad = 1.06;
    const scale = Math.min(1.6, Math.max(0.2, 0.92 / Math.max((bounds.width * pad) / width, (bounds.height * pad) / height)));
    const x = (width - bounds.width * scale) / 2 - bounds.x * scale;
    const y = (height - bounds.height * scale) / 2 - bounds.y * scale;
    this.svg.transition().duration(260).call(this.zoomBehavior.transform, zoomIdentity.translate(x, y).scale(scale));
  }
}
