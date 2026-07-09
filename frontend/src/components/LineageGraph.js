import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation } from 'd3-force';
import { select } from 'd3-selection';
import { zoom, zoomIdentity } from 'd3-zoom';
import { drag } from 'd3-drag';

const COLORS = {
  source: '#2563eb',
  bronze: '#8b5cf6',
  silver: '#0f766e',
  gold: '#ca8a04',
  job: '#ea580c',
  dimension: '#16a34a',
  measure: '#dc2626',
  kpi: '#9333ea',
  hierarchy: '#0284c7',
};

const DEFAULT_TYPES = new Set(['source', 'bronze', 'silver', 'gold', 'job', 'dimension', 'measure', 'kpi', 'hierarchy']);

function edgeSource(edge) {
  return edge.from || edge.source;
}

function edgeTarget(edge) {
  return edge.to || edge.target;
}

function buildVisibleGraph(lineage, options) {
  const allNodes = lineage?.nodes || [];
  const allEdges = lineage?.edges || [];
  const showFull = !!options.showFull;
  const enabledTypes = options.enabledTypes || DEFAULT_TYPES;
  const selectedKpi = options.selectedKpi || '';
  const kpiNodes = allNodes.filter(node => node.type === 'kpi');
  const defaultKpiIds = new Set(kpiNodes.slice(0, 5).map(node => node.id));
  const selectedEdges = new Set();

  if (selectedKpi) {
    let changed = true;
    while (changed) {
      changed = false;
      allEdges.forEach(edge => {
        const source = edgeSource(edge);
        const target = edgeTarget(edge);
        if (target === selectedKpi || selectedEdges.has(target) || ['gold_kpi', 'silver_table', 'bronze_table'].includes(target)) {
          if (!selectedEdges.has(source)) {
            selectedEdges.add(source);
            changed = true;
          }
          selectedEdges.add(target);
        }
      });
    }
    selectedEdges.add(selectedKpi);
    selectedEdges.add('source_qvd');
    selectedEdges.add('bronze_table');
    selectedEdges.add('silver_table');
    selectedEdges.add('gold_kpi');
  }

  let nodes = allNodes.filter(node => enabledTypes.has(node.type || ''));
  if (!showFull && !selectedKpi) {
    nodes = nodes.filter(node => ['source', 'bronze', 'silver', 'gold', 'hierarchy'].includes(node.type) || defaultKpiIds.has(node.id) || node.type === 'dimension');
    nodes = nodes.filter((node, index) => node.type !== 'dimension' || index < 18);
  }
  if (selectedKpi) {
    nodes = allNodes.filter(node => selectedEdges.has(node.id));
  }
  const ids = new Set(nodes.map(node => node.id));
  const edges = allEdges.filter(edge => ids.has(edgeSource(edge)) && ids.has(edgeTarget(edge)));
  return { nodes, edges };
}

export class LineageGraph {
  constructor(container, lineage, options = {}) {
    this.container = container;
    this.lineage = lineage || { nodes: [], edges: [] };
    this.options = {
      showFull: false,
      selectedKpi: '',
      enabledTypes: DEFAULT_TYPES,
      onNodeClick: null,
      ...options,
    };
    this.svg = null;
    this.g = null;
    this.simulation = null;
    this.zoomBehavior = null;
    this.render();
  }

  update(options = {}) {
    this.options = { ...this.options, ...options };
    this.render();
  }

  destroy() {
    if (this.simulation) this.simulation.stop();
    this.container.innerHTML = '';
  }

  render() {
    const graph = buildVisibleGraph(this.lineage, this.options);
    this.container.innerHTML = '';
    if (!graph.nodes.length) {
      this.container.innerHTML = '<div class="inspect-empty" style="display:flex;align-items:center;justify-content:center;height:100%">No lineage nodes generated yet.</div>';
      return;
    }
    const rect = this.container.getBoundingClientRect();
    const width = rect.width || 900;
    const height = rect.height || 520;
    this.svg = select(this.container)
      .append('svg')
      .attr('width', '100%')
      .attr('height', '100%')
      .attr('viewBox', `0 0 ${width} ${height}`);
    this.g = this.svg.append('g');
    this.zoomBehavior = zoom().scaleExtent([0.3, 3]).on('zoom', event => this.g.attr('transform', event.transform));
    this.svg.call(this.zoomBehavior);
    // Layered (tree/flow) layout: columns left→right by medallion layer so the
    // source→destination flow reads clearly. Opt-in; force layout is the default.
    if (this.options.layout === 'layered') {
      this.renderLayered(graph);
      return;
    }
    const nodes = graph.nodes.map(node => ({ ...node }));
    const edges = graph.edges.map(edge => ({ ...edge }));
    const nodeMap = new Map(nodes.map(node => [node.id, node]));
    const links = edges
      .map(edge => ({ ...edge, source: nodeMap.get(edgeSource(edge)), target: nodeMap.get(edgeTarget(edge)) }))
      .filter(edge => edge.source && edge.target);
    this.simulation = forceSimulation(nodes)
      .force('link', forceLink(links).id(node => node.id).distance(150))
      .force('charge', forceManyBody().strength(-460))
      .force('center', forceCenter(width / 2, height / 2))
      .force('collision', forceCollide().radius(74));

    const link = this.g.append('g').selectAll('line').data(links).enter().append('line')
      .attr('stroke', '#94a3b8')
      .attr('stroke-width', 1.4)
      .attr('stroke-opacity', 0.75);
    const labels = this.g.append('g').selectAll('text').data(links).enter().append('text')
      .attr('font-size', 10)
      .attr('fill', '#64748b')
      .attr('text-anchor', 'middle')
      .text(edge => edge.label || '');
    const node = this.g.append('g').selectAll('g').data(nodes).enter().append('g')
      .attr('class', 'qvd-lineage-node')
      .style('cursor', 'pointer')
      .call(drag()
        .on('start', (event, datum) => {
          if (!event.active) this.simulation.alphaTarget(0.3).restart();
          datum.fx = datum.x;
          datum.fy = datum.y;
        })
        .on('drag', (event, datum) => {
          datum.fx = event.x;
          datum.fy = event.y;
        })
        .on('end', (event, datum) => {
          if (!event.active) this.simulation.alphaTarget(0);
          datum.fx = null;
          datum.fy = null;
        }))
      .on('click', (_, datum) => {
        if (this.options.onNodeClick) this.options.onNodeClick(datum);
      });

    node.append('rect')
      .attr('x', -70)
      .attr('y', -28)
      .attr('width', 140)
      .attr('height', 56)
      .attr('rx', 8)
      .attr('fill', datum => COLORS[datum.type] || '#475569')
      .attr('opacity', 0.13)
      .attr('stroke', datum => COLORS[datum.type] || '#475569')
      .attr('stroke-width', 1.4);
    // Prefer a compact display label (e.g. schema.table); fall back to the full
    // label. The full name is available on hover via the <title> below.
    node.append('title').text(datum => String(datum.label || datum.id));
    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('y', -4)
      .attr('font-size', 11)
      .attr('font-weight', 700)
      .attr('fill', '#0f172a')
      .text(datum => String(datum.display || datum.label || datum.id).slice(0, 22));
    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('y', 14)
      .attr('font-size', 10)
      .attr('fill', '#64748b')
      .text(datum => datum.type || '');

    this.simulation.on('tick', () => {
      link.attr('x1', d => d.source.x).attr('y1', d => d.source.y).attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      labels.attr('x', d => (d.source.x + d.target.x) / 2).attr('y', d => (d.source.y + d.target.y) / 2);
      node.attr('transform', d => `translate(${d.x},${d.y})`);
    });
    setTimeout(() => this.fitView(), 250);
  }

  // Static layered layout — one column per medallion layer (source → bronze →
  // silver → gold), nodes stacked & centered within each column, with curved,
  // arrow-headed flow links and per-column headers. No force simulation, so the
  // graph is stable and reads as a left-to-right tree from source to destination.
  renderLayered(graph) {
    const NODE_W = 152;
    const NODE_H = 44;
    const COL_GAP = 230;
    const ROW_GAP = 64;
    const RANK = { source: 0, bronze: 1, silver: 2, gold: 3 };
    const RANK_TYPE = { 0: 'source', 1: 'bronze', 2: 'silver', 3: 'gold' };
    const RANK_NAME = { 0: 'SOURCE', 1: 'BRONZE', 2: 'SILVER', 3: 'GOLD' };

    const nodes = graph.nodes.map(node => ({ ...node }));
    const nodeMap = new Map(nodes.map(node => [node.id, node]));

    // Bucket nodes into columns by layer, then place & vertically-center each column.
    const byRank = new Map();
    nodes.forEach(node => {
      const r = RANK[node.type] != null ? RANK[node.type] : 2;
      if (!byRank.has(r)) byRank.set(r, []);
      byRank.get(r).push(node);
    });
    const ranks = [...byRank.keys()].sort((a, b) => a - b);
    const maxCount = Math.max(1, ...ranks.map(r => byRank.get(r).length));
    ranks.forEach((r, colIdx) => {
      const colNodes = byRank.get(r)
        .sort((a, b) => String(a.display || a.label).localeCompare(String(b.display || b.label)));
      const colH = (colNodes.length - 1) * ROW_GAP;
      colNodes.forEach((node, i) => {
        node.x = colIdx * COL_GAP;
        node.y = i * ROW_GAP - colH / 2;
      });
    });

    // Arrowhead marker for link direction.
    this.svg.append('defs').append('marker')
      .attr('id', 'lg-arrow').attr('viewBox', '0 0 10 10').attr('refX', 9).attr('refY', 5)
      .attr('markerWidth', 7).attr('markerHeight', 7).attr('orient', 'auto-start-reverse')
      .append('path').attr('d', 'M0,0 L10,5 L0,10 z').attr('fill', '#94a3b8');

    // Curved left→right flow links.
    const links = graph.edges
      .map(edge => ({ ...edge, source: nodeMap.get(edgeSource(edge)), target: nodeMap.get(edgeTarget(edge)) }))
      .filter(edge => edge.source && edge.target);
    const linkPath = (d) => {
      const x1 = d.source.x + NODE_W / 2, y1 = d.source.y;
      const x2 = d.target.x - NODE_W / 2, y2 = d.target.y;
      const mx = (x1 + x2) / 2;
      return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
    };
    // "copy"/"fk" are association links (same table in both systems / key
    // reference), not a dataflow step — render them dashed and arrow-less so the
    // real flow arrows stand out.
    const isAssoc = d => d.label === 'copy' || d.label === 'fk';
    this.g.append('g').selectAll('path').data(links).enter().append('path')
      .attr('d', linkPath).attr('fill', 'none')
      .attr('stroke', d => isAssoc(d) ? '#cbd5e1' : '#94a3b8')
      .attr('stroke-width', 1.5).attr('stroke-opacity', 0.8)
      .attr('stroke-dasharray', d => isAssoc(d) ? '4 4' : null)
      .attr('marker-end', d => isAssoc(d) ? null : 'url(#lg-arrow)');
    this.g.append('g').selectAll('text').data(links.filter(d => d.label)).enter().append('text')
      .attr('x', d => (d.source.x + d.target.x) / 2)
      .attr('y', d => (d.source.y + d.target.y) / 2 - 4)
      .attr('text-anchor', 'middle').attr('font-size', 9)
      .attr('paint-order', 'stroke').attr('stroke', '#f8fafc').attr('stroke-width', 3)
      .attr('fill', '#64748b').text(d => d.label);

    // Column headers (SOURCE / BRONZE / SILVER / GOLD).
    const headerY = -((maxCount - 1) * ROW_GAP) / 2 - 30;
    ranks.forEach((r, colIdx) => {
      this.g.append('text')
        .attr('x', colIdx * COL_GAP).attr('y', headerY).attr('text-anchor', 'middle')
        .attr('font-size', 11).attr('font-weight', 800).attr('letter-spacing', '1px')
        .attr('fill', COLORS[RANK_TYPE[r]] || '#475569')
        .text(RANK_NAME[r] || String(r));
    });

    // Nodes.
    const node = this.g.append('g').selectAll('g').data(nodes).enter().append('g')
      .style('cursor', 'pointer')
      .attr('transform', d => `translate(${d.x},${d.y})`)
      .on('click', (_, datum) => { if (this.options.onNodeClick) this.options.onNodeClick(datum); });
    node.append('rect')
      .attr('x', -NODE_W / 2).attr('y', -NODE_H / 2).attr('width', NODE_W).attr('height', NODE_H)
      .attr('rx', 8).attr('fill', d => COLORS[d.type] || '#475569').attr('opacity', 0.13)
      .attr('stroke', d => COLORS[d.type] || '#475569').attr('stroke-width', 1.4);
    node.append('title').text(d => String(d.label || d.id));
    node.append('text')
      .attr('text-anchor', 'middle').attr('y', -2).attr('font-size', 11).attr('font-weight', 700)
      .attr('fill', '#0f172a').text(d => String(d.display || d.label || d.id).slice(0, 24));
    node.append('text')
      .attr('text-anchor', 'middle').attr('y', 13).attr('font-size', 9).attr('fill', '#64748b')
      .text(d => d.type || '');

    setTimeout(() => this.fitView(), 60);
  }

  fitView() {
    if (!this.svg || !this.g || !this.zoomBehavior) return;
    const bounds = this.g.node()?.getBBox?.();
    if (!bounds || !bounds.width || !bounds.height) return;
    const viewBox = String(this.svg.attr('viewBox') || '0 0 900 520').split(/\s+/).map(Number);
    const width = viewBox[2] || 900;
    const height = viewBox[3] || 520;
    const scale = Math.min(2, Math.max(0.35, 0.92 / Math.max(bounds.width / width, bounds.height / height)));
    const x = (width - bounds.width * scale) / 2 - bounds.x * scale;
    const y = (height - bounds.height * scale) / 2 - bounds.y * scale;
    this.svg.transition().duration(260).call(this.zoomBehavior.transform, zoomIdentity.translate(x, y).scale(scale));
  }
}
