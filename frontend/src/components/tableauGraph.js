/**
 * Tableau lineage → {nodes, edges} graphs for the NeuralChart layered view.
 *
 *   tableauModelGraph — object level: datasources ↔ worksheets ↔ dashboards
 *                       (+ parameters), with field/calc counts folded into each
 *                       datasource label. Small and readable.
 *   tableauFieldGraph — for one datasource, the calculation lineage: each calc and
 *                       the columns it derives from (the real "lineage between fields").
 */

export const TAB_COLOR = {
  datasource: '#2563eb', worksheet: '#0f766e', dashboard: '#ca8a04',
  parameter: '#9333ea', field: '#16a34a', calc: '#dc2626',
};

// Column (layer) each node type sits in for the object-level model view.
export const TAB_MODEL_RANK = { datasource: 0, parameter: 0, worksheet: 1, dashboard: 2 };
export const TAB_MODEL_RANK_LABELS = { 0: 'DATASOURCES', 1: 'WORKSHEETS', 2: 'DASHBOARDS' };

export const TAB_FIELD_RANK = { field: 0, calc: 1 };
export const TAB_FIELD_RANK_LABELS = { 0: 'COLUMNS', 1: 'CALCULATED FIELDS' };

const _isObj = t => ['datasource', 'worksheet', 'dashboard', 'parameter'].includes(t);

export function datasourceNodes(lineage) {
  return ((lineage && lineage.nodes) || [])
    .filter(n => n.type === 'datasource' || String(n.id).startsWith('ds:'))
    .sort((a, b) => String(a.label || a.id).localeCompare(String(b.label || b.id)));
}

/** Object-level data model: datasources ↔ worksheets ↔ dashboards (+ parameters). */
export function tableauModelGraph(lineage) {
  const allNodes = (lineage && lineage.nodes) || [];
  const allEdges = (lineage && lineage.edges) || [];

  // field/calc counts per datasource (collapsed in this view).
  const counts = new Map();
  allNodes.forEach(n => {
    if ((n.type === 'field' || n.type === 'calc') && n.datasource) {
      const c = counts.get(n.datasource) || { fields: 0, calcs: 0 };
      if (n.type === 'calc') c.calcs += 1; else c.fields += 1;
      counts.set(n.datasource, c);
    }
  });

  const nodes = allNodes.filter(n => _isObj(n.type)).map(n => {
    if (n.type === 'datasource') {
      const c = counts.get(n.id);
      const base = n.display || n.label || n.id;
      return { ...n, display: c ? `${base} · ${c.fields}f/${c.calcs}c` : base };
    }
    return { ...n };
  });
  const keep = new Set(nodes.map(n => n.id));
  const edges = allEdges.filter(e => keep.has(e.from) && keep.has(e.to));
  return { nodes, edges };
}

/** Calculation lineage for one datasource: calcs → the columns they derive from. */
export function tableauFieldGraph(lineage, datasourceId) {
  const allNodes = (lineage && lineage.nodes) || [];
  const allEdges = (lineage && lineage.edges) || [];
  const dss = allNodes.filter(n => n.type === 'datasource');
  const dsId = (datasourceId && allNodes.some(n => n.id === datasourceId))
    ? datasourceId : (dss[0] && dss[0].id);
  if (!dsId) return { nodes: [], edges: [] };

  const CAP = 60;
  const calcs = allNodes.filter(n => n.type === 'calc' && n.datasource === dsId).slice(0, CAP);
  const calcIds = new Set(calcs.map(n => n.id));
  const deriveEdges = allEdges.filter(e => e.label === 'derives' && calcIds.has(e.from));
  const refIds = new Set(deriveEdges.map(e => e.to));
  const byId = new Map(allNodes.map(n => [n.id, n]));
  const refNodes = [...refIds].map(id => byId.get(id)).filter(Boolean);

  const nodes = [...calcs, ...refNodes].map(n => ({ ...n }));
  const ids = new Set(nodes.map(n => n.id));
  const edges = deriveEdges.filter(e => ids.has(e.from) && ids.has(e.to));
  return { nodes, edges };
}
