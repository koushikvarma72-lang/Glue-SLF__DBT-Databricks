/**
 * Migration Map — the "what moved where, and how" tab.
 *
 * One page that answers, for every part of the source estate, exactly which target
 * artifact it became and by what mechanism (AI conversion vs deterministic engine),
 * plus its current status (generated / deployed). Pure render over store state —
 * sfGlueConversion, sfGlueWorkflows, sfGlueWorkflowRun — no API calls.
 */
import { store } from '../store.js';
import { esc } from '../components/ui.js';

const MECH = {
  ai:      { label: 'AI conversion',     color: 'var(--error)',      bg: 'var(--error-soft)' },
  det:     { label: 'Deterministic',     color: 'var(--primary)',    bg: 'var(--primary-soft)' },
  retired: { label: 'Retired by design', color: 'var(--text-muted)', bg: 'var(--bg-inset)' },
  manual:  { label: 'Needs review',      color: 'var(--warning)',    bg: 'var(--warning-soft)' },
};

function mechChip(kind) {
  const m = MECH[kind] || MECH.det;
  return `<span class="mm-mech" style="color:${m.color};background:${m.bg}">
    <span class="mm-mech-dot" style="background:${m.color}"></span>${m.label}</span>`;
}

// Status tone is read from the label text (with an `ok` hint) so all the existing
// call sites keep working — deployed/generated/compiled/mapped → green, retired →
// neutral, everything else (planned/open/unmapped/missing) → amber.
function statusChip(ok, label) {
  const l = String(label || '').toLowerCase();
  let c, bg, mark;
  if (/retired/.test(l)) { c = 'var(--text-muted)'; bg = 'var(--bg-inset)'; mark = '⊘'; }
  else if (!ok || /missing|open|unmapped|planned/.test(l)) { c = 'var(--warning)'; bg = 'var(--warning-soft)'; mark = '●'; }
  else { c = 'var(--success)'; bg = 'var(--success-soft)'; mark = '✓'; }
  return `<span class="mm-status" style="color:${c};background:${bg}">${mark} ${esc(label)}</span>`;
}

function row(cols) {
  return `<div class="mm-row map-row" data-search="${esc(cols.search || '')}">
    <div class="mm-src">${cols.source}</div>
    <div class="mm-arrow">→</div>
    <div class="mm-tgt">${cols.target}</div>
    <div class="mm-how">${mechChip(cols.mech)}${cols.how ? `<span class="mm-how-text">${esc(cols.how)}</span>` : ''}</div>
    <div class="mm-statuscell">${cols.status}</div>
  </div>`;
}

function section(icon, title, subtitle, rows) {
  if (!rows.length) return '';
  return `
    <div class="mm-section">
      <div class="mm-section-head">
        <div class="mm-section-titles">
          <div class="mm-section-title">${esc(title)}</div>
          <div class="mm-section-sub">${esc(subtitle)}</div>
        </div>
        <div class="mm-section-count">${rows.length} item${rows.length === 1 ? '' : 's'}</div>
      </div>
      <div class="mm-head-row">
        <div>Source element</div><div></div><div>Becomes</div><div>How</div><div>Status</div>
      </div>
      ${rows.join('')}
    </div>`;
}

const code = (t) => `<code class="mm-code">${esc(t)}</code>`;
const sub = (t) => `<span class="mm-type">${esc(t)}</span>`;
const muted = (t) => `<span style="color:var(--text-muted)">${esc(t)}</span>`;
const codeList = (arr, max = 6) => {
  const shown = arr.slice(0, max).map(code).join(', ');
  return shown + (arr.length > max ? ` <span style="color:var(--text-muted)">+${arr.length - max} more</span>` : '');
};

// ─── Mapping derivation (mirrors the converters' naming contracts) ────────────
function buildSections(state) {
  const conv = state.sfGlueConversion || {};
  const plan = conv.plan || {};
  const notebooks = Object.keys(conv.notebooks || {});
  const models = Object.keys(conv.dbt_models || {});
  const ddl = Object.keys(conv.ddl || {});
  const cp = conv.control_plane || null;
  const wf = state.sfGlueWorkflows || {};
  const deployed = wf.deployed || [];
  const deployedByName = {};
  deployed.forEach(d => { if (d && d.name) deployedByName[d.name.replace(/^sfglue — /, '')] = d; });

  const out = [];

  // ── 1. Glue jobs ──────────────────────────────────────────────────────────
  {
    const rows = [];
    const ingestion = plan.ingestion_jobs || [];
    const transforms = plan.transformation_jobs || [];
    const publish = plan.publish_jobs || [];
    const pysparkT = new Set(plan.pyspark_transform_jobs || []);
    ingestion.forEach(n => {
      const nb = notebooks.filter(f => f === `${n}.py` || f.startsWith(`${n}__`));
      rows.push(row({
        search: n, source: `${code(n)} ${sub('E+L job')}`,
        target: nb.length ? codeList(nb) : muted('—'),
        mech: 'ai', how: 'Glue PySpark rewritten as a Databricks bronze-ingest notebook; S3 source kept as parameters, writes Delta to the source()/bronze location.',
        status: statusChip(nb.length > 0, nb.length ? 'generated' : 'missing'),
      }));
    });
    transforms.forEach(n => {
      if (pysparkT.has(n)) {
        rows.push(row({
          search: n, source: `${code(n)} ${sub('transform (procedural)')}`,
          target: codeList(notebooks.filter(f => f.startsWith(`${n}__`))),
          mech: 'ai', how: 'Non-relational logic (config loops / file reads / UDFs) — ported whole as a PySpark notebook so nothing is lost in a dbt rewrite.',
          status: statusChip(true, 'generated'),
        }));
      } else {
        const mine = models.filter(f => f === `${n}.sql`);
        rows.push(row({
          search: n, source: `${code(n)} ${sub('transform job')}`,
          target: mine.length ? codeList(mine) : 'dbt models (one per output table — see the dbt models section)',
          mech: 'ai', how: 'Decomposed one-dbt-model-per-output-table; refs/sources wired from the catalog + real bronze columns, not guessed.',
          status: statusChip(true, 'generated'),
        }));
      }
    });
    publish.forEach(n => {
      rows.push(row({
        search: n, source: `${code(n)} ${sub('publish job')}`,
        target: muted('nothing — intentionally retired'),
        mech: 'retired', how: 'This job replicated to Snowflake. On the target side, Databricks IS the warehouse — the gold layer serves consumers directly.',
        status: statusChip(true, 'retired'),
      }));
    });
    out.push(section('', 'Glue jobs', `${ingestion.length} E+L · ${transforms.length} transform · ${publish.length} publish`, rows));
  }

  // ── 2. Control framework (Postgres metadata) ──────────────────────────────
  if (cp) {
    const rows = [];
    const ctrlDdl = ddl.filter(d => d.startsWith('control__'));
    const qcModels = models.filter(f =>
      !f.endsWith('__rejects.sql') &&
      !(plan.transformation_jobs || []).some(n => f === `${n}.sql`));
    const rejects = models.filter(f => f.endsWith('__rejects.sql'));
    (cp.tables || []).forEach(t => {
      if (t === 'query_configuration') {
        rows.push(row({
          search: t, source: `${code(t)} ${sub('transform SQL rows')}`,
          target: `${qcModels.length} dbt models — ${codeList(qcModels, 5)}`,
          mech: 'ai', how: 'Each config row’s SQL rewritten to Databricks dialect as a governed dbt model (scaffold fallback if AI fails); the config-driven design becomes version-controlled code.',
          status: statusChip(qcModels.length > 0, `${qcModels.length} generated` + ((cp.skipped || []).length ? ` · ${cp.skipped.length} skipped` : '')),
        }));
      } else if (t === 'parent_batch_process') {
        rows.push(row({
          search: t, source: `${code(t)} ${sub('batch lifecycle')}`,
          target: `${code('control__' + t)} DDL + ${code('fw_batch_open.py')}, ${code('fw_batch_close.py')}`,
          mech: 'det', how: 'Table recreated as Delta; open/close semantics templated into framework notebooks that bracket every Job run.',
          status: statusChip(notebooks.includes('fw_batch_open.py'), 'generated'),
        }));
      } else if (t === 'dq_rules') {
        const dq = cp.dq_summary || {};
        rows.push(row({
          search: t, source: `${code(t)} ${sub('quality rules')}`,
          target: `${dq.dbt_tests || 0} dbt tests + ${rejects.length} quarantine model(s) + ${dq.notebook_checks || 0} notebook check(s)`,
          mech: 'det', how: 'Rules classified by shape: column constraints → dbt schema tests; reject-routing → __rejects quarantine models; file-level → bronze notebook checks.',
          status: statusChip(true, 'compiled'),
        }));
      } else if (t === 'message_template') {
        rows.push(row({
          search: t, source: code(t),
          target: 'Databricks Jobs email_notifications block',
          mech: 'det', how: 'Alert templates mapped to native Job success/failure notifications.',
          status: statusChip(!!conv.notifications, 'generated'),
        }));
      } else {
        const hit = ctrlDdl.find(d => d.includes(t));
        rows.push(row({
          search: t, source: `${code(t)} ${sub('framework table')}`,
          target: hit ? code(hit) + ' (Delta DDL)' : 'control-schema Delta DDL',
          mech: 'det', how: 'Postgres types mapped to Delta; lands in the control schema so the migrated framework keeps its ledger.',
          status: statusChip(!!hit || ctrlDdl.length > 0, 'generated'),
        }));
      }
    });
    out.push(section('', 'Control framework (Postgres → Unity Catalog)',
      `${(cp.tables || []).length} framework tables detected`, rows));
  }

  // ── 3. Orchestration (Glue Workflows + Airflow DAGs → Databricks Jobs) ────
  {
    const rows = [];
    (wf.planned || []).forEach(j => {
      const dagTasks = (j.dag && j.dag.tasks) || [];
      const jobTasks = ((j.job || {}).tasks) || [];
      const byKey = {};
      jobTasks.forEach(t => { byKey[t.task_key] = t; });
      const dep = deployedByName[(j.dag || {}).name] || deployed.find(d => (d.name || '').includes((j.dag || {}).name || ' '));
      const srcTag = j.source === 'airflow' ? 'Airflow DAG' : 'Glue Workflow';
      rows.push(row({
        search: (j.dag || {}).name || '',
        source: `${code((j.dag || {}).name || 'workflow')} ${sub(srcTag)}`,
        target: `Databricks Job ${code('sfglue — ' + ((j.dag || {}).name || ''))}${(j.dag || {}).schedule ? ' · Quartz ' + code((j.dag || {}).schedule) : ''}`,
        mech: 'det', how: 'Graph + triggers normalized to a task DAG, emitted as Jobs 2.1 JSON + DAB YAML; deployed idempotently by tag.',
        status: statusChip(!!(dep && dep.success), dep && dep.success ? `deployed · job ${dep.job_id}` : 'planned'),
      }));
      dagTasks.forEach(t => {
        const jt = byKey[t.key] || {};
        let target, mech = 'det', how;
        if (t.kind === 'sensor') {
          target = 'Job file-arrival trigger ' + muted('(task dropped)');
          how = 'Databricks has no in-job sensors — the polling task becomes a native trigger on the Job.';
        } else if (jt.dbt_task) {
          target = `dbt task ${code((jt.dbt_task.commands || ['dbt build']).join(' '))}`;
          how = 'Legacy transform task bound to the converted dbt models via the artifact map.';
        } else if (jt.notebook_task) {
          target = `notebook task ${code(jt.notebook_task.notebook_path || '')}`;
          how = 'Bound to its converted notebook via the auto-derived artifact map.';
        } else {
          target = muted('placeholder task — map an artifact');
          mech = 'manual';
          how = 'No converted artifact matched this legacy name; assign one in the artifact map.';
        }
        rows.push(row({
          search: `${t.key} ${t.legacy_name}`,
          source: `<span class="mm-child">└</span> ${code(t.legacy_name || t.key)} ${sub(t.kind || 'task')}`,
          target, mech, how,
          status: statusChip(mech !== 'manual', mech !== 'manual' ? 'mapped' : 'unmapped'),
        }));
      });
      ((j.dag || {}).warnings || []).forEach(w => {
        rows.push(row({
          search: 'warning', source: muted('warning'),
          target: `<span style="font-size:11px;color:var(--text-secondary)">${esc(w)}</span>`,
          mech: 'manual', how: '', status: '',
        }));
      });
    });
    out.push(section('', 'Orchestration (Workflows + Airflow → Databricks Jobs)',
      `${(wf.planned || []).length} pipeline(s) planned · ${deployed.filter(d => d.success).length} deployed`, rows));
  }

  // ── 4. Tables & project scaffolding ───────────────────────────────────────
  {
    const rows = [];
    const dataDdl = ddl.filter(d => !d.startsWith('control__'));
    if (dataDdl.length) {
      rows.push(row({
        search: 'tables ddl', source: `${dataDdl.length} source tables ${sub('Snowflake / catalog')}`,
        target: codeList(dataDdl, 5) + ' (Delta DDL)',
        mech: 'det', how: 'Types mapped Snowflake → Databricks; medallion schemas (bronze/silver/gold) preserved.',
        status: statusChip(true, 'generated'),
      }));
    }
    if ((plan.bronze_tables || []).length) {
      rows.push(row({
        search: 'bronze sources', source: `${(plan.bronze_tables || []).length} bronze entities`,
        target: `${code('sources.yml')} — dbt source('bronze', …) definitions`,
        mech: 'det', how: 'The raw vocabulary every staging model reads from; names locked to what the ingest notebooks write.',
        status: statusChip(!!conv.sources_yml, 'generated'),
      }));
    }
    [['schema_yml', 'schema.yml — column contracts + tests'], ['unit_tests_yml', 'dbt unit-test scaffolds'],
     ['dbt_project_yml', 'dbt_project.yml + profiles.yml — runnable project'], ['governance_md', 'governance / cutover checklist']]
      .forEach(([k, label]) => {
        if (conv[k]) rows.push(row({
          search: k, source: 'project scaffolding',
          target: esc(label), mech: 'det',
          how: 'Generated from the conversion metadata — no AI.',
          status: statusChip(true, 'generated'),
        }));
      });
    if ((conv.untranslatable || []).length) {
      rows.push(row({
        search: 'review queue', source: `${(conv.untranslatable || []).length} flagged item(s)`,
        target: 'human review queue (Review & Edit step)',
        mech: 'manual', how: 'TODOs the converters could not translate faithfully — the ship gate stays red until these are resolved.',
        status: statusChip(false, 'open'),
      }));
    }
    out.push(section('', 'Tables, contracts & project', 'DDL · dbt project scaffolding · review queue', rows));
  }

  return out.filter(Boolean);
}

// KPI tiles across the top — a section-by-section headline, computed straight from
// the conversion/workflow state (independent of the row markup below).
function buildKpis(state) {
  const conv = state.sfGlueConversion || {};
  const plan = conv.plan || {};
  const cp = conv.control_plane;
  const wf = state.sfGlueWorkflows || {};
  const glueJobs = (plan.ingestion_jobs || []).length + (plan.transformation_jobs || []).length + (plan.publish_jobs || []).length;
  const fwTables = cp ? (cp.tables || []).length : 0;
  const pipelines = (wf.planned || []).length;
  const deployedN = (wf.deployed || []).filter(d => d && d.success).length;
  const reviewN = (conv.untranslatable || []).length;
  const tile = (num, label, accent) =>
    `<div class="mm-kpi"><div class="mm-kpi-num" style="${accent ? `color:${accent}` : ''}">${num}</div>
      <div class="mm-kpi-label">${label}</div></div>`;
  return `
    ${tile(glueJobs, 'Glue jobs')}
    ${tile(fwTables, 'Framework tables')}
    ${tile(`${deployedN}<span class="mm-kpi-of">/${pipelines}</span>`, 'Pipelines deployed')}
    ${tile(reviewN, 'Needs review', reviewN ? 'var(--warning)' : 'var(--success)')}`;
}

const LEGEND = Object.entries(MECH).map(([, m]) =>
  `<span class="mm-legend-item"><span class="mm-dot" style="background:${m.color}"></span>${m.label}</span>`).join('');

const STYLES = `<style>
  .mm-wrap { max-width: 1180px; margin: 0 auto; }
  .mm-title { margin: 0 0 4px; }
  .mm-subtitle { color: var(--text-secondary); margin: 0 0 18px; font-size: 13px; line-height: 1.6; }

  .mm-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .mm-kpi { background: var(--glass-bg); -webkit-backdrop-filter: var(--glass-blur); backdrop-filter: var(--glass-blur);
    border: 1px solid var(--glass-border); border-radius: var(--radius-lg); box-shadow: var(--shadow-sm); padding: 15px 17px; }
  .mm-kpi-num { font-size: 26px; font-weight: 700; color: var(--text-primary); line-height: 1; letter-spacing: -0.5px; }
  .mm-kpi-of { font-size: 16px; font-weight: 600; color: var(--text-muted); }
  .mm-kpi-label { font-size: 10.5px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.6px; margin-top: 8px; font-weight: 600; }

  .mm-toolbar { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; margin-bottom: 18px; }
  .mm-legend { display: flex; flex-wrap: wrap; gap: 14px; }
  .mm-legend-item { display: inline-flex; align-items: center; gap: 6px; font-size: 11px; color: var(--text-secondary); }
  .mm-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
  .mm-filter { flex: 1 1 260px; min-width: 220px; max-width: 420px; margin-left: auto; padding: 9px 13px; font-size: 12px;
    border: 1px solid var(--glass-border); border-radius: var(--radius-md); background: var(--bg-surface); color: var(--text-primary); }
  .mm-filter:focus { outline: none; border-color: var(--border-active); box-shadow: 0 0 0 3px var(--primary-glow); }

  .mm-section { background: var(--glass-bg); -webkit-backdrop-filter: var(--glass-blur); backdrop-filter: var(--glass-blur);
    border: 1px solid var(--glass-border); border-radius: var(--radius-lg); box-shadow: var(--shadow-sm); margin-bottom: 18px; overflow: hidden; }
  .mm-section-head { display: flex; align-items: center; gap: 13px; padding: 14px 18px;
    background: linear-gradient(180deg, rgba(255,255,255,0.5), rgba(255,255,255,0.12)); border-bottom: 1px solid var(--glass-border-soft); }
  .mm-section-icon { width: 36px; height: 36px; border-radius: 10px; display: flex; align-items: center; justify-content: center;
    font-size: 18px; background: var(--bg-inset); flex-shrink: 0; }
  .mm-section-titles { min-width: 0; }
  .mm-section-title { font-size: 14px; font-weight: 700; color: var(--text-primary); }
  .mm-section-sub { font-size: 11px; color: var(--text-muted); margin-top: 2px; }
  .mm-section-count { margin-left: auto; font-size: 11px; font-weight: 600; color: var(--text-secondary);
    background: var(--bg-inset); padding: 4px 11px; border-radius: var(--radius-full); white-space: nowrap; flex-shrink: 0; }

  .mm-head-row, .mm-row { display: grid;
    grid-template-columns: minmax(0, 2.4fr) 22px minmax(0, 2.7fr) minmax(0, 3fr) minmax(0, 1.35fr);
    gap: 14px; padding: 11px 18px; align-items: start; }
  .mm-head-row { font-size: 10px; font-weight: 700; color: var(--text-muted); text-transform: uppercase;
    letter-spacing: 0.6px; background: var(--bg-elevated); border-bottom: 1px solid var(--glass-border-soft); }
  .mm-row { border-top: 1px solid var(--glass-border-soft); transition: background var(--transition-fast); }
  .mm-row:hover { background: var(--bg-hover); }

  .mm-src { font-size: 12.5px; font-weight: 600; color: var(--text-primary); word-break: break-word; line-height: 1.5; }
  .mm-type { display: block; font-size: 10px; font-weight: 500; color: var(--text-muted); margin-top: 3px; }
  .mm-child { color: var(--text-dim); margin-right: 2px; }
  .mm-arrow { color: var(--text-dim); font-size: 13px; text-align: center; padding-top: 1px; }
  .mm-tgt { font-size: 12px; color: var(--text-primary); word-break: break-word; line-height: 1.55; }
  .mm-code { font-family: var(--font-mono); font-size: 11px; background: var(--bg-inset); padding: 1px 5px; border-radius: 4px; }
  .mm-how { font-size: 11px; line-height: 1.5; }
  .mm-how-text { display: block; margin-top: 6px; color: var(--text-muted); }
  .mm-mech { display: inline-flex; align-items: center; gap: 5px; font-size: 10px; font-weight: 700;
    padding: 3px 9px; border-radius: var(--radius-full); white-space: nowrap; }
  .mm-mech-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
  .mm-statuscell { display: flex; justify-content: flex-end; }
  .mm-status { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; font-weight: 600;
    padding: 3px 10px; border-radius: var(--radius-full); white-space: nowrap; height: fit-content; }

  .mm-empty { background: var(--glass-bg); border: 1px solid var(--glass-border); border-radius: var(--radius-lg);
    padding: 22px; font-size: 13px; color: var(--text-secondary); line-height: 1.6; }
</style>`;

// ─── Page ─────────────────────────────────────────────────────────────────────
export function renderSfGlueMapPage(container) {
  const state = store.get();
  const conv = state.sfGlueConversion;

  container.innerHTML = `
    ${STYLES}
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div class="mm-wrap">
        <h2 class="mm-title">Migration Map</h2>
        <p class="mm-subtitle">Every source element, what it became on Databricks, and how.</p>
        ${conv ? `
          <div class="mm-kpis">${buildKpis(state)}</div>
          <div class="mm-toolbar">
            <div class="mm-legend">${LEGEND}</div>
            <input id="map-filter" class="mm-filter" placeholder="Filter by name (job, table, task…)" spellcheck="false">
          </div>
          <div id="map-sections">${buildSections(state).join('')}</div>
        ` : `
          <div class="mm-empty">
            <div>No conversion yet — generate one and the full source→target map appears here.</div>
            <button class="btn btn-primary" id="map-goto-review" style="margin-top:12px">Go to Review &amp; Edit →</button>
          </div>
        `}
      </div>
    </div>`;

  container.querySelector('#map-filter')?.addEventListener('input', (e) => {
    const q = (e.target.value || '').toLowerCase().trim();
    container.querySelectorAll('.map-row').forEach(r => {
      const hay = (r.getAttribute('data-search') || '') + ' ' + r.textContent;
      r.style.display = !q || hay.toLowerCase().includes(q) ? '' : 'none';
    });
    // Hide a whole section when the filter empties it out.
    container.querySelectorAll('.mm-section').forEach(sec => {
      const anyVisible = [...sec.querySelectorAll('.map-row')].some(r => r.style.display !== 'none');
      sec.style.display = anyVisible ? '' : 'none';
    });
  });

  container.querySelector('#map-goto-review')?.addEventListener('click', () => store.navigate('sfglue-review'));
}

export function destroySfGlueMapPage() { /* stateless */ }
