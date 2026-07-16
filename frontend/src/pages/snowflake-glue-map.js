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
  ai: { label: 'AI conversion', color: 'var(--error)' },          // red-orange accent
  det: { label: 'Deterministic', color: 'var(--primary)' },
  retired: { label: 'Retired by design', color: 'var(--text-muted)' },
  manual: { label: 'Needs review', color: 'var(--warning, #b8860b)' },
};

function mechChip(kind) {
  const m = MECH[kind] || MECH.det;
  return `<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;
    border:1px solid ${m.color};color:${m.color};white-space:nowrap">${m.label}</span>`;
}

function statusChip(ok, label) {
  const c = ok ? 'var(--success)' : 'var(--text-muted)';
  return `<span style="font-size:11px;color:${c};white-space:nowrap">${ok ? '✓' : '○'} ${esc(label)}</span>`;
}

function row(cols) {
  return `<div class="map-row" data-search="${esc(cols.search || '')}"
    style="display:grid;grid-template-columns:2.2fr 0.5fr 2.6fr 2.6fr 1fr;gap:10px;
    padding:9px 14px;border-bottom:1px solid var(--border);align-items:start">
    <div style="font-size:12px;font-weight:600;color:var(--text-primary);word-break:break-word">${cols.source}</div>
    <div style="text-align:center;color:var(--text-muted);font-size:12px">→</div>
    <div style="font-size:12px;color:var(--text-primary);word-break:break-word">${cols.target}</div>
    <div style="font-size:11px;color:var(--text-secondary)">${mechChip(cols.mech)} ${esc(cols.how)}</div>
    <div>${cols.status}</div>
  </div>`;
}

function section(title, subtitle, rows) {
  if (!rows.length) return '';
  return `
    <div class="card" style="margin-bottom:16px">
      <div class="card-header"><div class="card-title">${title}</div>
        <div style="font-size:11px;color:var(--text-muted)">${esc(subtitle)}</div></div>
      <div class="card-body" style="padding:0">
        <div style="display:grid;grid-template-columns:2.2fr 0.5fr 2.6fr 2.6fr 1fr;gap:10px;
          padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;
          color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">
          <div>Source element</div><div></div><div>Becomes</div><div>How</div><div>Status</div>
        </div>
        ${rows.join('')}
      </div>
    </div>`;
}

const code = (t) => `<code style="font-size:11px">${esc(t)}</code>`;
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
  const pushed = !!(state.sfGlueWorkflowRun || deployed.length);
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
    const transformModels = new Set(transforms.map(n => `${n}.sql`));
    ingestion.forEach(n => {
      const nb = notebooks.filter(f => f === `${n}.py` || f.startsWith(`${n}__`));
      rows.push(row({
        search: n, source: `${code(n)} <span style="color:var(--text-muted);font-size:10px">E+L job</span>`,
        target: nb.length ? codeList(nb) : '<span style="color:var(--text-muted)">—</span>',
        mech: 'ai', how: 'Glue PySpark rewritten as a Databricks bronze-ingest notebook; S3 source kept as parameters, writes Delta to the source()/bronze location.',
        status: statusChip(nb.length > 0, nb.length ? 'generated' : 'missing'),
      }));
    });
    transforms.forEach(n => {
      if (pysparkT.has(n)) {
        rows.push(row({
          search: n, source: `${code(n)} <span style="color:var(--text-muted);font-size:10px">transform (procedural)</span>`,
          target: codeList(notebooks.filter(f => f.startsWith(`${n}__`))),
          mech: 'ai', how: 'Non-relational logic (config loops / file reads / UDFs) — ported whole as a PySpark notebook so nothing is lost in a dbt rewrite.',
          status: statusChip(true, 'generated'),
        }));
      } else {
        const mine = models.filter(f => f === `${n}.sql`);
        rows.push(row({
          search: n, source: `${code(n)} <span style="color:var(--text-muted);font-size:10px">transform job</span>`,
          target: mine.length ? codeList(mine) : 'dbt models (one per output table — see the dbt models section)',
          mech: 'ai', how: 'Decomposed one-dbt-model-per-output-table; refs/sources wired from the catalog + real bronze columns, not guessed.',
          status: statusChip(true, 'generated'),
        }));
      }
    });
    publish.forEach(n => {
      rows.push(row({
        search: n, source: `${code(n)} <span style="color:var(--text-muted);font-size:10px">publish job</span>`,
        target: '<span style="color:var(--text-muted)">nothing — intentionally retired</span>',
        mech: 'retired', how: 'This job replicated to Snowflake. On the target side, Databricks IS the warehouse — the gold layer serves consumers directly.',
        status: statusChip(true, 'retired'),
      }));
    });
    out.push(section('⚙️ Glue jobs', `${ingestion.length} E+L · ${transforms.length} transform · ${publish.length} publish`, rows));
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
          search: t, source: `${code(t)} <span style="color:var(--text-muted);font-size:10px">transform SQL rows</span>`,
          target: `${qcModels.length} dbt models — ${codeList(qcModels, 5)}`,
          mech: 'ai', how: 'Each config row’s SQL rewritten to Databricks dialect as a governed dbt model (scaffold fallback if AI fails); the config-driven design becomes version-controlled code.',
          status: statusChip(qcModels.length > 0, `${qcModels.length} generated` + ((cp.skipped || []).length ? ` · ${cp.skipped.length} skipped` : '')),
        }));
      } else if (t === 'parent_batch_process') {
        rows.push(row({
          search: t, source: `${code(t)} <span style="color:var(--text-muted);font-size:10px">batch lifecycle</span>`,
          target: `${code('control__' + t)} DDL + ${code('fw_batch_open.py')}, ${code('fw_batch_close.py')}`,
          mech: 'det', how: 'Table recreated as Delta; open/close semantics templated into framework notebooks that bracket every Job run.',
          status: statusChip(notebooks.includes('fw_batch_open.py'), 'generated'),
        }));
      } else if (t === 'dq_rules') {
        const dq = cp.dq_summary || {};
        rows.push(row({
          search: t, source: `${code(t)} <span style="color:var(--text-muted);font-size:10px">quality rules</span>`,
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
          search: t, source: `${code(t)} <span style="color:var(--text-muted);font-size:10px">framework table</span>`,
          target: hit ? code(hit) + ' (Delta DDL)' : 'control-schema Delta DDL',
          mech: 'det', how: 'Postgres types mapped to Delta; lands in the control schema so the migrated framework keeps its ledger.',
          status: statusChip(!!hit || ctrlDdl.length > 0, 'generated'),
        }));
      }
    });
    out.push(section('🗄️ Control framework (Postgres → Unity Catalog)',
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
      const dep = deployedByName[(j.dag || {}).name] || deployed.find(d => (d.name || '').includes((j.dag || {}).name || ' '));
      const srcTag = j.source === 'airflow' ? 'Airflow DAG' : 'Glue Workflow';
      rows.push(row({
        search: (j.dag || {}).name || '',
        source: `${code((j.dag || {}).name || 'workflow')} <span style="color:var(--text-muted);font-size:10px">${srcTag}</span>`,
        target: `Databricks Job ${code('sfglue — ' + ((j.dag || {}).name || ''))}${(j.dag || {}).schedule ? ' · Quartz ' + code((j.dag || {}).schedule) : ''}`,
        mech: 'det', how: 'Graph + triggers normalized to a task DAG, emitted as Jobs 2.1 JSON + DAB YAML; deployed idempotently by tag.',
        status: statusChip(!!(dep && dep.success), dep && dep.success ? `deployed · job ${dep.job_id}` : 'planned'),
      }));
      dagTasks.forEach(t => {
        const jt = byKey[t.key] || {};
        let target, mech = 'det', how;
        if (t.kind === 'sensor') {
          target = 'Job file-arrival trigger <span style="color:var(--text-muted)">(task dropped)</span>';
          how = 'Databricks has no in-job sensors — the polling task becomes a native trigger on the Job.';
        } else if (jt.dbt_task) {
          target = `dbt task ${code((jt.dbt_task.commands || ['dbt build']).join(' '))}`;
          how = 'Legacy transform task bound to the converted dbt models via the artifact map.';
        } else if (jt.notebook_task) {
          target = `notebook task ${code(jt.notebook_task.notebook_path || '')}`;
          how = 'Bound to its converted notebook via the auto-derived artifact map.';
        } else {
          target = '<span style="color:var(--text-muted)">placeholder task — map an artifact</span>';
          mech = 'manual';
          how = 'No converted artifact matched this legacy name; assign one in the artifact map.';
        }
        rows.push(row({
          search: `${t.key} ${t.legacy_name}`,
          source: `<span style="color:var(--text-muted)">└</span> ${code(t.legacy_name || t.key)} <span style="color:var(--text-muted);font-size:10px">${esc(t.kind || 'task')}</span>`,
          target, mech, how,
          status: statusChip(mech !== 'manual', mech !== 'manual' ? 'mapped' : 'unmapped'),
        }));
      });
      ((j.dag || {}).warnings || []).forEach(w => {
        rows.push(row({
          search: 'warning', source: '<span style="color:var(--text-muted)">⚠ warning</span>',
          target: `<span style="font-size:11px;color:var(--text-secondary)">${esc(w)}</span>`,
          mech: 'manual', how: '', status: '',
        }));
      });
    });
    out.push(section('🔀 Orchestration (Workflows + Airflow → Databricks Jobs)',
      `${(wf.planned || []).length} pipeline(s) planned · ${deployed.filter(d => d.success).length} deployed`, rows));
  }

  // ── 4. Tables & project scaffolding ───────────────────────────────────────
  {
    const rows = [];
    const dataDdl = ddl.filter(d => !d.startsWith('control__'));
    if (dataDdl.length) {
      rows.push(row({
        search: 'tables ddl', source: `${dataDdl.length} source tables <span style="color:var(--text-muted);font-size:10px">Snowflake / catalog</span>`,
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
    out.push(section('🧱 Tables, contracts & project', 'DDL · dbt project scaffolding · review queue', rows));
  }

  return out.filter(Boolean);
}

// ─── Page ─────────────────────────────────────────────────────────────────────
export function renderSfGlueMapPage(container) {
  const state = store.get();
  const conv = state.sfGlueConversion;

  container.innerHTML = `
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div style="max-width:1180px;margin:0 auto">
        <h2 style="margin:0 0 4px">Migration Map</h2>
        <p style="color:var(--text-secondary);margin:0 0 14px;font-size:13px;line-height:1.6">
          Every part of the source estate, what it became on Databricks, and how it got there —
          <strong>AI conversion</strong> where language changes, <strong>deterministic engines</strong> everywhere else.
        </p>
        ${conv ? `
          <input id="map-filter" placeholder="Filter by name (job, table, task…)" spellcheck="false"
            style="width:100%;max-width:420px;margin-bottom:14px;padding:8px 12px;font-size:12px;
            border:1px solid var(--border);border-radius:8px;background:var(--bg-surface);color:var(--text-primary)">
          <div id="map-sections">${buildSections(state).join('')}</div>
        ` : `
          <div class="card"><div class="card-body" style="font-size:13px;color:var(--text-secondary)">
            No conversion yet — run the <strong>Automated migration</strong> (or Generate conversion in
            Review &amp; Edit) and the full source→target map appears here.
          </div></div>
        `}
      </div>
    </div>`;

  container.querySelector('#map-filter')?.addEventListener('input', (e) => {
    const q = (e.target.value || '').toLowerCase().trim();
    container.querySelectorAll('.map-row').forEach(r => {
      const hay = (r.getAttribute('data-search') || '') + ' ' + r.textContent;
      r.style.display = !q || hay.toLowerCase().includes(q) ? '' : 'none';
    });
  });
}

export function destroySfGlueMapPage() { /* stateless */ }
