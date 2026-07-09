/**
 * Snowflake + AWS Glue → Databricks/DBT — Migration Report.
 *
 * The capstone for the Snowflake/Glue flow, mirroring the Qlik Migration Report
 * (same scorecard layout: gauge, stat tiles, stage checklist, AI summary, table
 * list, Markdown export) but derived entirely from the Snowflake/Glue session
 * state — lineage, the generated conversion, and the reconciliation result.
 *
 * Everything here is computed from what the rest of the flow already produced;
 * stages that haven't run yet render as "pending" rather than inventing numbers.
 */
import { store } from '../store.js';
import { api } from '../api.js';
import { escapeHtml } from '../utils.js';

let sfGrading = false;      // an accuracy grade is in flight (prevents double-fire)
let sfGradeFailed = false;  // grading failed for the current conversion (stops auto-retry)

// --- Derive the report model from live Snowflake/Glue state ------------------

function buildReport(state) {
  const lineageWrap = state.sfGlueLineage || null;
  const lin = (lineageWrap && lineageWrap.lineage) || {};
  const nodes = lin.nodes || [];
  const dataNodes = nodes.filter(n => n.type !== 'job');
  const jobs = nodes.filter(n => n.type === 'job');
  const edges = lin.edges || [];
  const duplicates = lineageWrap ? (lineageWrap.duplicates || []) : [];

  const tableCount = dataNodes.length;
  const columnCount = dataNodes.reduce((s, n) => s + (Number(n.column_count) || 0), 0);
  const relCount = edges.length;
  const dupCount = duplicates.length;
  const jobCount = jobs.length;

  const conv = state.sfGlueConversion || null;
  const dbtModels = conv && conv.dbt_models ? Object.keys(conv.dbt_models).length : 0;
  const ddlCount = conv && conv.ddl ? Object.keys(conv.ddl).length : 0;
  const notebookCount = conv && conv.notebooks ? Object.keys(conv.notebooks).length : 0;
  const untranslatable = (conv && conv.untranslatable) || [];
  const hasConv = !!conv && (dbtModels + ddlCount + notebookCount) > 0;

  const recon = state.sfGlueReconcile || null;
  const reconResults = (recon && recon.results) || [];
  const reconTotal = reconResults.length;
  const reconPassed = reconResults.filter(r => r.passed).length;
  const reconScore = reconTotal ? Math.round((reconPassed / reconTotal) * 100) : null;

  const sfOk = !!(state.sfGlueSnowflakeConnection && state.sfGlueSnowflakeConnection.success);
  const glueOk = !!(state.sfGlueGlueConnection && state.sfGlueGlueConnection.success);
  const sourcesLabel = [sfOk && 'Snowflake', glueOk && 'AWS Glue'].filter(Boolean).join(' + ') || 'none';

  const dest = state.sfGlueDestination || {};
  const destLabel = dest.catalog ? `Databricks / DBT · ${dest.catalog}` : 'Databricks / DBT';

  const tableRows = dataNodes.map(n => ({
    name: n.display || n.label || '(unnamed)',
    columns: Number(n.column_count) || 0,
    role: n.system || n.type || '—',
  }));

  const stages = [
    { label: 'Sources connected', done: sfOk || glueOk, detail: (sfOk || glueOk) ? sourcesLabel : 'not connected' },
    { label: 'Lineage built', done: !!lineageWrap && tableCount > 0,
      detail: (lineageWrap && tableCount > 0) ? `${tableCount} tables · ${relCount} relationships${dupCount ? ` · ${dupCount} duplicate group${dupCount === 1 ? '' : 's'}` : ''}` : 'not built yet' },
    { label: 'Conversion generated', done: hasConv,
      detail: hasConv ? `${dbtModels} dbt · ${ddlCount} DDL · ${notebookCount} notebook${notebookCount === 1 ? '' : 's'}` : 'not generated yet' },
    { label: 'Verified against source', done: reconTotal > 0 && reconPassed === reconTotal,
      detail: reconTotal ? `${reconPassed}/${reconTotal} table${reconTotal === 1 ? '' : 's'} match source` : 'not run yet' },
  ];
  const doneCount = stages.filter(s => s.done).length;
  const readiness = Math.round((doneCount / stages.length) * 100);

  // AI accuracy grade of converted artifacts vs the original source. Keyed by a
  // conversion signature so a re-conversion invalidates a stale grade.
  const convSig = `${dbtModels}|${ddlCount}|${notebookCount}`;
  const rawGrade = state.sfGlueQualityGrade || null;
  const grade = (rawGrade && rawGrade._sig === convSig) ? rawGrade : null;

  // Headline priority: an LLM is systematically overconfident about its own conversions,
  // so its self-grade must NOT be the ship metric. The headline is the INDEPENDENT gate —
  // execution reconciliation match (proven against the real source) > stage-completion
  // readiness. The AI grade is surfaced separately, explicitly as a triage estimate.
  const headline = reconScore != null
    ? { score: reconScore, label: 'Verification match' }
    : { score: readiness, label: 'Migration readiness' };

  // Independent ship gate: no unresolved review-queue blockers AND the generated dbt
  // tests/contracts pass on the warehouse AND every table verified against source. This —
  // not the AI grade — is what "ready to ship" means.
  const blockersEmpty = untranslatable.length === 0;
  const reconAllPassed = reconTotal > 0 && reconPassed === reconTotal;
  const testRun = state.sfGlueTests || null;
  const testTotal = (testRun && (testRun.results || []).length) || 0;
  const testPassed = testRun ? (testRun.results || []).filter(t => t.passed).length : 0;
  const testsAllPassed = !!testRun && testRun.all_passed;
  const shipGate = {
    ready: blockersEmpty && testsAllPassed && reconAllPassed,
    blockersEmpty, reconAllPassed, testsAllPassed, testTotal, testPassed,
    reason: !blockersEmpty ? `${untranslatable.length} review-queue item(s) unresolved`
      : !testsAllPassed ? (testRun ? `${testTotal - testPassed} dbt test(s) failing` : 'dbt tests/contracts not run yet')
        : !reconAllPassed ? (reconTotal ? `${reconTotal - reconPassed} table(s) not yet verified against source` : 'reconciliation not run yet')
          : 'all gates passed',
  };

  return {
    sourcesLabel, destLabel, tableRows, tableCount, columnCount, relCount, dupCount, jobCount,
    conv, hasConv, dbtModels, ddlCount, notebookCount, untranslatable,
    reconResults, reconTotal, reconPassed, reconScore, stages, headline, grade, convSig, shipGate,
  };
}

function scoreTone(score) {
  if (score >= 90) return { cls: 'success', color: 'var(--success)' };
  if (score >= 60) return { cls: 'warning', color: 'var(--warning)' };
  return { cls: 'error', color: 'var(--error)' };
}

// Build a plain-text digest of the migration for the AI summary endpoint.
function summaryDigest(r) {
  const tags = [...new Set(r.untranslatable.map(u => u.tag).filter(Boolean))];
  return [
    `Snowflake + AWS Glue → ${r.destLabel} migration`,
    `Sources connected: ${r.sourcesLabel}`,
    `Lineage: ${r.tableCount} tables, ${r.columnCount} columns, ${r.relCount} relationships, ${r.dupCount} duplicate group(s), ${r.jobCount} Glue job(s)`,
    `Generated: ${r.dbtModels} dbt models, ${r.ddlCount} Databricks DDL, ${r.notebookCount} bronze notebook(s)`,
    r.untranslatable.length ? `Needs human review: ${r.untranslatable.length} flagged construct(s)${tags.length ? ` (${tags.join(', ')})` : ''}` : 'Needs human review: none flagged',
    r.reconTotal ? `Verification: ${r.reconPassed}/${r.reconTotal} tables match source` : 'Verification: not run',
    '',
    'Tables:',
    ...r.tableRows.map(t => `- ${t.name} (${t.columns} cols, ${t.role})`),
  ].join('\n');
}

// --- Render ------------------------------------------------------------------

export function renderSfGlueReportPage(container) {
  const state = store.get();

  const nothingYet = !state.sfGlueLineage && !state.sfGlueConversion
    && !(state.sfGlueSnowflakeConnection && state.sfGlueSnowflakeConnection.success)
    && !(state.sfGlueGlueConnection && state.sfGlueGlueConnection.success);

  if (nothingYet) {
    container.innerHTML = `
      <div class="page" style="flex:1">
        <div class="empty-state">
          <div class="empty-state-icon" aria-hidden="true">📋</div>
          <div class="empty-state-title">No migration yet</div>
          <div class="empty-state-text">Connect Snowflake/Glue, check lineage and generate a conversion — your migration report will appear here.</div>
          <button class="btn btn-primary btn-lg" id="sfreport-go-connect" style="margin-top:var(--space-lg)">Connect sources</button>
        </div>
      </div>`;
    document.getElementById('sfreport-go-connect')?.addEventListener('click', () => store.navigate('sfglue-connect'));
    return;
  }

  const r = buildReport(state);
  const tone = scoreTone(r.headline.score);
  const C = 2 * Math.PI * 52; // gauge circumference (r=52)
  const dash = (r.headline.score / 100) * C;

  container.innerHTML = `
    <style>
      .report-wrap { flex:1; overflow-y:auto; padding:28px 32px; }
      .report-grid { max-width:1100px; margin:0 auto; display:flex; flex-direction:column; gap:20px; }
      .report-head { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
      .report-title { font-size:var(--text-2xl); font-weight:700; color:var(--text-primary); }
      .report-sub { font-size:var(--text-sm); color:var(--text-muted); margin-top:2px; }
      .report-hero { display:flex; gap:24px; align-items:center; }
      .gauge { flex-shrink:0; position:relative; width:128px; height:128px; }
      .gauge-num { position:absolute; inset:0; display:flex; flex-direction:column; align-items:center; justify-content:center; }
      .gauge-num b { font-size:30px; font-weight:800; line-height:1; color:${tone.color}; }
      .gauge-num span { font-size:10px; color:var(--text-muted); margin-top:4px; text-align:center; max-width:90px; }
      .report-stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:12px; flex:1; }
      .stat { background:var(--bg-surface); border:1px solid var(--border); border-radius:var(--radius-lg); padding:14px 16px; }
      .stat b { display:block; font-size:24px; font-weight:700; color:var(--text-primary); line-height:1.1; }
      .stat span { font-size:11px; color:var(--text-muted); }
      .report-stages { display:flex; flex-direction:column; gap:8px; }
      .stage-row { display:flex; align-items:center; gap:10px; font-size:13px; }
      .stage-dot { width:18px; height:18px; border-radius:50%; flex-shrink:0; display:flex; align-items:center; justify-content:center; font-size:11px; color:#fff; }
      .stage-detail { color:var(--text-muted); font-size:12px; margin-left:auto; }
      .report-card { background:var(--bg-primary); border:1px solid var(--border); border-radius:var(--radius-lg); padding:18px 20px; }
      .report-card h3 { font-size:13px; font-weight:700; color:var(--text-secondary); margin-bottom:12px; display:flex; align-items:center; justify-content:space-between; gap:8px; }
      .rtable { width:100%; border-collapse:collapse; font-size:12px; }
      .rtable th { text-align:left; color:var(--text-muted); font-weight:600; padding:6px 8px; border-bottom:1px solid var(--border); }
      .rtable td { padding:6px 8px; border-bottom:1px solid var(--border); color:var(--text-secondary); }
      .rtable tr:last-child td { border-bottom:none; }
      .ai-summary { font-size:13px; line-height:1.6; color:var(--text-secondary); white-space:pre-wrap; }
      .ai-summary.placeholder { color:var(--text-dim); }
      .grade-dims { display:flex; flex-direction:column; gap:12px; }
      .grade-dim-head { display:flex; justify-content:space-between; font-size:12px; color:var(--text-secondary); margin-bottom:4px; }
      .grade-dim-head b { color:var(--text-primary); }
      .grade-bar { height:7px; border-radius:var(--radius-full); background:var(--bg-active); overflow:hidden; }
      .grade-bar span { display:block; height:100%; border-radius:var(--radius-full); }
      .grade-note { font-size:11px; color:var(--text-muted); margin-top:3px; }
    </style>
    <div class="report-wrap">
      <div class="report-grid">
        <div class="report-head">
          <div>
            <div class="report-title">Migration Report</div>
            <div class="report-sub">Snowflake + AWS Glue → ${escapeHtml(r.destLabel)} · sources: ${escapeHtml(r.sourcesLabel)}</div>
          </div>
          <div style="display:flex;gap:8px">
            <button class="btn btn-secondary" id="sfreport-download">⬇ Download report (.md)</button>
          </div>
        </div>

        <div class="report-card">
          <div class="report-hero">
            <div class="gauge" role="img" aria-label="${r.headline.label}: ${r.headline.score} percent">
              <svg width="128" height="128" viewBox="0 0 128 128">
                <circle cx="64" cy="64" r="52" fill="none" stroke="var(--border)" stroke-width="12"/>
                <circle cx="64" cy="64" r="52" fill="none" stroke="${tone.color}" stroke-width="12" stroke-linecap="round"
                  stroke-dasharray="${dash.toFixed(1)} ${(C - dash).toFixed(1)}" transform="rotate(-90 64 64)"/>
              </svg>
              <div class="gauge-num"><b>${r.headline.score}%</b><span>${r.headline.label}</span></div>
            </div>
            <div class="report-stats">
              <div class="stat"><b>${r.tableCount}</b><span>tables</span></div>
              <div class="stat"><b>${r.columnCount}</b><span>columns</span></div>
              <div class="stat"><b>${r.relCount}</b><span>relationships</span></div>
              <div class="stat"><b>${r.jobCount}</b><span>Glue jobs</span></div>
              <div class="stat"><b>${r.hasConv ? r.dbtModels : '—'}</b><span>dbt models</span></div>
              <div class="stat"><b>${r.untranslatable.length}</b><span>review-flagged</span></div>
            </div>
          </div>
          <div class="report-stages" style="margin-top:18px">
            ${r.stages.map(s => `
              <div class="stage-row">
                <span class="stage-dot" style="background:${s.done ? 'var(--success)' : 'var(--text-dim)'}">${s.done ? '✓' : '·'}</span>
                <span>${escapeHtml(s.label)}</span>
                <span class="stage-detail">${escapeHtml(s.detail)}</span>
              </div>`).join('')}
          </div>
        </div>

        <div class="report-card" style="border-left:4px solid ${r.shipGate.ready ? 'var(--success)' : 'var(--warning)'}">
          <h3>${r.shipGate.ready ? '✅' : '⛔'} Ship gate <span style="font-weight:400;font-size:var(--text-sm);color:var(--text-muted)">— independent of the AI grade</span></h3>
          <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:6px">
            <div class="stage-row"><span class="stage-dot" style="background:${r.shipGate.blockersEmpty ? 'var(--success)' : 'var(--text-dim)'}">${r.shipGate.blockersEmpty ? '✓' : '·'}</span><span>No unresolved review-queue blockers</span></div>
            <div class="stage-row"><span class="stage-dot" style="background:${r.shipGate.testsAllPassed ? 'var(--success)' : 'var(--text-dim)'}">${r.shipGate.testsAllPassed ? '✓' : '·'}</span><span>dbt tests + contracts pass${r.shipGate.testTotal ? ` (${r.shipGate.testPassed}/${r.shipGate.testTotal})` : ''}</span></div>
            <div class="stage-row"><span class="stage-dot" style="background:${r.shipGate.reconAllPassed ? 'var(--success)' : 'var(--text-dim)'}">${r.shipGate.reconAllPassed ? '✓' : '·'}</span><span>Verified against source (reconciliation)</span></div>
          </div>
          <div class="ai-summary" style="margin-top:10px">${r.shipGate.ready
            ? 'Ready to ship: every model matched its source and the review queue is clear. Run dbt tests + contracts on the warehouse as the final build-time gate.'
            : `Not ready: ${escapeHtml(r.shipGate.reason)}. The AI fidelity grade below is a triage signal only — it is <strong>not</strong> a ship criterion.`}</div>
        </div>

        <div class="report-card">
          <h3>🤖 AI fidelity grade <span style="font-weight:400;font-size:var(--text-sm);color:var(--text-muted)">— triage estimate, not a ship criterion</span>
            <button class="btn btn-secondary btn-sm" id="sfreport-regrade" ${r.hasConv ? '' : 'disabled'}>${r.grade ? '↻ Re-score' : 'Score'}</button>
          </h3>
          <div class="ai-summary" style="font-size:var(--text-xs);color:var(--text-muted);margin-bottom:8px">LLMs are systematically overconfident about their own conversions — use this to <em>prioritise</em> review, never to decide what ships. The ship gate above is the real verdict.</div>
          ${r.grade ? `
            <div class="grade-dims">
              ${(r.grade.dimensions || []).map(d => `
                <div class="grade-dim">
                  <div class="grade-dim-head"><span>${escapeHtml(d.name)}</span><b>${d.score}%</b></div>
                  <div class="grade-bar"><span style="width:${d.score}%;background:${scoreTone(d.score).color}"></span></div>
                  ${d.note ? `<div class="grade-note">${escapeHtml(d.note)}</div>` : ''}
                </div>`).join('')}
            </div>
            ${r.grade.summary ? `<div class="ai-summary" style="margin-top:12px">${escapeHtml(r.grade.summary)}</div>` : ''}
          ` : `<div class="ai-summary placeholder" id="sfreport-quality-note">${r.hasConv ? 'Scoring the converted artifacts against the original Snowflake views + Glue scripts…' : 'Generate a conversion first, then it can be scored against the source.'}</div>`}
        </div>

        <div class="report-card">
          <h3>✨ AI summary
            <button class="btn btn-secondary btn-sm" id="sfreport-summarize" ${r.hasConv ? '' : 'disabled'}>Summarize this migration</button>
          </h3>
          <div class="ai-summary placeholder" id="sfreport-summary-body">${r.hasConv ? 'Click “Summarize this migration” for a plain-English overview of what was migrated and how.' : 'Generate a conversion first, then an AI summary can be produced.'}</div>
        </div>

        <div class="report-card">
          <h3>Tables (${r.tableCount})</h3>
          <table class="rtable">
            <thead><tr><th>Table</th><th>Columns</th><th>System</th></tr></thead>
            <tbody>
              ${r.tableRows.map(t => `
                <tr><td>${escapeHtml(t.name)}</td><td>${t.columns || '—'}</td><td>${escapeHtml(t.role)}</td></tr>`).join('')
                || '<tr><td colspan="3" style="color:var(--text-dim)">No tables in lineage yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    </div>`;

  // -- Handlers --
  document.getElementById('sfreport-download')?.addEventListener('click', () => downloadMarkdown(state, r));

  // Accuracy grade: re-score on demand, auto-score once when missing/stale.
  document.getElementById('sfreport-regrade')?.addEventListener('click', () => {
    sfGradeFailed = false; sfGrading = false;
    store.set({ sfGlueQualityGrade: null }); // re-render → auto-grade fires
  });
  if (r.hasConv && !r.grade && !sfGrading && !sfGradeFailed) {
    sfGrading = true;
    gradeSfGlueQuality(state, r.convSig);
  }

  document.getElementById('sfreport-summarize')?.addEventListener('click', async (e) => {
    const btn = e.currentTarget;
    const body = document.getElementById('sfreport-summary-body');
    if (!body) return;
    btn.disabled = true;
    const original = btn.textContent;
    btn.textContent = 'Summarizing…';
    body.classList.remove('placeholder');
    body.textContent = 'Thinking…';
    try {
      const res = await api.explainSnowflakeGlueArtifact({
        name: 'Migration overview',
        code: summaryDigest(r),
        kind: 'migration summary',
        glue: store.get().sfGlueGlueConfig || {},
      });
      const text = res.text || res.description || res.explanation || (typeof res === 'string' ? res : '');
      body.textContent = text || 'No summary returned.';
    } catch (err) {
      body.classList.add('placeholder');
      body.textContent = `Could not generate summary: ${err.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = original;
    }
  });
}

export function destroySfGlueReportPage() { /* no persistent listeners/timers to clean up */ }

// --- Accuracy grade (converted artifacts vs original source) ------------------

async function gradeSfGlueQuality(state, convSig) {
  try {
    const review = state.sfGlueReview || {};
    const conv = state.sfGlueConversion || {};
    const grade = await api.gradeSfGlue({
      glue: state.sfGlueGlueConfig || {},   // reuse the Glue connection's AWS creds for Bedrock
      // Source tables + relationships are the ground truth the models were derived from
      // when the source has no views (all logic in Glue) — without them the grader sees
      // "no source material" and floors completeness/correctness.
      tables: review.tables || [],
      relationships: review.relationships || [],
      views: review.views || [],
      glue_jobs: review.glue_jobs || [],
      business_logic: review.business_logic || '',
      dbt_models: conv.dbt_models || {},
      notebooks: conv.notebooks || {},
      ddl: conv.ddl || {},
      dialect: 'databricks',
    });
    grade._sig = convSig; // tie the grade to this conversion
    sfGrading = false;
    if (store.get().currentPage === 'sfglue-report') store.set({ sfGlueQualityGrade: grade });
  } catch (err) {
    sfGrading = false;
    sfGradeFailed = true;
    const note = document.getElementById('sfreport-quality-note');
    if (note) { note.classList.add('placeholder'); note.textContent = `Couldn't score accuracy: ${err.message}`; }
  }
}

// --- Markdown export ---------------------------------------------------------

function downloadMarkdown(state, r) {
  const tags = [...new Set(r.untranslatable.map(u => u.tag).filter(Boolean))];
  const lines = [
    `# Migration Report — Snowflake + AWS Glue`,
    ``,
    `**Sources:** ${r.sourcesLabel}  `,
    `**Target:** ${r.destLabel}  `,
    ``,
    `## Scorecard`,
    `- **Ship gate (independent of AI grade):** ${r.shipGate.ready ? '✅ READY' : `⛔ NOT READY — ${r.shipGate.reason}`}`,
    `- **${r.headline.label}:** ${r.headline.score}%`,
    `- **Tables:** ${r.tableCount}`,
    `- **Columns:** ${r.columnCount}`,
    `- **Relationships:** ${r.relCount}`,
    `- **Duplicate groups:** ${r.dupCount}`,
    `- **Glue jobs:** ${r.jobCount}`,
    `- **dbt models:** ${r.hasConv ? r.dbtModels : 'not generated'}`,
    `- **Databricks DDL:** ${r.hasConv ? r.ddlCount : 'not generated'}`,
    `- **Bronze notebooks:** ${r.hasConv ? r.notebookCount : 'not generated'}`,
    `- **Flagged for human review:** ${r.untranslatable.length}${tags.length ? ` (${tags.join(', ')})` : ''}`,
    ``,
    `## Stages`,
    ...r.stages.map(s => `- [${s.done ? 'x' : ' '}] ${s.label} — ${s.detail}`),
    ``,
    `## Tables`,
    `| Table | Columns | System |`,
    `| --- | --- | --- |`,
    ...r.tableRows.map(t => `| ${t.name} | ${t.columns} | ${t.role} |`),
    ``,
  ];
  if (r.grade) {
    lines.push(`## AI fidelity grade (triage estimate — not a ship criterion)`, `- **Overall:** ${r.grade.overall}%`);
    (r.grade.dimensions || []).forEach(d => lines.push(`- ${d.name}: ${d.score}%${d.note ? ` — ${d.note}` : ''}`));
    if (r.grade.summary) lines.push('', r.grade.summary);
    lines.push('');
  }
  if (r.reconTotal) {
    lines.push(`## Reconciliation`, `- ${r.reconPassed}/${r.reconTotal} tables match source`, '');
  }
  const blob = new Blob([lines.filter(l => l !== undefined).join('\n')], { type: 'text/markdown' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `migration-report-snowflake-glue.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
