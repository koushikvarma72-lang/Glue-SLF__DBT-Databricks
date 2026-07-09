/**
 * Landing / home page for the standalone sfglue app.
 *
 * A full-bleed entry screen (no workflow step-nav) that introduces the tool and
 * explains the pipeline, with one primary CTA into the flow. This is deliberately
 * NOT a registry step — the numbered Connect→…→Report nav only appears once the
 * user enters the workflow. main.js special-cases the 'sfglue-home' route.
 */
import { store } from '../store.js';

// The six workflow stages — this doubles as the "how it works" explanation.
const STAGES = [
  { n: 1, icon: '🔌', title: 'Connect sources',
    body: 'Point at your <strong>Snowflake</strong> account and <strong>AWS Glue</strong> catalog (and optionally an upstream Postgres). Credentials stay in your browser and are sent per-request — nothing is baked in.' },
  { n: 2, icon: '🔗', title: 'Check lineage',
    body: 'We introspect Snowflake tables/views + view SQL and the Glue Data Catalog + ETL job scripts, then build one <strong>source → Snowflake</strong> dataflow graph and flag duplicate tables & overlapping logic.' },
  { n: 3, icon: '⚡', title: 'Review & convert',
    body: 'Pick the tables to migrate and edit the source if needed. AI translates Glue jobs + Snowflake SQL into <strong>dbt models</strong>, <strong>Databricks DDL</strong>, and bronze <strong>PySpark notebooks</strong> — split E+L vs T at the bronze boundary.' },
  { n: 4, icon: '🧱', title: 'Databricks agent',
    body: 'A precheck compares the planned targets to what already exists in Unity Catalog (so nothing is duplicated), then deploys DDL and runs the bronze ingestion into your lakehouse.' },
  { n: 5, icon: '📦', title: 'dbt agent',
    body: 'Reconcile row counts against the source, run dbt tests & contracts, and export a <strong>runnable dbt project</strong> (dbt_project.yml + profiles + sources/schema/unit tests).' },
  { n: 6, icon: '📋', title: 'Report',
    body: 'A migration summary: what was converted, fidelity grade, reconciliation results, and everything you produced — ready to hand off.' },
];

export function renderSfGlueHomePage(container) {
  const s = store.get();
  const connected = !!(s.sfGlueSnowflakeConnection?.success || s.sfGlueGlueConnection?.success);
  const hasLineage = !!s.sfGlueLineage;
  // Resume where they left off if they've already started.
  const primaryTarget = hasLineage ? 'sfglue-review' : (connected ? 'sfglue-lineage' : 'sfglue-connect');
  const primaryLabel = (connected || hasLineage) ? 'Continue where you left off' : 'Get started';

  container.innerHTML = `
    <div class="sfglue-home">
      <div class="sfglue-home-bg" aria-hidden="true"></div>

      <header class="sfglue-home-nav">
        <div class="sfglue-home-brand">
          <span class="sfglue-home-logo" aria-hidden="true">❄️</span>
          <span>sfglue</span>
          <span class="sfglue-home-brand-sub">Snowflake&nbsp;+&nbsp;Glue&nbsp;→&nbsp;Databricks</span>
        </div>
      </header>

      <section class="sfglue-home-hero">
        <div class="sfglue-home-eyebrow">MIGRATION TOOL</div>
        <h1 class="sfglue-home-title">
          Move <span class="hl">Snowflake&nbsp;+&nbsp;AWS&nbsp;Glue</span><br/>
          pipelines to <span class="hl">Databricks&nbsp;&amp;&nbsp;dbt</span>
        </h1>
        <p class="sfglue-home-sub">
          Connect your warehouse and ETL, get a full lineage graph, and let AI convert Glue jobs
          and Snowflake SQL into dbt models, Delta DDL, and PySpark notebooks — with a
          reconciliation gate and a runnable dbt project at the end.
        </p>
        <div class="sfglue-home-cta">
          <button class="btn btn-primary sfglue-home-start" id="home-start">${primaryLabel} <span aria-hidden="true">→</span></button>
          ${(connected || hasLineage)
            ? `<button class="btn btn-outline" id="home-connect">Connect a new source</button>`
            : ''}
        </div>
        <div class="sfglue-home-notes">
          <span>⚙️ AI runs on Amazon Bedrock by default</span>
          <span>·</span>
          <span>🧩 Source-agnostic — works for any Glue + Snowflake flow</span>
        </div>
      </section>

      <section class="sfglue-home-how">
        <div class="sfglue-home-how-head">
          <h2>How it works</h2>
          <p>Six steps from live sources to a deployed, reconciled lakehouse.</p>
        </div>
        <div class="sfglue-home-steps">
          ${STAGES.map(st => `
            <div class="sfglue-home-step">
              <div class="sfglue-home-step-top">
                <span class="sfglue-home-step-icon" aria-hidden="true">${st.icon}</span>
                <span class="sfglue-home-step-n">${st.n}</span>
              </div>
              <div class="sfglue-home-step-title">${st.title}</div>
              <div class="sfglue-home-step-body">${st.body}</div>
            </div>`).join('')}
        </div>
      </section>

      <footer class="sfglue-home-foot">
        <span>Snowflake + AWS Glue → Databricks / dbt</span>
      </footer>
    </div>`;

  container.querySelector('#home-start')?.addEventListener('click', () => store.navigate(primaryTarget));
  container.querySelector('#home-connect')?.addEventListener('click', () => store.navigate('sfglue-connect'));
}

export function destroySfGlueHomePage() { /* no timers/listeners to clean up */ }
