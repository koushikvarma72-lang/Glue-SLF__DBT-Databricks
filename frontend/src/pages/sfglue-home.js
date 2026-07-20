/**
 * Landing / home page for the standalone sfglue app.
 *
 * A minimal entry screen (no workflow step-nav): what the tool does, one CTA,
 * and the six steps in a sentence each. Deliberately NOT a registry step — the
 * numbered Connect→…→Report nav only appears once the user enters the workflow.
 * main.js special-cases the 'sfglue-home' route.
 */
import { store } from '../store.js';

// The six workflow stages — one sentence each.
const STAGES = [
  { n: 1, title: 'Connect sources',
    body: 'Snowflake + AWS Glue (optionally Postgres). Credentials are sent per-request, never stored.' },
  { n: 2, title: 'Check lineage',
    body: 'One source → Snowflake dataflow graph, with tables that live in both systems flagged.' },
  { n: 3, title: 'Review & convert',
    body: 'Pick tables; AI translates Glue jobs + Snowflake SQL into dbt models, Delta DDL and bronze notebooks.' },
  { n: 4, title: 'Databricks agent',
    body: 'Precheck against Unity Catalog, deploy the DDL, run the bronze load.' },
  { n: 5, title: 'dbt agent',
    body: 'Verify every table against the source, run dbt tests, export the runnable dbt project.' },
  { n: 6, title: 'Report',
    body: 'What moved, whether it matches, and the ship-gate verdict — ready to hand off.' },
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
          <span>sfglue</span>
          <span class="sfglue-home-brand-sub">Snowflake&nbsp;+&nbsp;Glue&nbsp;→&nbsp;Databricks</span>
        </div>
      </header>

      <section class="sfglue-home-hero">
        <h1 class="sfglue-home-title">
          Move <span class="hl">Snowflake&nbsp;+&nbsp;AWS&nbsp;Glue</span><br/>
          pipelines to <span class="hl">Databricks&nbsp;&amp;&nbsp;dbt</span>
        </h1>
        <p class="sfglue-home-sub">
          AI converts the Glue jobs and Snowflake SQL; a reconciliation gate proves the
          result matches the source before anything ships.
        </p>
        <div class="sfglue-home-cta">
          <button class="btn btn-primary sfglue-home-start" id="home-start">${primaryLabel} →</button>
          ${(connected || hasLineage)
            ? `<button class="btn btn-outline" id="home-connect">Connect a new source</button>`
            : ''}
        </div>
      </section>

      <section class="sfglue-home-how">
        <div class="sfglue-home-how-head">
          <h2>How it works</h2>
        </div>
        <div class="sfglue-home-steps">
          ${STAGES.map(st => `
            <div class="sfglue-home-step">
              <div class="sfglue-home-step-top">
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
