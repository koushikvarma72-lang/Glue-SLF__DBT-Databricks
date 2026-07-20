/**
 * Snowflake/Glue → Databricks/DBT — Step 1: Connect sources.
 *
 * Collects Snowflake and AWS Glue connection details, tests each, and lets the
 * user continue to the Lineage step once at least one source is connected.
 * Connection configs are cached in localStorage by the store (incl. secrets).
 */
import { api } from '../api.js';
import { store } from '../store.js';
import { field, selectField, esc } from '../components/ui.js';
import { notify } from '../components/notify.js';
import { promptModal } from '../components/modal.js';

// Turn a raw STS/IAM ARN into a short, readable principal. An SSO login looks like
// `arn:aws:sts::<acct>:assumed-role/<RoleName>/<session>` (session = the user's
// email); IAM users look like `arn:aws:iam::<acct>:user/<name>`. We show the human
// part + account rather than the full ARN.
function friendlyIdentity(id) {
  if (!id) return '';
  if (id.user) return id.user;
  const arn = id.arn || '';
  const account = id.account || (arn.split(':')[4] || '');
  const resource = arn.split(':')[5] || '';            // e.g. assumed-role/Role/Session
  const segs = resource.split('/').filter(Boolean);
  let name = '';
  if (segs[0] === 'assumed-role') name = segs[segs.length - 1] || segs[1] || '';
  else if (segs.length) name = segs[segs.length - 1];
  const parts = [name || arn];
  if (account) parts.push(`acct ${account}`);
  if (id.region) parts.push(id.region);
  return parts.filter(Boolean).join(' · ');
}

// Which source panel is open (survives store re-renders, e.g. after a test).
let activeConnectTab = 'snowflake';
// Postgres is an optional external source, hidden until the operator opts in.
let pgRevealed = false;

function statusBadge(conn) {
  if (!conn) return '';
  if (conn.success) {
    const who = friendlyIdentity(conn.identity);
    const full = (conn.identity && conn.identity.arn) || '';
    return `<div class="badge badge-success" role="status" aria-live="polite" title="${esc(full).replace(/"/g, '&quot;')}" style="display:inline-flex;align-items:center;gap:6px;font-size:11px;margin-top:4px">Connected${who ? ` — ${esc(who)}` : ''}</div>`;
  }
  return `<div class="badge badge-error" role="alert" style="display:block;font-size:11px;margin-top:4px;white-space:normal;word-break:break-word">${esc(conn.error || 'Connection failed')}</div>`;
}

export function renderSfGlueConnectPage(container) {
  const state = store.get();
  const sf = state.sfGlueSnowflakeConfig || {};
  const glue = state.sfGlueGlueConfig || {};
  const sfConn = state.sfGlueSnowflakeConnection;
  const glueConn = state.sfGlueGlueConnection;
  const sfWarehouses = (sfConn && sfConn.warehouses) || [];
  const sfDatabases = (sfConn && sfConn.databases) || [];
  const sfSchemas = state.sfGlueSnowflakeSchemas || [];
  const loadingSchemas = !!state.sfGlueLoadingSchemas;
  const schemasError = state.sfGlueSchemasError || '';
  const canContinue = (sfConn && sfConn.success) || (glueConn && glueConn.success);

  const sfOk = !!(sfConn && sfConn.success);
  const glueOk = !!(glueConn && glueConn.success);

  // Postgres (external origin) — some pipelines land data in Postgres and ship it to
  // Snowflake; we can redirect the migrated bronze ingestion to read Postgres live.
  const pg = state.sfGluePostgresConfig || {};
  const pgConn = state.sfGluePostgresConnection;
  const pgOk = !!(pgConn && pgConn.success);
  // Keep Connect to the two primary sources by default; Postgres appears once the operator
  // adds it, or automatically when one is already configured. (Per-pipeline evidence-gating
  // isn't possible here — lineage doesn't exist until the next step.)
  const showPostgresConnect = pgRevealed || !!(pg.host || pgOk);
  // A test was attempted on both sources but neither connected — name the cause so the
  // disabled Continue button doesn't read as "you haven't tried yet".
  const bothFailed = !canContinue && sfConn && glueConn;
  const continueHint = bothFailed
    ? 'Both tests failed — fix credentials and re-test to continue.'
    : 'Connect Snowflake and/or AWS Glue to continue.';

  // ── Sidebar artwork — sources flowing into the lakehouse (theme colors only).
  // Pure decoration: two source nodes converge through an animated pipeline into
  // the medallion layers. Uses CSS vars so it follows the app theme exactly.
  const sidebarArt = `
    <style>
      @keyframes sfglue-flow { to { stroke-dashoffset: -26; } }
      @keyframes sfglue-pulse { 0%,100% { opacity:.35 } 50% { opacity:.9 } }
    </style>
    <svg viewBox="0 0 220 330" role="img" aria-label="Sources flowing into the Databricks lakehouse"
         style="width:100%;max-width:210px;height:auto;display:block;margin:0 auto">
      <!-- soft backdrop glow -->
      <circle cx="110" cy="180" r="86" fill="var(--primary-glow)"/>
      <!-- source nodes -->
      <g>
        <circle cx="58" cy="42" r="26" fill="var(--primary-soft)" stroke="var(--border)"/>
        <text x="58" y="47" text-anchor="middle" font-size="11" font-weight="600" fill="var(--text-secondary)">SF</text>
        <text x="58" y="86" text-anchor="middle" font-size="10" fill="var(--text-muted)">Snowflake</text>
        <circle cx="162" cy="42" r="26" fill="var(--primary-soft)" stroke="var(--border)"/>
        <text x="162" y="47" text-anchor="middle" font-size="11" font-weight="600" fill="var(--text-secondary)">Glue</text>
        <text x="162" y="86" text-anchor="middle" font-size="10" fill="var(--text-muted)">AWS Glue</text>
      </g>
      <!-- converging animated flows -->
      <path d="M58 95 C 58 130, 100 130, 108 160" fill="none" stroke="var(--primary)" stroke-width="2"
            stroke-dasharray="5 8" stroke-linecap="round" style="animation:sfglue-flow 1.6s linear infinite"/>
      <path d="M162 95 C 162 130, 120 130, 112 160" fill="none" stroke="var(--accent)" stroke-width="2"
            stroke-dasharray="5 8" stroke-linecap="round" style="animation:sfglue-flow 1.6s linear infinite"/>
      <!-- pipeline node -->
      <circle cx="110" cy="170" r="10" fill="var(--primary)" style="animation:sfglue-pulse 2.4s ease-in-out infinite"/>
      <circle cx="110" cy="170" r="4.5" fill="#fff" opacity=".85"/>
      <path d="M110 182 L110 208" stroke="var(--primary)" stroke-width="2" stroke-dasharray="5 8"
            stroke-linecap="round" style="animation:sfglue-flow 1.6s linear infinite"/>
      <!-- medallion lakehouse layers -->
      <g font-size="10">
        <rect x="45" y="212" width="130" height="26" rx="8" fill="var(--primary-soft)" stroke="var(--border)"/>
        <text x="110" y="229" text-anchor="middle" fill="var(--text-secondary)">Bronze — raw</text>
        <rect x="55" y="244" width="110" height="26" rx="8" fill="var(--accent-glow)" stroke="var(--border)"/>
        <text x="110" y="261" text-anchor="middle" fill="var(--text-secondary)">Silver — curated</text>
        <rect x="65" y="276" width="90" height="26" rx="8" fill="var(--primary-glow)" stroke="var(--primary)"/>
        <text x="110" y="293" text-anchor="middle" fill="var(--text-primary)" font-weight="600">Gold — marts</text>
      </g>
      <text x="110" y="322" text-anchor="middle" font-size="10.5" fill="var(--text-dim)">Databricks / dbt lakehouse</text>
    </svg>`;

  // ── Source tiles (one panel visible at a time) ──────────────────────────────
  const tiles = [
    { id: 'snowflake', icon: '', name: 'Snowflake', ok: sfOk, note: '' },
    { id: 'glue',      icon: '', name: 'AWS Glue',  ok: glueOk, note: '' },
    ...(showPostgresConnect ? [{ id: 'postgres', icon: '', name: 'PostgreSQL', ok: pgOk, note: '(Optional)' }] : []),
  ];
  if (!tiles.some(t => t.id === activeConnectTab)) activeConnectTab = 'snowflake';
  const tileRow = tiles.map(t => {
    const sel = t.id === activeConnectTab;
    return `
      <button type="button" class="src-tile" data-tab="${t.id}" aria-pressed="${sel}"
        style="position:relative;flex:1;min-width:150px;display:flex;align-items:center;justify-content:center;gap:8px;padding:12px 14px;
               border-radius:10px;border:1px solid ${sel ? 'var(--primary)' : 'var(--border)'};
               background:${sel ? 'var(--primary-soft)' : 'transparent'};cursor:pointer;transition:border-color .15s, background .15s">
        <span style="font-size:14px;font-weight:600;color:var(--text-primary)">${t.name}${t.note ? ` <span style="font-weight:400;color:var(--text-muted)">${t.note}</span>` : ''}</span>
        ${t.ok ? '<span aria-hidden="true" style="color:var(--success);font-size:13px">✓</span>' : ''}
      </button>`;
  }).join('');

  container.innerHTML = `
    <div class="page" id="sfglue-connect-page">
      <!-- Sidebar -->
      <div class="sidebar animate-slide-left">
        <div class="sidebar-section" style="text-align:center">
          <div class="sidebar-section-title">Migration</div>
          <div style="display:flex;align-items:center;justify-content:center;gap:8px;font-size:13px;font-weight:600;color:var(--text-primary);padding:2px 0;flex-wrap:wrap">
                        <span>Snowflake + Glue</span>
            <span style="color:var(--text-dim)">→</span>
                        <span>Databricks / dbt</span>
          </div>
        </div>

        <div style="flex:1;display:flex;align-items:center;justify-content:center;padding:18px 16px;min-height:0">
          ${sidebarArt}
        </div>

        <div class="sidebar-section" style="border-top:1px solid var(--border);border-bottom:none;margin-top:auto;padding-bottom:16px">
          <button class="btn btn-outline btn-block" id="sfglue-exit" style="color:var(--text-dim);border-color:var(--border);width:100%;font-size:12px">
            ← Back to home
          </button>
        </div>
      </div>

      <!-- Main -->
      <div class="upload-content">
        <div class="upload-main-area" style="overflow:auto;padding:28px 32px">
          <div style="max-width:1000px;margin:0 auto">
            <h2 style="margin:0 0 4px;font-size:20px">Connect your sources</h2>
            <p class="sfg-lead" style="margin-bottom:22px">
              Connect at least one source — its tables, views and ETL scripts feed the lineage step.
            </p>

            <!-- Source tiles — pick which connector to configure -->
            <div style="display:flex;gap:14px;margin-bottom:20px;flex-wrap:wrap;align-items:center">
              ${tileRow}
              ${showPostgresConnect ? '' : '<button type="button" id="pg-reveal" class="btn btn-ghost" style="font-size:12px;color:var(--text-muted)">+ PostgreSQL source</button>'}
            </div>

            <!-- Snowflake panel -->
            <div class="card connect-panel" data-panel="snowflake" style="${activeConnectTab === 'snowflake' ? '' : 'display:none'}">
              <div class="card-header">
                <div class="card-title">Snowflake connection details</div>
                ${sfOk ? '<span class="badge badge-success" role="status" style="margin-left:auto;font-size:11px">Connected</span>' : ''}
              </div>
              <div class="card-body">
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${field('sf-account', 'Account identifier', sf.account, { placeholder: 'ab12345.us-east-1' })}
                  ${field('sf-user', 'Username', sf.user, { placeholder: 'SVC_MIGRATION' })}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${field('sf-password', 'Password', sf.password, { type: 'password', placeholder: sf.password ? '•••••• (saved)' : '' })}
                  ${field('sf-role', 'Role (optional)', sf.role, { placeholder: 'SYSADMIN' })}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${sfWarehouses.length
                    ? selectField('sf-warehouse', 'Warehouse', sf.warehouse, sfWarehouses, { hint: `${sfWarehouses.length} loaded` })
                    : field('sf-warehouse', 'Warehouse', sf.warehouse, { placeholder: 'WH_XS' })}
                  ${sfDatabases.length
                    ? selectField('sf-database', 'Database', sf.database, sfDatabases, { hint: `${sfDatabases.length} loaded` })
                    : field('sf-database', 'Database', sf.database, { placeholder: 'Connect first to load databases' })}
                </div>
                ${loadingSchemas
                  ? field('sf-schema', 'Schema (optional)', '', { placeholder: 'Loading schemas…', hint: 'Loading schemas…' })
                  : sfSchemas.length
                  ? selectField('sf-schema', 'Schema (optional)', sf.schema, sfSchemas, { blankLabel: '(all schemas)', hint: `${sfSchemas.length} schema(s) — pick one (recommended for large DBs) or all` })
                  : field('sf-schema', 'Schema (optional)', sf.schema, { hint: schemasError ? 'Could not load schemas — type one manually.' : 'Blank = all schemas. Select a database to load its schemas.' })}
                <div style="display:flex;align-items:center;gap:10px;margin-top:12px;flex-wrap:wrap">
                  <button class="btn btn-primary" id="sf-test" ${state.isTestingSnowflake ? 'disabled' : ''}>
                    ${state.isTestingSnowflake ? 'Testing…' : 'Test connection'}
                  </button>
                  <div style="flex:1;min-width:0">${statusBadge(sfConn)}</div>
                  <button class="btn btn-secondary" data-next-tab="glue">Next: AWS Glue →</button>
                </div>
              </div>
            </div>

            <!-- AWS Glue panel -->
            <div class="card connect-panel" data-panel="glue" style="${activeConnectTab === 'glue' ? '' : 'display:none'}">
              <div class="card-header">
                <div class="card-title">AWS Glue connection details</div>
                ${glueOk ? '<span class="badge badge-success" role="status" style="margin-left:auto;font-size:11px">Connected</span>' : ''}
              </div>
              <div class="card-body">
                  <!-- "Sign in with AWS" — Identity Center device flow; fills the key
                       fields below with short-lived role credentials. -->
                  <button class="btn btn-secondary" id="glue-sso-login" style="width:100%;justify-content:center;margin-bottom:4px">
                    Sign in with AWS SSO
                  </button>
                  <div id="glue-sso-status" role="status" style="font-size:11.5px;color:var(--text-muted);margin-bottom:8px"></div>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                    ${field('glue-region', 'Region', glue.region, { placeholder: 'us-east-1' })}
                    ${field('glue-profile', 'AWS profile (optional)', glue.profile_name, { hint: 'Use a named profile, or the access keys below.' })}
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                    ${field('glue-access-key', 'Access key ID', glue.access_key_id, { placeholder: 'AKIA…' })}
                    ${field('glue-secret-key', 'Secret access key', glue.secret_access_key, { type: 'password', placeholder: glue.secret_access_key ? '•••••• (saved)' : '' })}
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                    ${field('glue-session-token', 'Session token (optional)', glue.session_token, { type: 'password', placeholder: glue.session_token ? '•••••• (saved)' : '' })}
                    ${field('glue-catalog-id', 'Catalog ID (optional)', glue.catalog_id, { hint: 'AWS account id of the Glue catalog, if not the caller account.' })}
                  </div>
                  <div style="display:flex;align-items:center;gap:10px;margin-top:12px;flex-wrap:wrap">
                    <button class="btn btn-primary" id="glue-test" ${state.isTestingGlue ? 'disabled' : ''}>
                      ${state.isTestingGlue ? 'Testing…' : 'Test connection'}
                    </button>
                    <div style="flex:1;min-width:0">${statusBadge(glueConn)}</div>
                    ${showPostgresConnect ? '<button class="btn btn-secondary" data-next-tab="postgres">Next: PostgreSQL →</button>' : ''}
                  </div>
                  ${glueOk ? `
                  <div style="display:flex;gap:8px;align-items:flex-end;margin-top:10px">
                    <div style="flex:1">${selectField('glue-bucket', 'Pipeline bucket', state.sfGlueSelectedBucket, state.sfGlueBuckets || [], { hint: 'The S3 bucket the reference pipeline should use (zones + repoint target).', blankLabel: state.sfGlueBuckets ? '\u2014 choose bucket \u2014' : 'loading\u2026' })}</div>
                    <button class="btn btn-secondary" id="glue-buckets-refresh" title="Reload bucket list" style="margin-bottom:10px;padding:6px 10px">\u21bb</button>
                  </div>` : ''}
              </div>
            </div>

            ${showPostgresConnect ? `
            <!-- Postgres panel (external origin — optional). Field IDs / handlers unchanged. -->
            <div class="card connect-panel" data-panel="postgres" style="${activeConnectTab === 'postgres' ? '' : 'display:none'}">
              <div class="card-header">
                <div class="card-title">PostgreSQL connection details <span style="font-size:11px;color:var(--text-muted);font-weight:400">(external source — optional)</span></div>
                ${pgOk ? '<span class="badge badge-success" role="status" style="margin-left:auto;font-size:11px">Connected</span>' : ''}
              </div>
              <div class="card-body">
                <div style="display:grid;grid-template-columns:2fr 1fr;gap:10px">
                  ${field('pg-host', 'Host', pg.host, { placeholder: 'db.internal.example.com' })}
                  ${field('pg-port', 'Port', pg.port || '5432', { placeholder: '5432' })}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${field('pg-database', 'Database', pg.database, { placeholder: 'app_prod' })}
                  ${field('pg-schema', 'Schema (optional)', pg.schema, { hint: 'Blank = all non-system schemas.' })}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${field('pg-user', 'User', pg.user, { placeholder: 'readonly' })}
                  ${field('pg-password', 'Password', pg.password, { type: 'password', placeholder: pg.password ? '•••••• (saved)' : '' })}
                </div>
                <div style="display:flex;align-items:center;gap:10px;margin-top:12px;flex-wrap:wrap">
                  <button class="btn btn-primary" id="pg-test" ${state.isTestingPostgres ? 'disabled' : ''}>
                    ${state.isTestingPostgres ? 'Testing…' : 'Test connection'}
                  </button>
                  <div style="flex:1;min-width:0">${statusBadge(pgConn)}</div>
                </div>
              </div>
            </div>` : ''}

          </div>
        </div>

        <!-- Bottom Bar -->
        <div class="review-footer">
          <div style="display:flex;align-items:center;gap:8px">
            ${sfOk ? '<span class="badge badge-success">Snowflake connected</span>' : ''}
            ${glueOk ? '<span class="badge badge-success">Glue connected</span>' : ''}
            ${!canContinue ? `<span role="status" style="font-size:12px;color:${bothFailed ? 'var(--error)' : 'var(--text-muted)'}">${continueHint}</span>` : ''}
          </div>
          <div style="display:flex;gap:8px">
            <button class="btn btn-secondary btn-lg" id="sfglue-manual" ${canContinue ? '' : 'disabled'} title="Step through it manually">Check lineage →</button>
            <button class="btn btn-primary btn-lg" id="sfglue-run" ${canContinue ? '' : 'disabled'} title="Run the whole migration automatically, with one review checkpoint">Run migration</button>
          </div>
        </div>
      </div>
    </div>`;

  const readSnowflake = () => ({
    account: container.querySelector('#sf-account').value.trim(),
    user: container.querySelector('#sf-user').value.trim(),
    password: container.querySelector('#sf-password').value || sf.password || '',
    role: container.querySelector('#sf-role').value.trim(),
    warehouse: container.querySelector('#sf-warehouse').value.trim(),
    database: container.querySelector('#sf-database').value.trim(),
    schema: container.querySelector('#sf-schema').value.trim(),
    authenticator: sf.authenticator || '',
  });
  const readGlue = () => ({
    region: container.querySelector('#glue-region').value.trim(),
    profile_name: container.querySelector('#glue-profile').value.trim(),
    access_key_id: container.querySelector('#glue-access-key').value.trim(),
    secret_access_key: container.querySelector('#glue-secret-key').value || glue.secret_access_key || '',
    session_token: container.querySelector('#glue-session-token').value || glue.session_token || '',
    catalog_id: container.querySelector('#glue-catalog-id').value.trim(),
  });
  const readPostgres = () => ({
    host: container.querySelector('#pg-host')?.value.trim() || '',
    port: container.querySelector('#pg-port')?.value.trim() || '5432',
    database: container.querySelector('#pg-database')?.value.trim() || '',
    user: container.querySelector('#pg-user')?.value.trim() || '',
    password: container.querySelector('#pg-password')?.value || pg.password || '',
    schema: container.querySelector('#pg-schema')?.value.trim() || '',
  });

  container.querySelector('#sfglue-exit')?.addEventListener('click', () => {
    store.navigate('sfglue-home');
  });

  // ── Tile / panel switching (no re-render — panels stay mounted so field
  //    values and handlers survive; only visibility flips) ──────────────────────
  const selectTab = (tab) => {
    activeConnectTab = tab;
    container.querySelectorAll('.connect-panel').forEach((p) => {
      p.style.display = p.dataset.panel === tab ? '' : 'none';
    });
    container.querySelectorAll('.src-tile').forEach((t) => {
      const sel = t.dataset.tab === tab;
      t.setAttribute('aria-pressed', String(sel));
      t.style.borderColor = sel ? 'var(--primary)' : 'var(--border)';
      t.style.background = sel ? 'var(--primary-soft)' : 'transparent';
    });
  };
  container.querySelectorAll('.src-tile').forEach((t) =>
    t.addEventListener('click', () => selectTab(t.dataset.tab)));
  container.querySelectorAll('[data-next-tab]').forEach((b) =>
    b.addEventListener('click', () => selectTab(b.dataset.nextTab)));
  // Reveal the optional Postgres connector and open it (full re-render adds the tile+panel).
  container.querySelector('#pg-reveal')?.addEventListener('click', () => {
    pgRevealed = true;
    activeConnectTab = 'postgres';
    renderSfGlueConnectPage(container);
  });

  // Load the schemas of a database so the Schema field can be a picker.
  const loadSchemas = async (database) => {
    if (!database) { store.set({ sfGlueSnowflakeSchemas: [], sfGlueLoadingSchemas: false, sfGlueSchemasError: '' }); return; }
    store.set({ sfGlueLoadingSchemas: true, sfGlueSchemasError: '' });
    try {
      const res = await api.listSnowflakeSchemas({ ...store.get().sfGlueSnowflakeConfig, database });
      store.set({ sfGlueSnowflakeSchemas: res.schemas || [], sfGlueLoadingSchemas: false, sfGlueSchemasError: '' });
    } catch (err) {
      // Don't swallow: an empty picker must be distinguishable from "couldn't load".
      store.set({ sfGlueSnowflakeSchemas: [], sfGlueLoadingSchemas: false, sfGlueSchemasError: err.message || 'Could not load schemas' });
      notify('Could not load schemas — type one manually.', { kind: 'warning', title: 'Snowflake' });
    }
  };

  // Persist Database/Warehouse/Schema picks immediately (they only become <select>s
  // after a successful test). Changing the database resets the schema, reloads its
  // schema list, and invalidates prior lineage so a re-analyze rebuilds fresh.
  const dbEl = container.querySelector('#sf-database');
  if (dbEl && dbEl.tagName === 'SELECT') {
    dbEl.addEventListener('change', (e) => {
      store.patchSfGlueConfig('snowflake', { database: e.target.value, schema: '' }, { invalidateLineage: true });
      loadSchemas(e.target.value);  // store.set inside → re-render shows the schema picker
    });
  }
  const whEl = container.querySelector('#sf-warehouse');
  if (whEl && whEl.tagName === 'SELECT') {
    whEl.addEventListener('change', (e) =>
      store.patchSfGlueConfig('snowflake', { warehouse: e.target.value }));
  }
  const scEl = container.querySelector('#sf-schema');
  if (scEl && scEl.tagName === 'SELECT') {
    scEl.addEventListener('change', (e) =>
      store.patchSfGlueConfig('snowflake', { schema: e.target.value }, { invalidateLineage: true }));
  }

  container.querySelector('#sf-test')?.addEventListener('click', async () => {
    const config = readSnowflake();
    store.set({ sfGlueSnowflakeConfig: config, isTestingSnowflake: true, sfGlueSnowflakeConnection: null });
    try {
      const result = await api.testSnowflakeConnection(config);
      store.set({ sfGlueSnowflakeConnection: result, isTestingSnowflake: false });
      if (result.success) notify('Snowflake connected.', { kind: 'success', title: 'Snowflake' });
      else notify(result.error || 'Connection failed', { kind: 'error', title: 'Connection failed' });
      // If a database is already chosen, preload its schemas for the picker.
      if (result.success && config.database) loadSchemas(config.database);
    } catch (err) {
      store.set({ sfGlueSnowflakeConnection: { success: false, error: err.message }, isTestingSnowflake: false });
      notify(err.message, { kind: 'error', title: 'Connection failed' });
    }
  });

  container.querySelector('#pg-test')?.addEventListener('click', async () => {
    const config = readPostgres();
    store.set({ sfGluePostgresConfig: config, isTestingPostgres: true, sfGluePostgresConnection: null });
    try {
      const result = await api.testPostgresConnection(config);
      store.set({ sfGluePostgresConnection: result, isTestingPostgres: false });
      if (result.success) notify('Postgres connected.', { kind: 'success', title: 'Postgres' });
      else notify(result.error || 'Connection failed', { kind: 'error', title: 'Connection failed' });
    } catch (err) {
      store.set({ sfGluePostgresConnection: { success: false, error: err.message }, isTestingPostgres: false });
      notify(err.message, { kind: 'error', title: 'Connection failed' });
    }
  });

  container.querySelector('#glue-test')?.addEventListener('click', async () => {
    const config = readGlue();
    store.set({ sfGlueGlueConfig: config, isTestingGlue: true, sfGlueGlueConnection: null });
    try {
      const result = await api.testGlueConnection(config);
      store.set({ sfGlueGlueConnection: result, isTestingGlue: false });
      if (result.success) notify('AWS Glue connected.', { kind: 'success', title: 'AWS Glue' });
      else notify(result.error || 'Connection failed', { kind: 'error', title: 'Connection failed' });
    } catch (err) {
      store.set({ sfGlueGlueConnection: { success: false, error: err.message }, isTestingGlue: false });
      notify(err.message, { kind: 'error', title: 'Connection failed' });
    }
  });

  // ── "Sign in with AWS SSO" (Identity Center device flow) ───────────────────
  // start → open verification URL → poll until approved → pick account/role →
  // short-lived role creds drop into the key fields → auto-test the connection.
  container.querySelector('#glue-sso-login')?.addEventListener('click', async () => {
    const status = container.querySelector('#glue-sso-status');
    const say = (html) => { if (status) status.innerHTML = html; };
    const region = container.querySelector('#glue-region').value.trim() || 'us-east-1';
    const startRes = await promptModal({
      title: 'Sign in with AWS SSO',
      message: 'Your AWS Identity Center start URL:',
      fields: [{
        id: 'startUrl', label: 'Start URL', type: 'text',
        placeholder: 'https://your-org.awsapps.com/start',
        value: localStorage.getItem('qvf_aws_sso_start_url') || 'https://your-org.awsapps.com/start',
      }],
      confirmLabel: 'Continue',
    });
    if (!startRes) return;
    const startUrl = (startRes.startUrl || '').trim();
    if (!startUrl) return;
    localStorage.setItem('qvf_aws_sso_start_url', startUrl);
    try {
      say('starting sign-in…');
      const s = await api.awsSsoStart({ startUrl, region });
      window.open(s.verification_uri, '_blank');
      say(`approve in the AWS tab (code <strong>${esc(s.user_code)}</strong>) — waiting…`);
      const deadline = Date.now() + (s.expires_in || 600) * 1000;
      let authorized = false;
      while (Date.now() < deadline) {
        await new Promise(r => setTimeout(r, (s.interval || 5) * 1000));
        const p = await api.awsSsoPoll(s.session_id);
        if (p.status === 'authorized') { authorized = true; break; }
        say(`approve in the AWS tab (code <strong>${esc(s.user_code)}</strong>) — waiting…`);
      }
      if (!authorized) { say('sign-in window expired — try again.'); return; }

      say('signed in ✓ — loading accounts…');
      const acc = await api.awsSsoAccounts(s.session_id);
      const accounts = acc.accounts || [];
      if (!accounts.length) { say('no accounts available for this user.'); return; }
      // pick account (auto when there's exactly one) and role (auto when one)
      let acct = accounts[0];
      if (accounts.length > 1) {
        const pick = await promptModal({
          title: 'Choose an AWS account',
          fields: [{
            id: 'account', label: 'AWS account', type: 'select',
            options: accounts.map((a, i) => ({
              value: String(i),
              label: `${a.account_name || a.account_id} (${a.account_id})`,
            })),
          }],
          confirmLabel: 'Select',
        });
        if (!pick) { say('sign-in cancelled.'); return; }
        acct = accounts[parseInt(pick.account, 10)] || accounts[0];
      }
      let role = (acct.roles || [])[0];
      if ((acct.roles || []).length > 1) {
        const pick = await promptModal({
          title: `Choose a role in ${acct.account_id}`,
          fields: [{
            id: 'role', label: 'IAM role', type: 'select',
            options: acct.roles.map((r, i) => ({ value: String(i), label: r })),
          }],
          confirmLabel: 'Select',
        });
        if (!pick) { say('sign-in cancelled.'); return; }
        role = acct.roles[parseInt(pick.role, 10)] || acct.roles[0];
      }
      if (!role) { say('no roles available in that account.'); return; }

      say(`getting credentials for ${esc(role)} @ ${esc(acct.account_id)}…`);
      const cred = await api.awsSsoCredentials({ sessionId: s.session_id,
                                                 accountId: acct.account_id, roleName: role });
      container.querySelector('#glue-access-key').value = cred.access_key_id;
      container.querySelector('#glue-secret-key').value = cred.secret_access_key;
      container.querySelector('#glue-session-token').value = cred.session_token;
      container.querySelector('#glue-profile').value = '';
      const mins = cred.expiration_ms ? Math.max(1, Math.round((cred.expiration_ms - Date.now()) / 60000)) : 60;
      say(`✓ signed in as <strong>${esc(role)}</strong> @ ${esc(acct.account_id)} — credentials valid ~${mins} min. Testing connection…`);
      container.querySelector('#glue-test')?.click();
    } catch (err) {
      say(`${esc(err.message)}`);
    }
  });

  // ── Bucket picker + reference-environment tools ────────────────────────────
  const loadBuckets = async () => {
    try {
      const r = await api.listAwsBuckets({ glue: readGlue() });
      store.set({ sfGlueBuckets: r.buckets || [] });
    } catch (err) {
      notify(err.message, { kind: 'error', title: 'Bucket list failed' });
    }
  };
  if ((store.get().sfGlueGlueConnection || {}).success && !store.get().sfGlueBuckets) loadBuckets();
  container.querySelector('#glue-buckets-refresh')?.addEventListener('click', loadBuckets);
  container.querySelector('#glue-bucket')?.addEventListener('change', (e) => {
    store.set({ sfGlueSelectedBucket: e.target.value });
  });


  // Persist whatever's typed (so the next step uses current values), then navigate.
  const persistAndGo = (page) => {
    store.set({ sfGlueSnowflakeConfig: readSnowflake(), sfGlueGlueConfig: readGlue() });
    store.navigate(page);
  };
  container.querySelector('#sfglue-manual')?.addEventListener('click', () => persistAndGo('sfglue-lineage'));
  container.querySelector('#sfglue-run')?.addEventListener('click', () => persistAndGo('sfglue-run'));
}

export function destroySfGlueConnectPage() {
  /* no timers/graphs to clean up */
}
