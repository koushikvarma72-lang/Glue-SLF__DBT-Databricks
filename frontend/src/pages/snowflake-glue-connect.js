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

function statusBadge(conn) {
  if (!conn) return '';
  if (conn.success) {
    const who = friendlyIdentity(conn.identity);
    const full = (conn.identity && conn.identity.arn) || '';
    return `<div class="badge badge-success" role="status" aria-live="polite" title="${esc(full).replace(/"/g, '&quot;')}" style="display:inline-flex;align-items:center;gap:6px;font-size:11px;margin-top:4px"><span aria-hidden="true">✓</span> Connected${who ? ` — ${esc(who)}` : ''}</div>`;
  }
  return `<div class="badge badge-error" role="alert" style="display:block;font-size:11px;margin-top:4px;white-space:normal;word-break:break-word"><span aria-hidden="true">⚠</span> ${esc(conn.error || 'Connection failed')}</div>`;
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
  // Visibility gate: for now the Postgres connector is always shown. LATER this should be
  // true only when an external DB is detected (e.g. a Glue job reads JDBC, or lineage flags
  // an external DB source). Flip this to that condition when the trigger is finalised.
  const showPostgresConnect = true;
  // A test was attempted on both sources but neither connected — name the cause so the
  // disabled Continue button doesn't read as "you haven't tried yet".
  const bothFailed = !canContinue && sfConn && glueConn;
  const continueHint = bothFailed
    ? 'Both tests failed — fix credentials and re-test to continue.'
    : 'Connect Snowflake and/or AWS Glue to continue.';

  // A compact connection-status row for the sidebar Sources section.
  const sourceStatusRow = (icon, name, ok, conn) => {
    const dot = ok ? 'var(--success)' : (conn ? 'var(--error)' : 'var(--text-dim)');
    const txt = ok ? 'Connected' : (conn ? (conn.error || 'Failed') : 'Not connected');
    return `
      <div style="display:flex;align-items:center;gap:8px;font-size:12px;padding:5px 0">
        <span aria-hidden="true" style="font-size:14px">${icon}</span>
        <span style="flex:1;color:var(--text-primary)">${name}</span>
        <span role="status" style="display:inline-flex;align-items:center;gap:5px;color:${ok ? 'var(--success)' : 'var(--text-muted)'};font-size:11px;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(txt).replace(/"/g, '&quot;')}">
          <span aria-hidden="true" style="width:7px;height:7px;border-radius:50%;background:${dot};flex-shrink:0"></span>${esc(txt)}
        </span>
      </div>`;
  };

  container.innerHTML = `
    <div class="page" id="sfglue-connect-page">
      <!-- Sidebar -->
      <div class="sidebar animate-slide-left">
        <div class="sidebar-section">
          <div class="sidebar-section-title">Migration</div>
          <div style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600;color:var(--text-primary);padding:2px 0">
            <span aria-hidden="true">❄️🪣</span>
            <span>Snowflake + Glue</span>
            <span style="color:var(--text-dim)">→</span>
            <span aria-hidden="true">🧱</span>
            <span>Databricks / dbt</span>
          </div>
        </div>

        <div class="sidebar-section">
          <div class="sidebar-section-title">Sources</div>
          ${sourceStatusRow('❄️', 'Snowflake', sfOk, sfConn)}
          ${sourceStatusRow('🪣', 'AWS Glue', glueOk, glueConn)}
        </div>

        <div class="sidebar-content">
          <div style="font-size:12px;color:var(--text-secondary);line-height:1.6">
            Connect at least one source to continue. We read Snowflake tables/views and the
            Glue Data Catalog + ETL scripts to build a full source→Snowflake lineage and flag
            duplication before migrating.
          </div>
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
            <p style="color:var(--text-secondary);margin:0 0 22px;font-size:13px;line-height:1.6">
              Connect <strong>Snowflake</strong> and <strong>AWS Glue</strong>. We'll read Snowflake tables/views and the Glue
              Data Catalog + ETL job scripts to build a full source→Snowflake lineage and flag duplication before migrating to Databricks/DBT.
            </p>

            <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(280px, 1fr));gap:20px">
              <!-- Snowflake -->
              <div class="card">
                <div class="card-header">
                  <div class="card-title"><span aria-hidden="true">❄️</span> Snowflake</div>
                  ${sfOk ? '<span class="badge badge-success" role="status" style="margin-left:auto;font-size:11px"><span aria-hidden="true">✓</span> Connected</span>' : ''}
                </div>
                <div class="card-body">
                  ${field('sf-account', 'Account', sf.account, { placeholder: 'ab12345.us-east-1' })}
                  ${field('sf-user', 'User', sf.user, { placeholder: 'SVC_MIGRATION' })}
                  ${field('sf-password', 'Password', sf.password, { type: 'password', placeholder: sf.password ? '•••••• (saved)' : '' })}
                  ${sfDatabases.length
                    ? selectField('sf-database', 'Database', sf.database, sfDatabases, { hint: `${sfDatabases.length} loaded` })
                    : field('sf-database', 'Database', sf.database, { placeholder: 'Connect first to load databases' })}
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                    ${sfWarehouses.length
                      ? selectField('sf-warehouse', 'Warehouse', sf.warehouse, sfWarehouses, { hint: `${sfWarehouses.length} loaded` })
                      : field('sf-warehouse', 'Warehouse', sf.warehouse, { placeholder: 'WH_XS' })}
                    ${field('sf-role', 'Role (optional)', sf.role, { placeholder: 'SYSADMIN' })}
                  </div>
                  ${loadingSchemas
                    ? field('sf-schema', 'Schema (optional)', '', { placeholder: 'Loading schemas…', hint: 'Loading schemas…' })
                    : sfSchemas.length
                    ? selectField('sf-schema', 'Schema (optional)', sf.schema, sfSchemas, { blankLabel: '(all schemas)', hint: `${sfSchemas.length} schema(s) — pick one (recommended for large DBs) or all` })
                    : field('sf-schema', 'Schema (optional)', sf.schema, { hint: schemasError ? 'Could not load schemas — type one manually.' : 'Blank = all schemas. Select a database to load its schemas.' })}
                  <button class="btn btn-primary" id="sf-test" ${state.isTestingSnowflake ? 'disabled' : ''} style="width:100%;margin-top:8px;justify-content:center">
                    ${state.isTestingSnowflake ? 'Testing…' : '<span aria-hidden="true">🔌</span> Test Snowflake connection'}
                  </button>
                  ${statusBadge(sfConn)}
                </div>
              </div>

              <!-- AWS Glue -->
              <div class="card">
                <div class="card-header">
                  <div class="card-title"><span aria-hidden="true">🪣</span> AWS Glue</div>
                  ${glueOk ? '<span class="badge badge-success" role="status" style="margin-left:auto;font-size:11px"><span aria-hidden="true">✓</span> Connected</span>' : ''}
                </div>
                <div class="card-body">
                  ${field('glue-region', 'Region', glue.region, { placeholder: 'us-east-1' })}
                  ${field('glue-profile', 'AWS profile (optional)', glue.profile_name, { hint: 'Use a named profile, or the access keys below.' })}
                  ${field('glue-access-key', 'Access key ID', glue.access_key_id, { placeholder: 'AKIA…' })}
                  ${field('glue-secret-key', 'Secret access key', glue.secret_access_key, { type: 'password', placeholder: glue.secret_access_key ? '•••••• (saved)' : '' })}
                  ${field('glue-session-token', 'Session token (optional)', glue.session_token, { type: 'password', placeholder: glue.session_token ? '•••••• (saved)' : '' })}
                  ${field('glue-catalog-id', 'Catalog ID (optional)', glue.catalog_id, { hint: 'AWS account id of the Glue catalog, if not the caller account.' })}
                  <button class="btn btn-primary" id="glue-test" ${state.isTestingGlue ? 'disabled' : ''} style="width:100%;margin-top:8px;justify-content:center">
                    ${state.isTestingGlue ? 'Testing…' : '<span aria-hidden="true">🔌</span> Test AWS Glue connection'}
                  </button>
                  ${statusBadge(glueConn)}
                </div>
              </div>

            </div>

            ${showPostgresConnect ? `
            <!-- Postgres (external origin) — collapsible optional connector at the bottom.
                 Field IDs / handlers are unchanged; only the presentation moved. -->
            <details class="pg-dropdown" ${pgOk ? 'open' : ''}>
              <summary>
                <span aria-hidden="true" style="font-size:16px">🐘</span>
                <span style="flex:1">Postgres <span style="font-size:11px;color:var(--text-muted);font-weight:400">(external source — optional)</span></span>
                ${pgOk ? '<span class="badge badge-success" role="status" style="font-size:11px"><span aria-hidden="true">✓</span> Connected</span>' : ''}
                <span class="pg-chev" aria-hidden="true">▾</span>
              </summary>
              <div class="pg-dropdown-body">
                <div style="display:grid;grid-template-columns:2fr 1fr;gap:10px">
                  ${field('pg-host', 'Host', pg.host, { placeholder: 'db.internal.example.com' })}
                  ${field('pg-port', 'Port', pg.port || '5432', { placeholder: '5432' })}
                </div>
                ${field('pg-database', 'Database', pg.database, { placeholder: 'app_prod' })}
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                  ${field('pg-user', 'User', pg.user, { placeholder: 'readonly' })}
                  ${field('pg-password', 'Password', pg.password, { type: 'password', placeholder: pg.password ? '•••••• (saved)' : '' })}
                </div>
                ${field('pg-schema', 'Schema (optional)', pg.schema, { hint: 'Blank = all non-system schemas.' })}
                <button class="btn btn-primary" id="pg-test" ${state.isTestingPostgres ? 'disabled' : ''} style="width:100%;margin-top:8px;justify-content:center">
                  ${state.isTestingPostgres ? 'Testing…' : '<span aria-hidden="true">🔌</span> Test Postgres connection'}
                </button>
                ${statusBadge(pgConn)}
              </div>
            </details>` : ''}
          </div>
        </div>

        <!-- Bottom Bar -->
        <div class="review-footer">
          <div style="display:flex;align-items:center;gap:8px">
            ${sfOk ? '<span class="badge badge-success"><span aria-hidden="true">❄️</span> Snowflake connected</span>' : ''}
            ${glueOk ? '<span class="badge badge-success"><span aria-hidden="true">🪣</span> Glue connected</span>' : ''}
            ${!canContinue ? `<span role="status" style="font-size:12px;color:${bothFailed ? 'var(--error)' : 'var(--text-muted)'}">${continueHint}</span>` : ''}
          </div>
          <div style="display:flex;gap:8px">
            <button class="btn btn-secondary btn-lg" id="sfglue-manual" ${canContinue ? '' : 'disabled'} title="Step through it manually">Check lineage →</button>
            <button class="btn btn-primary btn-lg" id="sfglue-run" ${canContinue ? '' : 'disabled'} title="Run the whole migration automatically, with one review checkpoint">🚀 Run migration</button>
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
