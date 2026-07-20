/**
 * The one Databricks destination form — shared by the Databricks Agent and the
 * Automated Run pages so the connection is entered once and stays identical.
 *
 * Both pages already read/write store.sfGlueDestination; this removes the two
 * divergent forms (different field sets, different persistence) behind a single
 * component, and always persists to localStorage so values survive a reload.
 *
 * Only one instance is ever mounted at a time (separate routes), so fixed
 * `dest-*` field ids are safe.
 */
import { store } from '../store.js';
import { field } from './ui.js';

const FIELD_IDS = [
  'dest-url', 'dest-token', 'dest-warehouse', 'dest-catalog',
  'dest-bronze', 'dest-silver', 'dest-gold', 'dest-source-catalog', 'dest-source-schema',
];

/** The destination field grid (9 fields), prefilled from ``dest`` (store.sfGlueDestination). */
export function destinationForm(dest = {}) {
  return `
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px">
      ${field('dest-url', 'Workspace URL', dest.workspace_url, { placeholder: 'https://dbc-xxxx.cloud.databricks.com' })}
      ${field('dest-token', 'Access token', dest.token, { type: 'password', placeholder: dest.token ? '•••••• (saved)' : 'dapi…' })}
      ${field('dest-warehouse', 'SQL Warehouse ID', dest.sql_warehouse_id, {})}
      ${field('dest-catalog', 'Catalog', dest.catalog, { placeholder: 'lakehouse' })}
      ${field('dest-bronze', 'Bronze schema', dest.bronze_schema, { placeholder: 'bronze' })}
      ${field('dest-silver', 'Silver schema', dest.silver_schema, { placeholder: 'silver' })}
      ${field('dest-gold', 'Gold schema', dest.gold_schema, { placeholder: 'gold' })}
      ${field('dest-source-catalog', 'Source catalog', dest.source_catalog, { placeholder: 'raw_catalog' })}
      ${field('dest-source-schema', 'Source schema', dest.source_schema, { placeholder: 'raw' })}
    </div>`;
}

const val = (c, id) => (c.querySelector('#' + id)?.value || '').trim();

/** Read the form into a full destination object (with the same defaults both pages used). */
export function readDestination(container) {
  const prev = store.get().sfGlueDestination || {};
  return {
    workspace_url: val(container, 'dest-url'),
    // token field carries the real value; fall back to the saved one when left blank.
    token: (container.querySelector('#dest-token')?.value || '') || prev.token || '',
    sql_warehouse_id: val(container, 'dest-warehouse'),
    catalog: val(container, 'dest-catalog') || 'lakehouse',
    bronze_schema: val(container, 'dest-bronze') || 'bronze',
    silver_schema: val(container, 'dest-silver') || 'silver',
    gold_schema: val(container, 'dest-gold') || 'gold',
    // Raw landing location for {{ source('bronze', …) }}; blank → server falls back to catalog/bronze.
    source_catalog: val(container, 'dest-source-catalog'),
    source_schema: val(container, 'dest-source-schema'),
  };
}

/** Persist the form to the store AND localStorage (survives reload). Returns the dest. */
export function persistDestination(container) {
  const d = readDestination(container);
  store.get().sfGlueDestination = d;
  try { localStorage.setItem('qvf_sfglue_destination', JSON.stringify(d)); } catch (_) { /* non-fatal */ }
  return d;
}

/** Persist on every field change (no re-render, so tabbing between fields isn't disrupted). */
export function wireDestination(container) {
  FIELD_IDS.forEach(id =>
    container.querySelector('#' + id)?.addEventListener('change', () => persistDestination(container)));
}
