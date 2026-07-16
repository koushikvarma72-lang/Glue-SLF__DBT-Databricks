/**
 * Migration tool registry — sfglue app (single tool).
 *
 * This is the standalone Snowflake + AWS Glue → Databricks/dbt app, so the registry holds
 * only that tool. (In the combined BI Migration Tool this also carried the Cognos/Qlik and
 * Tableau → Power BI tools; those live in the separate qvf_decoder app.)
 */
import { renderSfGlueConnectPage, destroySfGlueConnectPage } from '../pages/snowflake-glue-connect.js';
import { renderSfGlueLineagePage, destroySfGlueLineagePage } from '../pages/snowflake-glue-lineage.js';
import { renderSfGlueReviewPage, destroySfGlueReviewPage } from '../pages/snowflake-glue-review.js';
import { renderSfGlueDatabricksAgentPage, destroySfGlueDatabricksAgentPage } from '../pages/snowflake-glue-databricks-agent.js';
import { renderSfGlueDbtAgentPage, destroySfGlueDbtAgentPage } from '../pages/snowflake-glue-dbt-agent.js';
import { renderSfGlueReportPage, destroySfGlueReportPage } from '../pages/snowflake-glue-report.js';
import { renderSfGlueMapPage, destroySfGlueMapPage } from '../pages/snowflake-glue-map.js';

const sfGlueConnected = s =>
  !!((s.sfGlueSnowflakeConnection && s.sfGlueSnowflakeConnection.success) ||
     (s.sfGlueGlueConnection && s.sfGlueGlueConnection.success));

export const TOOLS = {
  snowflake_glue: {
    id: 'snowflake_glue',
    label: 'Snowflake + AWS Glue → Databricks/DBT',
    steps: [
      { page: 'sfglue-connect', label: 'Connect Sources', enabled: () => true,
        render: renderSfGlueConnectPage, destroy: destroySfGlueConnectPage },
      { page: 'sfglue-lineage', label: 'Check Lineage', enabled: sfGlueConnected,
        render: renderSfGlueLineagePage, destroy: destroySfGlueLineagePage },
      { page: 'sfglue-review', label: 'Review & Edit', enabled: s => !!s.sfGlueLineage,
        render: renderSfGlueReviewPage, destroy: destroySfGlueReviewPage },
      { page: 'sfglue-databricks-agent', label: 'Databricks Agent', enabled: s => !!s.sfGlueLineage,
        render: renderSfGlueDatabricksAgentPage, destroy: destroySfGlueDatabricksAgentPage },
      { page: 'sfglue-dbt-agent', label: 'DBT Agent', enabled: s => !!s.sfGlueLineage,
        render: renderSfGlueDbtAgentPage, destroy: destroySfGlueDbtAgentPage },
      { page: 'sfglue-map', label: 'Migration Map', enabled: s => !!s.sfGlueConversion,
        render: renderSfGlueMapPage, destroy: destroySfGlueMapPage },
      { page: 'sfglue-report', label: 'Report', enabled: s => !!s.sfGlueLineage || !!s.sfGlueConversion,
        render: renderSfGlueReportPage, destroy: destroySfGlueReportPage },
    ],
  },
};

/** The tool that owns the current uploadMode, or null. */
export function toolForMode(uploadMode) {
  return TOOLS[uploadMode] || null;
}

/** The step whose page matches, across all tools (for dispatch). */
export function stepForPage(page) {
  for (const tool of Object.values(TOOLS)) {
    const step = tool.steps.find(s => s.page === page);
    if (step) return step;
  }
  return null;
}

/** Every registered tool page id (for hash-route validation). */
export function allToolPages() {
  return Object.values(TOOLS).flatMap(t => t.steps.map(s => s.page));
}

/** Run every tool's destroy hooks (called before rendering a new page). */
export function destroyAllToolPages() {
  Object.values(TOOLS).forEach(t => t.steps.forEach(s => s.destroy && s.destroy()));
}
