export const agentState = {
  connected: false,
  runId: null,
  runHref: '',
  status: '',
  statusDetail: '',
  account: null,
  projects: [],
  jobs: [],
  error: '',

  // Local dbt-Core run (against the connected Databricks workspace).
  localJobId: null,
  localRunning: false,
  localFinished: false,
  localStatus: '', // running | success | error | cancelled
  localSummary: '',
  localLogs: '',
  localLogOffset: 0, // absolute count of log lines already consumed (incremental fetch)
  localModels: [], // per-model results from run_results.json
  localCancelling: false,
  localError: '',
};
