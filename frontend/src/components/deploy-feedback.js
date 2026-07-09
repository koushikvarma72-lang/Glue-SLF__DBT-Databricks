/**
 * Deploy feedback loop helpers.
 *
 * recordDeployment(): every deploy path (dbt local, Databricks notebook, …)
 * records its last outcome here so the Report page can show a "Deployed"
 * stage without re-querying anything.
 *
 * fixInReview(): failed runs offer a one-click jump back to Review with the
 * failure preloaded into the Refine chat — closing the generate → deploy →
 * fix loop instead of leaving the error stranded on the deploy page.
 */
import { store } from '../store.js';

export function recordDeployment(target, status, detail) {
  // Quiet: the deploy pages update their own DOM; the Report page reads this
  // on its next render.
  store.setQuiet({
    lastDeployment: {
      target,
      status, // 'success' | 'failed'
      detail: (detail || '').slice(0, 400),
      at: new Date().toISOString(),
    },
  });
}

export function recordReconciliation(result) {
  // Quiet: the Report page reads this on its next render (it also fetches on
  // demand via the "Reconcile now" button, which calls this itself).
  store.setQuiet({ reconciliation: result || null });
}

export function fixInReview(errorText) {
  const state = store.get();
  const fileId = state.currentFileId || state.fileId;
  if (fileId) store.setFileReviewState(fileId, { activeRightTab: 'chat' });
  // Quiet set + navigate: one render, with the chat tab open and prefilled.
  store.setQuiet({
    reviewChatPrefill: (errorText || '').slice(0, 4000),
  });
  store.navigate('review');
}
