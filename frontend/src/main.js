/**
 * sfglue — Snowflake + AWS Glue → Databricks/dbt.  Standalone SPA entry (single tool).
 * Hash-based router; lands on the Connect step. (Split out of the combined BI Migration Tool —
 * the Qlik/Cognos/Tableau → Power BI flows live in the separate qvf_decoder app.)
 */
import './styles/main.css';
import { store } from './store.js';
import { TOOLS, stepForPage, allToolPages, destroyAllToolPages } from './tools/registry.js';
import { renderStepNav } from './components/step-nav.js';
import { renderSfGlueHomePage, destroySfGlueHomePage } from './pages/sfglue-home.js';
import { renderSfGlueRunPage, destroySfGlueRunPage } from './pages/snowflake-glue-run.js';

const app = document.getElementById('app');
const STEPS = TOOLS.snowflake_glue.steps;
// The landing page and the automated Run page sit outside the numbered workflow —
// they're not registry steps, so they're validated/routed here rather than via the registry.
const HOME_PAGE = 'sfglue-home';
const RUN_PAGE = 'sfglue-run';
const validNavPage = (hash) => hash === HOME_PAGE || hash === RUN_PAGE || allToolPages().includes(hash);

// ─── Route guards: gate each step on its prerequisite so a deep-link/back-forward
// can't land on a step with no inputs. ────────────────────────────────────────
const PAGE_GUARDS = {
  'sfglue-run': (s) => (
    s.sfGlueSnowflakeConnection?.success || s.sfGlueGlueConnection?.success ? null : 'sfglue-connect'
  ),
  'sfglue-lineage': (s) => (
    s.sfGlueSnowflakeConnection?.success || s.sfGlueGlueConnection?.success ? null : 'sfglue-connect'
  ),
  'sfglue-review': (s) => (s.sfGlueLineage ? null : 'sfglue-connect'),
  'sfglue-databricks-agent': (s) => (s.sfGlueLineage ? null : 'sfglue-connect'),
  'sfglue-dbt-agent': (s) => (s.sfGlueLineage ? null : 'sfglue-connect'),
  'sfglue-map': (s) => (s.sfGlueConversion ? null : 'sfglue-connect'),
  'sfglue-report': (s) => (s.sfGlueLineage || s.sfGlueConversion ? null : 'sfglue-connect'),
};

function resolveGuardedPage(page, state) {
  let current = page;
  for (let hops = 0; hops < 4; hops++) {
    const guard = PAGE_GUARDS[current];
    const target = guard ? guard(state) : null;
    if (!target || target === current) return current;
    current = target;
  }
  return 'sfglue-connect';
}

// ─── App shell ────────────────────────────────────────────────────────────────
function renderApp() {
  const state = store.get();
  if (!state.currentPage || !validNavPage(state.currentPage)) state.currentPage = HOME_PAGE;

  // Landing page: full-bleed, no workflow step-nav / status bar.
  if (state.currentPage === HOME_PAGE) {
    if (window.location.hash.slice(1) !== HOME_PAGE) window.location.hash = HOME_PAGE;
    destroyAllToolPages();
    app.innerHTML = '<div id="page-content" style="flex:1;display:flex;overflow:hidden"></div>';
    renderSfGlueHomePage(document.getElementById('page-content'));
    return;
  }

  // Entering the workflow — tear down the landing page if it was mounted.
  destroySfGlueHomePage();

  const guarded = resolveGuardedPage(state.currentPage, state);
  if (guarded !== state.currentPage) {
    state.currentPage = guarded;
    if (window.location.hash.slice(1) !== guarded) window.location.hash = guarded;
  }

  const acct = state.sfGlueSnowflakeConnection?.identity?.account;
  const connected = !!(state.sfGlueSnowflakeConnection?.success || state.sfGlueGlueConnection?.success);
  const activeName = acct ? `Snowflake: ${acct}` : (connected ? 'Snowflake + Glue' : '');

  app.innerHTML = `
    <nav class="navbar">
      <div class="navbar-brand">
        <div><div class="navbar-title">Snowflake + Glue → Databricks</div></div>
      </div>
      <div class="navbar-nav">${renderStepNav(STEPS, state.currentPage, state)}</div>
      <div class="navbar-actions">
        ${activeName ? `
          <span style="font-size:11px;color:var(--text-muted);display:flex;align-items:center;gap:6px">
            <span style="color:var(--success)">●</span>${activeName}
          </span>` : ''}
      </div>
    </nav>
    <div id="page-content" style="flex:1;display:flex;overflow:hidden"></div>
    <div class="status-bar">
      <div class="status-bar-left">
        <div class="status-indicator">
          <div class="status-dot ${activeName ? '' : 'warning'}"></div>
          <span>${activeName ? 'Connected' : 'Ready'}</span>
        </div>
      </div>
      <div class="status-bar-right"><span>sfglue — Snowflake + Glue → Databricks/dbt</span></div>
    </div>
  `;

  renderCurrentPage(document.getElementById('page-content'), state.currentPage);
  setupNavigation();
  requestAnimationFrame(positionNavPill);
}

// ─── Sliding nav-pill indicator (glide the highlight onto the active step) ──────
let lastPillRect = null;
function positionNavPill() {
  const nav = document.querySelector('.navbar-nav');
  if (!nav) return;
  let indicator = nav.querySelector('.nav-pill-indicator');
  if (!indicator) {
    indicator = document.createElement('div');
    indicator.className = 'nav-pill-indicator';
    nav.insertBefore(indicator, nav.firstChild);
  }
  const active = nav.querySelector('.nav-step.active');
  if (!active) { indicator.style.opacity = '0'; lastPillRect = null; return; }
  const navRect = nav.getBoundingClientRect();
  const aRect = active.getBoundingClientRect();
  const pos = { left: aRect.left - navRect.left, top: aRect.top - navRect.top, width: aRect.width, height: aRect.height };
  const apply = (r) => {
    indicator.style.transform = `translate(${r.left}px, ${r.top}px)`;
    indicator.style.width = `${r.width}px`;
    indicator.style.height = `${r.height}px`;
    indicator.style.opacity = '1';
  };
  if (lastPillRect) {
    indicator.style.transition = 'none';
    apply(lastPillRect);
    void indicator.offsetWidth;
    indicator.style.transition = 'transform 0.42s cubic-bezier(0.34, 1.18, 0.4, 1), width 0.42s cubic-bezier(0.34, 1.18, 0.4, 1)';
    apply(pos);
  } else {
    indicator.style.transition = 'none';
    apply(pos);
  }
  lastPillRect = pos;
}
window.addEventListener('resize', () => { lastPillRect = null; positionNavPill(); });

function renderCurrentPage(container, page) {
  destroyAllToolPages();
  destroySfGlueRunPage();
  // The automated Run page isn't a registry step — dispatch it explicitly.
  if (page === RUN_PAGE) { renderSfGlueRunPage(container); return; }
  const step = stepForPage(page);
  if (step) { step.render(container); return; }
  // Fallback: unknown page → Connect.
  stepForPage('sfglue-connect').render(container);
}

function setupNavigation() {
  document.querySelectorAll('.nav-step').forEach((step) => {
    const disabled = step.classList.contains('disabled');
    const active = step.classList.contains('active');
    step.setAttribute('role', 'link');
    if (active) step.setAttribute('aria-current', 'step'); else step.removeAttribute('aria-current');
    if (disabled) {
      step.setAttribute('aria-disabled', 'true');
      if (!step.title) step.title = 'Complete the previous steps to unlock this one';
    } else {
      step.removeAttribute('aria-disabled');
    }
    step.querySelector('.nav-step-number')?.setAttribute('aria-hidden', 'true');
  });
  document.querySelectorAll('.nav-step:not(.disabled)').forEach((step) => {
    step.addEventListener('click', (e) => {
      e.preventDefault();
      if (step.dataset.page) store.navigate(step.dataset.page);
    });
  });
}

// ─── State + browser navigation ─────────────────────────────────────────────
store.subscribe(() => renderApp());

window.addEventListener('hashchange', () => {
  const hash = window.location.hash.slice(1);
  if (validNavPage(hash) && store.get().currentPage !== hash) {
    store.set({ currentPage: hash });
  }
});

// ─── Initial render ───────────────────────────────────────────────────────────
store.set({ uploadMode: 'snowflake_glue' });
const initialHash = window.location.hash.slice(1);
if (!validNavPage(initialHash)) window.location.hash = HOME_PAGE;
else store.get().currentPage = initialHash;
renderApp();
