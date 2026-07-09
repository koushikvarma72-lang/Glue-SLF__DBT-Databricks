/**
 * Generic step navigation, rendered from a tool's step list.
 *
 * Produces the same markup as the hand-written nav (numbered .nav-step pills with
 * .active / .disabled), so a tool only declares its steps and the shell renders
 * them identically. Disabled steps keep their data-page but are skipped by
 * setupNavigation's `.nav-step:not(.disabled)` wiring.
 */
export function renderStepNav(steps, currentPage, state) {
  return steps.map((step, i) => {
    const active = currentPage === step.page;
    const disabled = typeof step.enabled === 'function' ? !step.enabled(state) : false;
    const divider = i > 0 ? '<div class="nav-divider"></div>' : '';
    return `${divider}
        <a class="nav-step ${active ? 'active' : ''} ${disabled ? 'disabled' : ''}" data-page="${step.page}" id="nav-${step.page}">
          <span class="nav-step-number">${i + 1}</span>
          ${step.label}
        </a>`;
  }).join('');
}
