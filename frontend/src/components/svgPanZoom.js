/**
 * Drag-to-pan / scroll-to-zoom for a rendered SVG (e.g. a Mermaid diagram).
 *
 * Mermaid produces a static SVG with no interaction. enableSvgPanZoom makes the
 * SVG fill its host box (fit-to-view) and attaches d3-zoom so the user can scroll
 * to zoom and drag to pan — the same feel as the D3 force graph. resetSvgPanZoom
 * snaps back to the fitted view.
 */
import { select } from 'd3-selection';
import { zoom as d3Zoom, zoomIdentity } from 'd3-zoom';

const SVG_NS = 'http://www.w3.org/2000/svg';

export function enableSvgPanZoom(container, { minScale = 0.1, maxScale = 8 } = {}) {
  const svg = container && container.querySelector('svg');
  if (!svg) return null;

  // Make the SVG fill its host so the whole diagram fits the box; d3-zoom then
  // scales/pans an inner layer rather than relying on native scrollbars.
  svg.removeAttribute('height');
  svg.setAttribute('width', '100%');
  svg.style.maxWidth = 'none';
  svg.style.width = '100%';
  svg.style.height = '100%';
  svg.style.display = 'block';
  svg.style.cursor = 'grab';

  // A viewBox + preserveAspectRatio is what fits the content into the host box.
  if (!svg.getAttribute('viewBox')) {
    try {
      const b = svg.getBBox();
      svg.setAttribute('viewBox', `${b.x} ${b.y} ${b.width} ${b.height}`);
    } catch { /* getBBox can throw before layout; mermaid usually sets viewBox itself */ }
  }
  svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');

  // Move all content into one transform layer (idempotent across re-renders).
  let layer = svg.querySelector(':scope > g.__pz_layer');
  if (!layer) {
    layer = document.createElementNS(SVG_NS, 'g');
    layer.setAttribute('class', '__pz_layer');
    const kids = [];
    svg.childNodes.forEach(n => { if (n !== layer) kids.push(n); });
    kids.forEach(n => layer.appendChild(n));
    svg.appendChild(layer);
  }

  const sel = select(svg);
  const gSel = select(layer);
  const behavior = d3Zoom()
    .scaleExtent([minScale, maxScale])
    .on('start', () => { svg.style.cursor = 'grabbing'; })
    .on('zoom', (event) => { gSel.attr('transform', event.transform); })
    .on('end', () => { svg.style.cursor = 'grab'; });

  // dblclick.zoom off → double-click doesn't surprise-zoom; node clicks still work.
  sel.call(behavior).on('dblclick.zoom', null);

  // Keyboard accessibility: focusable + arrow-pan / +-/0 zoom so the diagram
  // isn't mouse-only. aria-label doubles as the discoverability hint.
  svg.setAttribute('tabindex', '0');
  svg.setAttribute('role', 'application');
  svg.setAttribute('aria-label', 'Diagram — drag or arrow keys to pan, scroll or +/- to zoom, 0 to reset');
  if (!svg.__pzKeys) {
    svg.addEventListener('keydown', (e) => {
      const step = 40;
      if (e.key === 'ArrowLeft') sel.call(behavior.translateBy, step, 0);
      else if (e.key === 'ArrowRight') sel.call(behavior.translateBy, -step, 0);
      else if (e.key === 'ArrowUp') sel.call(behavior.translateBy, 0, step);
      else if (e.key === 'ArrowDown') sel.call(behavior.translateBy, 0, -step);
      else if (e.key === '+' || e.key === '=') sel.call(behavior.scaleBy, 1.2);
      else if (e.key === '-' || e.key === '_') sel.call(behavior.scaleBy, 1 / 1.2);
      else if (e.key === '0') sel.call(behavior.transform, zoomIdentity);
      else return;
      e.preventDefault();
    });
    svg.__pzKeys = true;
  }

  svg.__panzoom = { sel, behavior };
  return svg.__panzoom;
}

/** Snap the diagram back to the fitted, un-panned view. */
export function resetSvgPanZoom(container) {
  const svg = container && container.querySelector('svg');
  if (!svg || !svg.__panzoom) return;
  const { sel, behavior } = svg.__panzoom;
  sel.call(behavior.transform, zoomIdentity);
}
