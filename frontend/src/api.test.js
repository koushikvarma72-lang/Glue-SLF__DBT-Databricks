import { describe, it, expect } from 'vitest';
import { api } from './api.js';

// Smoke test: guards against syntax errors and accidental removal of the
// public API surface the pages depend on. Expand with behavioral tests
// (mock fetch / EventSource) as logic moves out of the page modules.
describe('api client surface', () => {
  it('exports an api object', () => {
    expect(api).toBeTypeOf('object');
  });

  it('exposes the core methods used across pages', () => {
    for (const method of ['uploadFile', 'streamJob', 'chatStream', 'regenerate']) {
      expect(api[method], `api.${method}`).toBeTypeOf('function');
    }
  });
});
