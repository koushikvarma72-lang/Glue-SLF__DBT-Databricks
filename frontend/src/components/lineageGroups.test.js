import { describe, it, expect } from 'vitest';
import { groupKeyFor, groupLineage } from './lineageGroups.js';

describe('groupKeyFor', () => {
  it('groups a Snowflake db.schema.table by its schema', () => {
    expect(groupKeyFor({ system: 'snowflake', type: 'silver', label: 'VEEVA_CRM.SALES.ACCOUNT' })).toBe('SALES');
  });

  it('groups a two-part Snowflake name by its database', () => {
    expect(groupKeyFor({ system: 'snowflake', type: 'silver', label: 'VEEVA_CRM.ACCOUNT' })).toBe('VEEVA_CRM');
  });

  it('groups a Glue database.table by its database', () => {
    expect(groupKeyFor({ system: 'glue', type: 'bronze', label: 'raw_db.account' })).toBe('raw_db');
  });

  it('groups an S3 source by its bucket', () => {
    expect(groupKeyFor({ type: 'source', label: 's3://my-bucket/raw/account/' })).toBe('s3://my-bucket');
  });
});

describe('groupLineage', () => {
  const lineage = {
    nodes: [
      { id: 'src1', type: 'source', label: 's3://bkt/raw/a.xlsx', display: 'a.xlsx' },
      { id: 'sf:acct', type: 'silver', system: 'snowflake', label: 'DB.SALES.ACCOUNT', display: 'SALES.ACCOUNT', column_count: 12 },
      { id: 'sf:call', type: 'silver', system: 'snowflake', label: 'DB.SALES.CALL', display: 'SALES.CALL', column_count: 8 },
      { id: 'sf:camp', type: 'silver', system: 'snowflake', label: 'DB.MKT.CAMPAIGN', display: 'MKT.CAMPAIGN' },
      { id: 'sf:dim', type: 'gold', system: 'snowflake', label: 'DB.MARTS.DIM_ACCOUNT', display: 'MARTS.DIM_ACCOUNT' },
      { id: 'job:j1', type: 'job', system: 'glue', label: 'load_account', display: 'load_account', job_type: 'glueetl' },
    ],
    edges: [],
  };

  it('drops empty layers and keeps the ones with nodes', () => {
    const layers = groupLineage(lineage).layers.map(l => l.layer);
    expect(layers).toEqual(['source', 'silver', 'gold']); // no bronze nodes here
  });

  it('buckets each layer by schema with correct counts', () => {
    const silver = groupLineage(lineage).layers.find(l => l.layer === 'silver');
    expect(silver.count).toBe(3);
    expect(silver.groups.map(g => [g.name, g.count])).toEqual([['MKT', 1], ['SALES', 2]]); // sorted by name
  });

  it('separates jobs from data nodes', () => {
    const model = groupLineage(lineage);
    expect(model.total).toBe(5); // 5 data nodes, job excluded
    expect(model.jobs.map(j => j.label)).toEqual(['load_account']);
  });

  it('handles empty input', () => {
    const model = groupLineage({ nodes: [], edges: [] });
    expect(model.layers).toEqual([]);
    expect(model.jobs).toEqual([]);
    expect(model.total).toBe(0);
  });
});
