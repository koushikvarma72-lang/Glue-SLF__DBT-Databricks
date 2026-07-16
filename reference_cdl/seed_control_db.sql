-- Complete the CDL control DB with the REAL framework schema.
--
-- v2: the first version of this file guessed simplified table shapes. The actual
-- Glue job source (parent_batch_open/close, landing_to_raw, raw_to_curated,
-- curated_to_publish, publish_to_snowflake) is the spec — every table/column here
-- is derived from the SQL those jobs execute. Column ORDER in parent_batch_process
-- matters: parent_batch_open INSERTs positionally.
--
--   psql -h localhost -d control -f seed_control_db.sql
--
-- Never touches your original 3 tables (configuration_master, query_configuration,
-- cdl_ds_snowflake_replicate). It DROPs only the v1 kit-created tables (wrong
-- shapes, effectively empty) before recreating them correctly.

DROP VIEW  IF EXISTS v_batch_summary;
DROP TABLE IF EXISTS dl_ingestion_log;      -- v1 kit shape — wrong columns
DROP TABLE IF EXISTS file_process_log;      -- v1 kit shape — wrong columns
DROP TABLE IF EXISTS parent_batch_process;  -- v1 kit shape — wrong columns

-- ── Batch tracking ───────────────────────────────────────────────────────────
-- Column order matches parent_batch_open's positional INSERT:
--   VALUES (batch_id, name, start, end, 'I', status, '', '', source_system,
--           created_date, created_by, updated_date, updated_by)
CREATE TABLE parent_batch_process (
    parent_batch_id    varchar(64),
    parent_batch_name  varchar(200),
    batch_start_date   timestamp,
    batch_end_date     timestamp,
    completion_status  varchar(5),      -- I | C | F
    process_status     varchar(30),     -- In Progress | Completed | Failed
    remarks            varchar(250) DEFAULT '',
    error_message      varchar(250) DEFAULT '',
    source_system      varchar(100),
    created_date       timestamp,
    created_by         varchar(100),
    updated_date       timestamp,
    updated_by         varchar(100)
);

-- landing_to_raw inserts named columns + reads created_date; file ids come from
-- this sequence (select nextval('file_process_log_id_sequence')).
CREATE SEQUENCE IF NOT EXISTS file_process_log_id_sequence;
CREATE TABLE file_process_log (
    file_id             bigint,
    file_name           text,
    pattern_name        text,
    record_count        bigint,
    configuration_id    bigint,
    source_system       varchar(100),
    processed_date_time timestamp,
    received_date_time  timestamp,
    parent_batch_id     varchar(64),
    process_status      varchar(30),
    created_date        date DEFAULT current_date
);

-- Per-layer status ledger — every column referenced by the jobs' INSERT/UPDATEs.
CREATE TABLE dl_ingestion_log (
    file_name                        text,
    batch_id                         varchar(64),
    file_id                          bigint,
    pattern_name                     text,
    data_source                      varchar(100),
    vendor                           varchar(100) DEFAULT '',
    fileingestion_start_timestamp    timestamp,
    landing_status                   varchar(20),
    raw_status                       varchar(20),
    landing_record_count             bigint,
    raw_record_count                 bigint,
    curated_status                   varchar(20),
    curated_location                 text,
    curated_completion_timestamp     timestamp,
    curated_record_count             bigint,
    curated_message                  text,
    failure_count                    bigint,
    published_status                 varchar(20),
    published_location               text,
    published_completion_timestamp   timestamp,
    published_record_count           bigint,
    published_message                text,
    snowflake_status                 varchar(20),
    snowflake_location               text,
    snowflake_completion_timestamp   timestamp,
    snowflake_record_count           bigint,
    snowflake_message                text
);

-- parent_batch_close checks outbound failures here.
CREATE TABLE IF NOT EXISTS outbound_logs (
    id             bigserial PRIMARY KEY,
    source_system  varchar(100),
    batch_id       varchar(64),
    output_status  varchar(20),
    file_name      text,
    logged_at      timestamp DEFAULT now()
);

-- raw_to_curated's DQ engine: per-source rules + the reusable rule templates.
CREATE TABLE IF NOT EXISTS dq_rules (
    id                  bigserial PRIMARY KEY,
    source_system       varchar(100),
    rule_name           varchar(100),
    column_name         text,
    tgt_table_name      varchar(200),
    sql_condition       text,
    sql_query           text,
    priority_level      varchar(20),     -- high => fail_, else warn_
    dq_failure_s3_path  text,
    active_flag         varchar(5) DEFAULT 'A'
);
CREATE TABLE IF NOT EXISTS dq_rules_master (
    rule_name  varchar(100),
    sql_query  text,
    is_active  varchar(5) DEFAULT 'Y'
);

-- curated_to_publish's stitching engine (full/append/upsert/delete_flag/roll_out/scd2).
CREATE TABLE IF NOT EXISTS stitching_configuration (
    id                    bigserial PRIMARY KEY,
    dl_source             varchar(100),
    pt_pattern_name       text,
    pattern_name          text,
    source_database_name  varchar(200),
    source_table_name     varchar(200),
    target_database_name  varchar(200),
    target_table_name     varchar(200),
    stitching_type        varchar(30),   -- veeva_crm uses 'full'
    primary_keys          text,
    order_key             text,
    record_load_key       text,
    active_flag           varchar(5) DEFAULT 'A'
);

-- landing_to_raw's error-file reconciliation reads the outbound target path here.
CREATE TABLE IF NOT EXISTS outbound_query_configuration (
    id                    bigserial PRIMARY KEY,
    source_system         varchar(100),
    target_location_path  text,
    sql_query             text,
    active_flag           varchar(5) DEFAULT 'A'
);

-- Rebuild the audit summary view on the real columns.
CREATE OR REPLACE VIEW v_batch_summary AS
SELECT p.parent_batch_id, p.source_system, p.process_status, p.completion_status,
       p.batch_start_date, p.batch_end_date,
       count(DISTINCT f.file_id) AS files,
       count(DISTINCT i.file_id) AS ingestion_rows
FROM parent_batch_process p
LEFT JOIN file_process_log f ON f.parent_batch_id = p.parent_batch_id
LEFT JOIN dl_ingestion_log i ON i.batch_id = p.parent_batch_id
GROUP BY 1, 2, 3, 4, 5, 6;

-- One completed demo batch so first introspection shows data.
INSERT INTO parent_batch_process VALUES
  ('demo-batch-000', 'parent_batch_process', now(), now(), 'C', 'Completed',
   'seeded by reference_cdl kit v2', '', 'veeva_crm', now(), 'seed', now(), 'seed');

-- Snowflake replication checklist (parent_batch_close LEFT JOINs this; empty is fine —
-- the Snowflake leg is retired in the demo, Databricks replaces it).
CREATE TABLE IF NOT EXISTS dl_ds_snowflake_replicate (
    source_system   varchar(100),
    source_schema   varchar(200),
    source_table    varchar(200),
    target_schema   varchar(200),
    target_table    varchar(200),
    data_load_flag  varchar(5) DEFAULT 'A'
);
