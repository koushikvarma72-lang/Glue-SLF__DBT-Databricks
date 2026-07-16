"""Unit tests for the gap-plan phase engines (1, 2, 3, 5, 6, 7) — pure functions only.

Run from the repo root:
    python -m unittest backend.tests.test_gap_phases -v
"""

import unittest

from backend.integrations.consumption_inventory import (
    generate_outbound_cutover_md,
    snowflake_pipeline_notes,
)
from backend.integrations.control_plane_migration import (
    convert_query_configuration_rows,
    generate_control_schema_ddl,
    generate_framework_notebooks,
    postgres_type_to_databricks,
)
from backend.integrations.dq_migration import (
    classify_dq_rule,
    compile_dq_rules,
    compile_notifications,
)
from backend.integrations.governance_migration import map_permissions_to_uc_grants
from backend.integrations.orchestration_migration import (
    build_databricks_job,
    glue_cron_to_quartz,
    job_to_dab_yaml,
    parse_workflow_dag,
)

DEST = {"catalog": "workspace", "gold_schema": "gold", "control_schema": "control"}


class TestOrchestration(unittest.TestCase):
    WORKFLOW = {
        "name": "cdl_ingest",
        "nodes": [
            {"id": "t0", "type": "trigger", "name": "nightly"},
            {"id": "j1", "type": "job", "name": "landing_to_raw"},
            {"id": "t1", "type": "trigger", "name": "after_landing"},
            {"id": "j2", "type": "job", "name": "raw_to_curated"},
            {"id": "t2", "type": "trigger", "name": "after_curated"},
            {"id": "j3", "type": "job", "name": "curated_to_publish"},
            {"id": "c1", "type": "crawler", "name": "landing_crawler"},
        ],
        "edges": [
            {"source": "t0", "target": "j1"}, {"source": "t0", "target": "c1"},
            {"source": "j1", "target": "t1"}, {"source": "t1", "target": "j2"},
            {"source": "j2", "target": "t2"}, {"source": "t2", "target": "j3"},
        ],
    }
    TRIGGERS = [
        {"name": "nightly", "type": "SCHEDULED", "schedule": "cron(0 2 * * ? *)",
         "actions": [], "conditions": [], "predicate_logical": ""},
        {"name": "after_landing", "type": "CONDITIONAL", "schedule": "",
         "conditions": [{"job_name": "landing_to_raw", "state": "SUCCEEDED",
                         "logical_operator": "EQUALS"}], "predicate_logical": "AND"},
        {"name": "after_curated", "type": "CONDITIONAL", "schedule": "",
         "conditions": [{"job_name": "raw_to_curated", "state": "SUCCEEDED",
                         "logical_operator": "EQUALS"}], "predicate_logical": "AND"},
    ]

    def test_cron_translation(self):
        self.assertEqual(glue_cron_to_quartz("cron(0 2 * * ? *)"), "0 0 2 * * ?")
        self.assertEqual(glue_cron_to_quartz("cron(15 12 ? * MON-FRI 2026)"),
                         "0 15 12 ? * MON-FRI 2026")
        # Quartz requires one of DOM/DOW to be "?" — DOW specified, so DOM becomes "?".
        self.assertEqual(glue_cron_to_quartz("30 6 * * 1"), "0 30 6 ? * 1")
        self.assertIsNone(glue_cron_to_quartz("rate(1 hour)"))
        self.assertIsNone(glue_cron_to_quartz(""))

    def test_dag_dependencies_and_schedule(self):
        dag = parse_workflow_dag(self.WORKFLOW, self.TRIGGERS)
        self.assertEqual(dag["schedule"], "0 0 2 * * ?")
        by_key = {t["key"]: t for t in dag["tasks"]}
        self.assertEqual(by_key["raw_to_curated"]["depends_on"], ["landing_to_raw"])
        self.assertEqual(by_key["curated_to_publish"]["depends_on"], ["raw_to_curated"])
        self.assertEqual(by_key["landing_to_raw"]["depends_on"], [])
        self.assertEqual(by_key["landing_crawler"]["kind"], "crawler")

    def test_on_failure_condition_warns(self):
        triggers = [dict(self.TRIGGERS[1])]
        triggers[0]["conditions"] = [{"job_name": "landing_to_raw", "state": "FAILED",
                                      "logical_operator": "EQUALS"}]
        dag = parse_workflow_dag(self.WORKFLOW, triggers)
        self.assertTrue(any("FAILED" in w for w in dag["warnings"]))

    def test_build_job_with_artifact_map(self):
        dag = parse_workflow_dag(self.WORKFLOW, self.TRIGGERS)
        amap = {
            "landing_to_raw": {"kind": "notebook", "path": "landing_to_raw.py"},
            "raw_to_curated": {"kind": "dbt", "models": ["stg_a.sql", "fct_b.sql"]},
            "curated_to_publish": {"kind": "framework", "notebook": "fw_batch_close.py"},
        }
        built = build_databricks_job(dag, artifact_map=amap, destination=DEST,
                                     email_notifications={"on_failure": ["ops@x.com"]})
        job = built["job"]
        self.assertEqual(job["schedule"]["quartz_cron_expression"], "0 0 2 * * ?")
        self.assertEqual(job["email_notifications"]["on_failure"], ["ops@x.com"])
        tasks = {t["task_key"]: t for t in job["tasks"]}
        self.assertIn("notebook_task", tasks["landing_to_raw"])
        self.assertIn("dbt_task", tasks["raw_to_curated"])
        self.assertIn("stg_a", tasks["raw_to_curated"]["dbt_task"]["commands"][1])
        # crawler had no artifact → placeholder + warning
        self.assertTrue(built["placeholders"])
        self.assertTrue(any("landing_crawler" in w for w in built["warnings"]))
        self.assertEqual(tasks["raw_to_curated"]["depends_on"], [{"task_key": "landing_to_raw"}])

    def test_file_arrival_trigger_beats_schedule(self):
        dag = parse_workflow_dag(self.WORKFLOW, self.TRIGGERS)
        built = build_databricks_job(dag, destination=DEST,
                                     file_arrival_url="s3://bucket/landing/")
        self.assertIn("trigger", built["job"])
        self.assertNotIn("schedule", built["job"])
        self.assertEqual(built["job"]["trigger"]["file_arrival"]["url"], "s3://bucket/landing/")

    def test_dab_yaml_serialization(self):
        dag = parse_workflow_dag(self.WORKFLOW, self.TRIGGERS)
        built = build_databricks_job(dag, destination=DEST)
        yml = job_to_dab_yaml(built["job"], dag["name"])
        self.assertIn("resources:", yml)
        self.assertIn("cdl_ingest:", yml)
        try:
            import yaml as pyyaml
            parsed = pyyaml.safe_load(yml)
            job = parsed["resources"]["jobs"]["cdl_ingest"]
            self.assertEqual(len(job["tasks"]), 4)
        except ImportError:
            pass


class TestControlPlane(unittest.TestCase):
    def test_pg_type_mapping(self):
        for pg, dbx in [("integer", "INT"), ("bigint", "BIGINT"), ("bigserial", "BIGINT"),
                        ("numeric(12,2)", "DECIMAL(12,2)"), ("numeric", "DECIMAL(38,6)"),
                        ("timestamp without time zone", "TIMESTAMP"), ("date", "DATE"),
                        ("boolean", "BOOLEAN"), ("jsonb", "STRING"), ("text", "STRING"),
                        ("double precision", "DOUBLE"), ("weird_type", "STRING")]:
            self.assertEqual(postgres_type_to_databricks(pg), dbx, pg)

    def test_control_schema_ddl(self):
        ddl = generate_control_schema_ddl(
            [{"name": "configuration_master", "schema": "public",
              "canonical": "configuration_master",
              "columns": [{"name": "source_system", "type": "text"},
                          {"name": "active_flag", "type": "character varying(1)"}]}], DEST)
        sql = ddl["control__configuration_master"]
        self.assertIn("CREATE TABLE IF NOT EXISTS `workspace`.`control`.`configuration_master`", sql)
        self.assertIn("`source_system` STRING", sql)
        self.assertTrue(sql.rstrip().endswith("USING DELTA;"))

    QC = {"columns": [{"name": "target_table"}, {"name": "query_text"},
                      {"name": "is_active"}, {"name": "execution_order"}],
          "rows": [
              ["gold.rpt_calls", "select * from curated.calls", "Y", 2],
              ["gold.dim_account", "select id, name from curated.account", "Y", 1],
              ["gold.old_extract", "select 1", "N", 3],
              ["", "", "Y", 4],
          ]}

    def test_qc_rows_no_ai_scaffold(self):
        res = convert_query_configuration_rows(None, self.QC, destination=DEST,
                                               bronze_sources=["calls", "account"])
        self.assertEqual(sorted(res["models"]), ["dim_account.sql", "rpt_calls.sql"])
        self.assertEqual(len(res["skipped"]), 2)  # inactive + empty
        self.assertIn("TODO[EXTERNAL]", res["models"]["rpt_calls.sql"])
        self.assertIn("select * from curated.calls", res["models"]["rpt_calls.sql"])

    def test_qc_rows_with_ai(self):
        calls = []

        def fake_ai(prompt, system_prompt=None, **kw):
            calls.append(prompt)
            return "select id from {{ source('bronze', 'account') }}"

        res = convert_query_configuration_rows(fake_ai, self.QC, destination=DEST,
                                               bronze_sources=["account"])
        self.assertEqual(len(calls), 2)
        # execution_order respected: dim_account (order 1) processed first
        self.assertIn("dim_account", calls[0])
        model = res["models"]["dim_account.sql"]
        self.assertIn("config(materialized='table')", model)
        self.assertIn("source('bronze', 'account')", model)

    def test_qc_no_sql_column(self):
        res = convert_query_configuration_rows(
            None, {"columns": [{"name": "foo"}], "rows": [["x"]]}, destination=DEST)
        self.assertEqual(res["models"], {})
        self.assertTrue(res["skipped"])

    def test_framework_notebooks(self):
        nbs = generate_framework_notebooks(DEST)
        self.assertEqual(sorted(nbs), ["fw_batch_close.py", "fw_batch_open.py", "fw_file_audit.py"])
        self.assertIn("MERGE INTO `workspace`.`control`.`batch_log`", nbs["fw_batch_open.py"])
        self.assertIn("ingestion_log", nbs["fw_file_audit.py"])


class TestDQCompiler(unittest.TestCase):
    DQ = {"columns": [{"name": "rule_type"}, {"name": "table_name"}, {"name": "column_name"},
                      {"name": "severity"}, {"name": "rule_expression"},
                      {"name": "ref_table"}, {"name": "ref_column"}],
          "rows": [
              ["not_null", "dim_account", "id", "critical", "", "", ""],
              ["unique", "dim_account", "id", "warn", "", "", ""],
              ["referential", "fct_calls", "account_id", "error", "", "dim_account", "id"],
              ["range_check", "fct_calls", "duration", "warn", "", "", ""],
              ["row_count", "landing_file", "", "error", "count > 0", "", ""],
              ["business_rule", "fct_calls", "", "quarantine", "amount >= 0", "", ""],
              ["mystery", "", "", "", "", "", ""],
          ]}

    def test_classification(self):
        r = classify_dq_rule({"rule_type": "fk_check", "table_name": "T", "column_name": "c",
                              "ref_table": "P", "ref_column": "id", "severity": "critical"})
        self.assertEqual(r["kind"], "relationships")
        self.assertEqual(r["severity"], "error")
        self.assertEqual(r["params"]["ref_table"], "p")

    def test_compile(self):
        out = compile_dq_rules(self.DQ, known_models=["dim_account", "fct_calls"])
        s = out["summary"]
        self.assertEqual(s["total"], 7)
        self.assertEqual(s["quarantine_models"], 1)
        self.assertEqual(s["notebook_checks"], 1)
        # range rule has no min/max columns → unclassified; mystery → unclassified
        self.assertEqual(s["unclassified"], 2)
        self.assertIn("fct_calls__rejects.sql", out["quarantine_models"])
        q = out["quarantine_models"]["fct_calls__rejects.sql"]
        self.assertIn("where not (amount >= 0)", q)
        self.assertIn("{{ ref('fct_calls') }}", q)
        yml = out["dq_schema_yml"]
        self.assertIn("- name: dim_account", yml)
        self.assertIn("- not_null", yml)
        self.assertIn("relationships", yml)
        self.assertIn("severity: warn", yml)

    def test_notifications(self):
        out = compile_notifications({
            "columns": [{"name": "template_name"}, {"name": "recipients"}, {"name": "subject"}],
            "rows": [["load_fail", "ops@corp.com; data@corp.com", "Load failed"],
                     ["ok", "", "Done"]]})
        self.assertEqual(out["email_notifications"]["on_failure"],
                         ["data@corp.com", "ops@corp.com"])
        self.assertEqual(len(out["templates"]), 2)


class TestGovernance(unittest.TestCase):
    PERMS = [
        {"principal": "arn:aws:iam::1:role/analysts", "resource_type": "table",
         "database": "cdl", "table": "dim_account", "columns": [], "permissions": ["SELECT"]},
        {"principal": "arn:aws:iam::1:role/etl", "resource_type": "database",
         "database": "cdl", "table": "", "columns": [], "permissions": ["ALL"]},
        {"principal": "arn:aws:iam::1:role/pii", "resource_type": "table",
         "database": "cdl", "table": "hcp", "columns": ["name"], "permissions": ["SELECT"]},
        {"principal": "arn:aws:iam::1:role/admin", "resource_type": "table",
         "database": "cdl", "table": "x", "columns": [], "permissions": ["DROP"]},
    ]

    def test_mapping(self):
        out = map_permissions_to_uc_grants(
            self.PERMS, catalog="workspace",
            principal_map={"arn:aws:iam::1:role/analysts": "analysts"})
        sql = out["grants_sql"]
        self.assertIn("GRANT SELECT ON TABLE `workspace`.`cdl`.`dim_account` TO `analysts`;", sql)
        self.assertIn("UNMAPPED PRINCIPAL", sql)          # etl role not mapped → commented
        self.assertIn("column-level grant", sql)          # pii columns → review line
        self.assertIn("no safe UC auto-translation", sql)  # DROP → review line
        self.assertIn("arn:aws:iam::1:role/etl", out["unmapped_principals"])
        self.assertEqual(out["stats"]["grants"], 1)


class TestOutboundAndPipeline(unittest.TestCase):
    def test_cutover_md(self):
        md = generate_outbound_cutover_md(
            [{"name": "IICS_SVC", "kind": "Informatica", "objects": ["PUB.PUB_CALL_FCT"]},
             {"name": "PBI", "kind": "Power BI", "objects": []}],
            {"workspace_url": "https://dbc-x.cloud.databricks.com",
             "sql_warehouse_id": "wh1", "catalog": "workspace"})
        self.assertIn("IICS_SVC", md)
        self.assertIn("Databricks Delta", md)
        self.assertIn("Partner Connect", md)
        self.assertIn("/sql/1.0/warehouses/wh1", md)

    def test_pipeline_notes(self):
        out = snowflake_pipeline_notes({
            "tasks": [{"name": "T_LOAD", "schedule": "USING CRON 0 2 * * *",
                       "state": "started", "definition": "insert into x select 1"}],
            "streams": [{"name": "S1", "table_name": "CALLS", "mode": "DEFAULT"}],
            "pipes": [{"name": "P1", "definition": "copy into ..."}],
            "procedures": [{"name": "SP_X", "arguments": "V VARCHAR"}],
            "stages": [{"name": "STG1", "url": "s3://b/p"}],
        }, DEST)
        self.assertEqual(len(out["notes"]), 5)
        self.assertEqual(out["job_task_seeds"][0]["name"], "T_LOAD")
        self.assertIn("Change Data Feed", out["notes"]["_pipeline__streams.md"])
        self.assertIn("Auto Loader", out["notes"]["_pipeline__pipes.md"])



class TestAirflow(unittest.TestCase):
    DAG_SRC = """
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from datetime import datetime

def notify(): pass

with DAG(dag_id="cdl_ingest_airflow", schedule_interval="0 2 * * *",
         start_date=datetime(2026, 1, 1)) as dag:
    wait_file = S3KeySensor(task_id="wait_landing_file", bucket_key="s3://b/landing/*")
    load_config = GlueJobOperator(task_id="load_config", job_name="load_confiq")
    batch_open = GlueJobOperator(task_id="batch_open", job_name="parent_batch_open")
    l2r = GlueJobOperator(task_id="landing_to_raw", job_name="landing_to_raw")
    r2c = GlueJobOperator(task_id="raw_to_curated", job_name="raw_to_curated")
    batch_close = GlueJobOperator(task_id="batch_close", job_name="parent_batch_close")
    alert = PythonOperator(task_id="notify_team", python_callable=notify)

    wait_file >> load_config >> batch_open >> [l2r, r2c] >> batch_close
    batch_close.set_downstream(alert)
"""

    def test_parse_dag_source(self):
        from backend.integrations.airflow_migration import parse_dag_source
        dag = parse_dag_source("f.py", self.DAG_SRC)
        self.assertEqual(dag["name"], "cdl_ingest_airflow")
        self.assertEqual(dag["schedule"], "0 0 2 * * ?")
        by = {t["key"]: t for t in dag["tasks"]}
        # Glue job_name becomes legacy_name so the Glue artifact_map applies as-is
        self.assertEqual(by["load_config"]["legacy_name"], "load_confiq")
        self.assertEqual(by["load_config"]["kind"], "glue_job")
        # fan-out list on the right of >>
        self.assertEqual(by["landing_to_raw"]["depends_on"], ["batch_open"])
        self.assertEqual(by["raw_to_curated"]["depends_on"], ["batch_open"])
        self.assertEqual(sorted(by["batch_close"]["depends_on"]),
                         ["landing_to_raw", "raw_to_curated"])
        # set_downstream
        self.assertEqual(by["notify_team"]["depends_on"], ["batch_close"])
        # sensor produces a file-arrival warning
        self.assertTrue(any("S3KeySensor" in w for w in dag["warnings"]))

    def test_schedule_presets(self):
        from backend.integrations.airflow_migration import airflow_schedule_to_quartz
        self.assertEqual(airflow_schedule_to_quartz("@daily"), "0 0 0 * * ?")
        self.assertEqual(airflow_schedule_to_quartz("@hourly"), "0 0 * * * ?")
        self.assertIsNone(airflow_schedule_to_quartz("@once"))
        self.assertIsNone(airflow_schedule_to_quartz(None))

    def test_dag_feeds_job_builder(self):
        from backend.integrations.airflow_migration import parse_dag_source
        from backend.integrations.orchestration_migration import build_databricks_job
        dag = parse_dag_source("f.py", self.DAG_SRC)
        amap = {"load_confiq": {"kind": "notebook", "path": "fw_file_audit.py"},
                "raw_to_curated": {"kind": "dbt", "models": ["stg_account.sql"]}}
        built = build_databricks_job(dag, artifact_map=amap,
                                     destination={"catalog": "workspace"})
        tasks = {t["task_key"]: t for t in built["job"]["tasks"]}
        self.assertIn("notebook_task", tasks["load_config"])
        self.assertIn("dbt_task", tasks["raw_to_curated"])
        self.assertEqual(built["job"]["schedule"]["quartz_cron_expression"],
                         "0 0 2 * * ?")

    def test_broken_dag_is_warning_not_crash(self):
        from backend.integrations.airflow_migration import parse_dag_source
        dag = parse_dag_source("bad.py", "def broken(:")
        self.assertEqual(dag["tasks"], [])
        self.assertTrue(any("does not parse" in w for w in dag["warnings"]))


class TestYamlFlow(unittest.TestCase):
    YAML_SRC = """
cdl_ingest_yaml:
  schedule_interval: "0 2 * * *"
  default_args:
    owner: cdl
  tasks:
    wait_for_files:
      operator: airflow.providers.amazon.aws.sensors.s3.S3KeySensor
      bucket_key: "s3://b/landing/*"
    load_config:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: load_confiq
      dependencies: [wait_for_files]
    landing_to_raw:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: landing_to_raw
      dependencies: [load_config]
    raw_to_curated:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: raw_to_curated
      dependencies: [landing_to_raw]
    notify:
      operator: airflow.operators.python.PythonOperator
      dependencies: [raw_to_curated]
"""

    def test_parse_dag_factory_yaml(self):
        from backend.integrations.airflow_migration import parse_dag_factory_yaml
        dags = parse_dag_factory_yaml("pipeline.yaml", self.YAML_SRC)
        self.assertEqual(len(dags), 1)
        dag = dags[0]
        self.assertEqual(dag["name"], "cdl_ingest_yaml")
        self.assertEqual(dag["schedule"], "0 0 2 * * ?")
        by = {t["key"]: t for t in dag["tasks"]}
        self.assertEqual(by["load_config"]["legacy_name"], "load_confiq")
        self.assertEqual(by["load_config"]["kind"], "glue_job")
        self.assertEqual(by["raw_to_curated"]["depends_on"], ["landing_to_raw"])
        self.assertEqual(by["wait_for_files"]["kind"], "sensor")
        self.assertTrue(any("S3KeySensor" in w for w in dag["warnings"]))

    def test_yaml_detection(self):
        from backend.integrations.airflow_migration import looks_like_yaml_dag
        self.assertTrue(looks_like_yaml_dag("dags.yaml", "x: 1"))
        self.assertTrue(looks_like_yaml_dag("pasted", self.YAML_SRC))
        self.assertFalse(looks_like_yaml_dag("dag.py", "from airflow import DAG"))
        self.assertFalse(looks_like_yaml_dag("pasted", "from airflow import DAG\nwith DAG(...): pass"))

    def test_yaml_dag_feeds_job_builder(self):
        from backend.integrations.airflow_migration import parse_dag_factory_yaml
        from backend.integrations.orchestration_migration import build_databricks_job
        dag = parse_dag_factory_yaml("p.yaml", self.YAML_SRC)[0]
        amap = {"raw_to_curated": {"kind": "dbt", "models": ["stg_account.sql"]}}
        built = build_databricks_job(dag, artifact_map=amap, destination={"catalog": "workspace"})
        tasks = {t["task_key"]: t for t in built["job"]["tasks"]}
        self.assertIn("dbt_task", tasks["raw_to_curated"])

    def test_layered_dbt_project(self):
        from backend.integrations.snowflake_glue_migration import (
            build_dbt_project_files, dbt_layer_for_model)
        self.assertEqual(dbt_layer_for_model("stg_account.sql"), "staging")
        self.assertEqual(dbt_layer_for_model("int_call_enriched.sql"), "intermediate")
        self.assertEqual(dbt_layer_for_model("dim_account.sql"), "marts")
        self.assertEqual(dbt_layer_for_model("fct_call.sql"), "marts")
        self.assertEqual(dbt_layer_for_model("weird.sql", "{{ config(schema='gold') }}"), "marts")
        conv = {
            "dbt_models": {
                "stg_account.sql": "{{ config(materialized='view', schema='silver') }}\nselect 1",
                "dim_account.sql": "{{ config(materialized='table', schema='gold') }}\nselect 1",
                "int_step.sql": "select 1",
            },
            "conf_files": {"pipeline_dags.yaml": "cdl: {tasks: {}}"},
        }
        files = build_dbt_project_files(conv, {"catalog": "workspace"})
        self.assertIn("models/staging/stg_account.sql", files)
        self.assertIn("models/marts/dim_account.sql", files)
        self.assertIn("models/intermediate/int_step.sql", files)
        self.assertIn("macros/generate_schema_name.sql", files)
        self.assertIn("conf/pipeline_dags.yaml", files)
        # persisted-tables contract: view upgraded to table
        self.assertIn("materialized='table'", files["models/staging/stg_account.sql"])
        self.assertNotIn("'view'", files["models/staging/stg_account.sql"])
        # layered project config with verbatim schemas
        proj = files["dbt_project.yml"]
        self.assertIn("staging:", proj)
        self.assertIn("marts:", proj)
        self.assertIn("+schema: gold", proj)

    def test_conf_files_in_push_plan(self):
        from backend.integrations.workspace_push import build_push_plan
        plan = build_push_plan({"notebooks": {}}, {"dbt_project.yml": "x"}, "/Shared/sfglue",
                               conf_files={"pipeline_dags.yaml": "cdl: 1"})
        paths = [p["path"] for p in plan]
        self.assertIn("/Shared/sfglue/conf/pipeline_dags.yaml", paths)


class TestTargetAirflow(unittest.TestCase):
    CONV = {
        "notebooks": {"fw_batch_open.py": "x", "fw_batch_close.py": "x",
                      "landing_to_raw__transform.py": "x", "fw_file_audit.py": "x"},
        "dbt_models": {"stg_account.sql": "{{ config(schema=' silver ') }}",
                       "int_call.sql": "select 1",
                       "dim_account.sql": "{{ config(schema=' gold ') }}"},
    }
    DEST = {"catalog": "workspace", "sql_warehouse_id": "wh123"}

    def test_emit_shape_and_roundtrip(self):
        import yaml
        from backend.integrations.airflow_migration import (
            emit_target_airflow_yaml, looks_like_yaml_dag, parse_dag_factory_yaml)
        out = emit_target_airflow_yaml(self.CONV, self.DEST, dag_id="cdl_migrated",
                                       file_arrival_path="s3://b/landing/*")
        self.assertEqual(out["layers"], ["staging", "intermediate", "marts"])
        self.assertTrue(looks_like_yaml_dag("x.yaml", out["yaml"]))
        doc = yaml.safe_load(out["yaml"])
        t = doc["cdl_migrated"]["tasks"]
        for tid in ("wait_for_files", "batch_open", "dbt_staging", "dbt_intermediate",
                    "dbt_marts", "batch_close", "notify"):
            self.assertIn(tid, t)
        self.assertEqual(t["dbt_marts"]["dbt_task"]["warehouse_id"], "wh123")
        self.assertIn("dbt build --select marts", t["dbt_marts"]["dbt_task"]["commands"])
        self.assertEqual(t["dbt_marts"]["dependencies"], ["dbt_intermediate"])
        self.assertEqual(t["dbt_intermediate"]["dependencies"], ["dbt_staging"])
        reparsed = parse_dag_factory_yaml("x.yaml", out["yaml"])[0]
        self.assertEqual(reparsed["name"], "cdl_migrated")
        self.assertTrue(any(x["key"] == "dbt_marts" for x in reparsed["tasks"]))

    def test_emit_dbt_source_modes(self):
        import yaml
        from backend.integrations.airflow_migration import emit_target_airflow_yaml
        g = emit_target_airflow_yaml(self.CONV, self.DEST, dbt_source="git",
                                     git_url="https://github.com/acme/cdl-dbt.git")
        t = yaml.safe_load(g["yaml"])["cdl_migrated_databricks"]["tasks"]
        self.assertEqual(t["dbt_staging"]["dbt_task"]["source"], "GIT")
        self.assertTrue(t["dbt_staging"]["git_source"]["git_url"].endswith("cdl-dbt.git"))
        c = emit_target_airflow_yaml(self.CONV, self.DEST, dbt_source="dbt_cloud",
                                     dbt_cloud_job_id="4242")
        t2 = yaml.safe_load(c["yaml"])["cdl_migrated_databricks"]["tasks"]
        self.assertIn("dbt_cloud_run", t2)
        self.assertNotIn("dbt_staging", t2)
        self.assertEqual(t2["dbt_cloud_run"]["job_id"], "4242")
        self.assertTrue(t2["dbt_cloud_run"]["operator"].endswith("DbtCloudRunJobOperator"))

    def test_emit_no_sensor_when_no_path(self):
        import yaml
        from backend.integrations.airflow_migration import emit_target_airflow_yaml
        out = emit_target_airflow_yaml(self.CONV, self.DEST)
        doc = yaml.safe_load(out["yaml"])
        self.assertNotIn("wait_for_files", doc["cdl_migrated_databricks"]["tasks"])


class TestOperationalLineage(unittest.TestCase):
    DAG = {"name": "wf", "tasks": [
        {"key": "load_cfg", "legacy_name": "load_confiq", "kind": "framework", "depends_on": []},
        {"key": "raw", "legacy_name": "landing_to_raw", "kind": "glue_job", "depends_on": ["load_cfg"]},
        {"key": "cur", "legacy_name": "raw_to_curated", "kind": "glue_job", "depends_on": ["raw"]},
        {"key": "pub", "legacy_name": "curated_to_publish", "kind": "glue_job", "depends_on": ["cur"]},
    ]}
    FW = [
        {"name": "query_configuration", "canonical": "query_configuration",
         "columns": [{"name": "interface"}, {"name": "source_tablename"},
                     {"name": "target_tablename"}, {"name": "sql_query"}],
         "rows": [["raw_to_curated", "raw_account", "account", "select 1"],
                  ["curated_to_publish", "account", "dim_account", "select 2"]], "row_count": 2},
        {"name": "parent_batch_process", "canonical": "parent_batch_process",
         "columns": [{"name": "parent_batch_id"}, {"name": "source_system"}],
         "rows": [["b1", "raw_to_curated"]], "row_count": 1},
    ]
    GLUE = [
        {"full_name": "medaffairs_silver.account", "columns": [1] * 14},
        {"full_name": "medaffairs_gold.dim_account", "columns": [1] * 11},
        {"full_name": "medaffairs_silver.orphan_tbl", "columns": [1] * 3},
    ]

    def _build(self, **kw):
        from backend.integrations.operational_lineage import build_operational_lineage
        return build_operational_lineage(self.DAG, self.FW, self.GLUE, {}, **kw)

    def test_execution_chain_from_triggers(self):
        out = self._build()
        ex = [(e["from"], e["to"]) for e in out["edges"] if e["kind"] == "execution"]
        self.assertEqual(len(ex), 3)  # 4 jobs chained

    def test_data_edges_from_config_rows(self):
        out = self._build()
        data = [(e["from"], e["to"]) for e in out["edges"] if e["kind"] == "data"]
        # account -> curated_to_publish -> dim_account is drawn from config, not scripts
        self.assertIn(("tbl:account", "job:curated_to_publish"), data)
        self.assertIn(("job:curated_to_publish", "tbl:dim_account"), data)

    def test_job_detail_and_flags(self):
        out = self._build(job_flags={"raw_to_curated": ["GLUE-CONSTRUCT x"]})
        pub = next(j for j in out["jobs"] if j["label"] == "curated_to_publish")
        self.assertEqual(pub["writes"], ["medaffairs_gold.dim_account"])
        self.assertEqual(len(pub["config_samples"]), 1)
        raw = next(j for j in out["jobs"] if j["label"] == "raw_to_curated")
        self.assertEqual(raw["flags"], ["GLUE-CONSTRUCT x"])
        self.assertIn("parent_batch_process", raw["control_tables"])

    def test_health_orphan_table(self):
        out = self._build()
        kinds = {h["kind"] for h in out["health"]}
        self.assertIn("orphan_table", kinds)

    def test_nothing_hardcoded_generic_names(self):
        # Arbitrary names must still fuse — proves nothing is keyed to the demo.
        dag = {"tasks": [{"key": "a", "legacy_name": "step_alpha", "depends_on": []},
                         {"key": "b", "legacy_name": "step_beta", "depends_on": ["a"]}]}
        fw = [{"name": "query_config", "canonical": "query_configuration",
               "columns": [{"name": "job"}, {"name": "src_table"}, {"name": "tgt_table"}],
               "rows": [["step_beta", "widgets_raw", "widgets_final"]], "row_count": 1}]
        glue = [{"full_name": "lake.widgets_raw", "columns": [1]},
                {"full_name": "lake.widgets_final", "columns": [1]}]
        from backend.integrations.operational_lineage import build_operational_lineage
        out = build_operational_lineage(dag, fw, glue, {})
        data = [(e["from"], e["to"]) for e in out["edges"] if e["kind"] == "data"]
        self.assertIn(("tbl:widgets_raw", "job:step_beta"), data)
        self.assertIn(("job:step_beta", "tbl:widgets_final"), data)


class TestAwsSsoAuth(unittest.TestCase):
    def test_start_rejects_bad_url(self):
        from backend.integrations.aws_sso_auth import start
        out = start("not-a-url", "us-west-2")
        self.assertFalse(out["success"])
        self.assertIn("start URL", out["error"])

    def test_poll_unknown_session(self):
        from backend.integrations.aws_sso_auth import poll
        out = poll("nope")
        self.assertFalse(out["success"])

    def test_credentials_requires_auth(self):
        from backend.integrations.aws_sso_auth import credentials
        out = credentials("nope", "123", "role")
        self.assertFalse(out["success"])

    def test_device_flow_with_stubbed_boto(self):
        # Full happy path with boto3 clients stubbed — no AWS access needed.
        import backend.integrations.aws_sso_auth as m

        class OIDC:
            def register_client(self, **k):
                return {"clientId": "cid", "clientSecret": "sec"}
            def start_device_authorization(self, **k):
                return {"deviceCode": "dev", "userCode": "ABCD-EFGH", "interval": 1,
                        "expiresIn": 600, "verificationUriComplete": "https://x/approve"}
            def create_token(self, **k):
                return {"accessToken": "tok", "expiresIn": 28800}

        class SSO:
            def get_paginator(self, name):
                class P:
                    def paginate(self_inner, **k):
                        if name == "list_accounts":
                            return [{"accountList": [{"accountId": "111", "accountName": "acct"}]}]
                        return [{"roleList": [{"roleName": "Admin"}]}]
                return P()
            def get_role_credentials(self, **k):
                return {"roleCredentials": {"accessKeyId": "AKIA", "secretAccessKey": "S",
                                            "sessionToken": "T", "expiration": 1}}

        orig_oidc, orig_sso = m._oidc, m._sso
        m._oidc, m._sso = (lambda r: OIDC()), (lambda r: SSO())
        try:
            st = m.start("https://org.awsapps.com/start", "us-west-2")
            self.assertTrue(st["success"])
            self.assertEqual(st["user_code"], "ABCD-EFGH")
            p = m.poll(st["session_id"])
            self.assertEqual(p["status"], "authorized")
            acc = m.accounts(st["session_id"])
            self.assertEqual(acc["accounts"][0]["roles"], ["Admin"])
            cred = m.credentials(st["session_id"], "111", "Admin")
            self.assertTrue(cred["success"])
            self.assertEqual(cred["access_key_id"], "AKIA")
        finally:
            m._oidc, m._sso = orig_oidc, orig_sso

if __name__ == "__main__":
    unittest.main()
