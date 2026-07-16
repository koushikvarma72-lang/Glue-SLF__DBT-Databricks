"""Phase 0 unit tests — pure functions only (no AWS/Postgres/warehouse needed).

Run from the repo root:
    python -m unittest backend.tests.test_phase0 -v
"""

import json
import unittest

from backend.integrations.bundle_export import (
    SCHEMA_VERSION,
    build_artifact_registry,
    build_bundle_files,
    build_state_manifest,
)
from backend.integrations.glue_client import (
    _normalize_crawler,
    _normalize_trigger,
    _normalize_workflow,
)
from backend.integrations.postgres_client import classify_framework_table, mask_row


class TestGlueNormalizers(unittest.TestCase):
    def test_trigger_conditional(self):
        raw = {
            "Name": "after_landing", "WorkflowName": "cdl_ingest", "Type": "CONDITIONAL",
            "State": "ACTIVATED",
            "Actions": [{"JobName": "raw_to_curated"}],
            "Predicate": {"Logical": "AND", "Conditions": [
                {"LogicalOperator": "EQUALS", "JobName": "landing_to_raw", "State": "SUCCEEDED"}]},
        }
        t = _normalize_trigger(raw)
        self.assertEqual(t["type"], "CONDITIONAL")
        self.assertEqual(t["workflow_name"], "cdl_ingest")
        self.assertEqual(t["actions"], [{"job_name": "raw_to_curated", "crawler_name": ""}])
        self.assertEqual(t["predicate_logical"], "AND")
        self.assertEqual(t["conditions"][0]["job_name"], "landing_to_raw")
        self.assertEqual(t["conditions"][0]["state"], "SUCCEEDED")

    def test_trigger_scheduled_and_empty(self):
        t = _normalize_trigger({"Name": "nightly", "Type": "SCHEDULED",
                                "Schedule": "cron(0 2 * * ? *)"})
        self.assertEqual(t["schedule"], "cron(0 2 * * ? *)")
        self.assertEqual(t["actions"], [])
        self.assertEqual(t["predicate_logical"], "")
        # Defensive on totally empty input.
        self.assertEqual(_normalize_trigger({})["conditions"], [])

    def test_crawler_targets_and_string_schedule(self):
        raw = {
            "Name": "landing_crawler", "DatabaseName": "cdl_landing", "TablePrefix": "lnd_",
            "Schedule": "cron(0 1 * * ? *)",  # API sometimes returns the bare string
            "Targets": {
                "S3Targets": [{"Path": "s3://gmb-cdl-dev/landing/commercial/"}],
                "JdbcTargets": [{"Path": "db/schema/%", "ConnectionName": "rds_conn"}],
                "CatalogTargets": [{"DatabaseName": "cat_db", "Tables": ["a", "b"]}],
            },
        }
        c = _normalize_crawler(raw)
        self.assertEqual(c["schedule"], "cron(0 1 * * ? *)")
        kinds = [t["kind"] for t in c["targets"]]
        self.assertEqual(kinds, ["s3", "jdbc", "catalog"])
        self.assertEqual(c["targets"][0]["path"], "s3://gmb-cdl-dev/landing/commercial/")
        self.assertEqual(c["targets"][2]["tables"], ["a", "b"])

    def test_workflow_graph(self):
        raw = {
            "Name": "cdl_ingest",
            "Graph": {
                "Nodes": [
                    {"UniqueId": "n1", "Type": "TRIGGER", "Name": "start"},
                    {"UniqueId": "n2", "Type": "JOB", "Name": "landing_to_raw"},
                ],
                "Edges": [{"SourceId": "n1", "DestinationId": "n2"}],
            },
        }
        w = _normalize_workflow(raw)
        self.assertEqual(w["name"], "cdl_ingest")
        self.assertEqual(w["nodes"][0]["type"], "trigger")
        self.assertEqual(w["edges"], [{"source": "n1", "target": "n2"}])


class TestFrameworkClassifier(unittest.TestCase):
    def test_match_by_name(self):
        for name, canonical in [
            ("configuration_master", "configuration_master"),
            ("CONFIG_MASTER", "configuration_master"),
            ("file_process_log", "file_process_log"),
            ("parent_batch_process", "parent_batch_process"),
            ("cdl_ingestion_log", "cdl_ingestion_log"),
            ("query_configuration", "query_configuration"),
            ("dq_rules", "dq_rules"),
            ("message_template", "message_template"),
            ("cdl_ds_snowflake_replicate", "cdl_ds_snowflake_replicate"),
        ]:
            got = classify_framework_table(name, [])
            self.assertIsNotNone(got, f"{name} should classify")
            self.assertEqual(got["canonical"], canonical, name)
            self.assertEqual(got["matched_by"], "name")

    def test_match_by_column_fingerprint(self):
        got = classify_framework_table(
            "tbl_rules_commercial",
            ["rule_name", "rule_type", "table_name", "severity", "created_at"])
        self.assertIsNotNone(got)
        self.assertEqual(got["canonical"], "dq_rules")
        self.assertEqual(got["matched_by"], "columns")

    def test_no_match(self):
        self.assertIsNone(classify_framework_table("customers", ["id", "name", "email"]))
        # Two hint columns are not enough for a fingerprint match.
        self.assertIsNone(classify_framework_table("misc", ["rule_name", "severity"]))

    def test_mask_row(self):
        cols = ["id", "sftp_password", "api_key", "note"]
        row = [1, "hunter2", "sk-123", "ok"]
        self.assertEqual(mask_row(cols, row), [1, "***", "***", "ok"])
        # None/empty secrets stay as-is (nothing to leak).
        self.assertEqual(mask_row(cols, [1, None, "", "ok"]), [1, None, "", "ok"])


class TestBundleExport(unittest.TestCase):
    CONVERSION = {
        "dbt_models": {"stg_orders.sql": "select 1", "fct_sales.sql": "select 2"},
        "ddl": {"orders": "CREATE TABLE ..."},
        "notebooks": {"bronze_ingest_orders.py": "# pyspark"},
        "sources_yml": "version: 2",
        "schema_yml": "version: 2",
    }
    DESTINATION = {"workspace_url": "https://dbc-123.cloud.databricks.com",
                   "catalog": "cdl", "sql_warehouse_id": "abc123"}

    def test_registry_typed_and_ordered(self):
        reg = build_artifact_registry(self.CONVERSION)
        ids = [r["id"] for r in reg]
        self.assertIn("dbt_model:fct_sales.sql", ids)
        self.assertIn("ddl:orders", ids)
        self.assertIn("notebook:bronze_ingest_orders.py", ids)
        self.assertIn("sources_yml:sources_yml", ids)
        # deterministic: sorted within type, types in fixed order
        self.assertEqual(reg, build_artifact_registry(dict(self.CONVERSION)))
        self.assertLess(ids.index("dbt_model:fct_sales.sql"), ids.index("dbt_model:stg_orders.sql"))

    def test_state_manifest_versioned(self):
        m = json.loads(build_state_manifest(self.CONVERSION))
        self.assertEqual(m["schemaVersion"], SCHEMA_VERSION)
        self.assertTrue(m["artifacts"])

    def test_bundle_layout(self):
        files = build_bundle_files(self.CONVERSION, self.DESTINATION, "My Migration!",
                                   dbt_files={"dbt_project.yml": "name: x",
                                              "models/stg_orders.sql": "select 1"})
        self.assertIn("databricks.yml", files)
        self.assertIn("resources/sfglue_bronze_job.yml", files)
        self.assertIn("src/notebooks/bronze_ingest_orders.py", files)
        self.assertIn("dbt/dbt_project.yml", files)
        self.assertIn("dbt/models/stg_orders.sql", files)
        self.assertIn("sfglue_state.json", files)
        self.assertIn("README.md", files)
        # name sanitized
        self.assertIn('name: "my_migration"', files["databricks.yml"])
        # targets present
        for target in ("dev:", "test:", "prod:"):
            self.assertIn(target, files["databricks.yml"])
        # job references the notebook relative to resources/
        self.assertIn("../src/notebooks/bronze_ingest_orders.py",
                      files["resources/sfglue_bronze_job.yml"])
        self.assertIn("task_key: bronze_ingest_orders", files["resources/sfglue_bronze_job.yml"])

    def test_bundle_without_notebooks(self):
        files = build_bundle_files({"dbt_models": {"m.sql": "select 1"}}, {}, None)
        self.assertIn("src/notebooks/README", files)
        self.assertIn("task_key: placeholder", files["resources/sfglue_bronze_job.yml"])


class TestSqlGuard(unittest.TestCase):
    """Regression: generated DDL carries a '-- migrated from …' header, which the
    guard must not mistake for a missing/forbidden verb (it reported "got 'empty'"
    and blocked every deploy)."""

    def test_leading_comment_create_accepted(self):
        from backend.integrations.sfglue_sql_guard import assert_safe_ddl
        assert_safe_ddl("-- migrated from VEEVA_CRM.PUB.PUB_ACCOUNT_DIM\n"
                        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (`ID` STRING) USING DELTA;")
        assert_safe_ddl("/* header */ CREATE TABLE t (a INT)")
        assert_safe_ddl("CREATE TABLE t (a INT);\n-- trailing note")

    def test_attacks_still_rejected(self):
        from backend.integrations.sfglue_sql_guard import UnsafeSqlError, assert_safe_ddl
        for bad in ("DROP TABLE t",
                    "-- sneaky\nDROP TABLE t",
                    "CREATE TABLE t (a INT); DROP TABLE u",
                    "",
                    "-- only a comment"):
            with self.assertRaises(UnsafeSqlError, msg=bad):
                assert_safe_ddl(bad)


if __name__ == "__main__":
    unittest.main()
