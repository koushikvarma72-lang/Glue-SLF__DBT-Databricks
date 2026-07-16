"""dl_common_lib_function — reconstructed shared library for the reference CDL Glue jobs.

Rebuilt from the exact call surface the job scripts use (the originals were lost
from S3). Every function below matches how load_confiq / parent_batch_open/close /
landing_to_raw / raw_to_curated / curated_to_publish / publish_to_snowflake call it:

    from dl_common_lib_function import get_dl_common_functions as dl_lib
    dl_lib.initiate_logger() / initiate_glue_client() / initiate_spark_session()
    dl_lib.get_secret_manager(name, region)
    dl_lib.read_from_db(secret=..., tbl_query="(select ...) query_wrap")
    dl_lib.get_postgres_conn_for_psycopg2(secret)      -> (conn, user)
    dl_lib.send_sns_notify(TopicArn, Message, Subject)

The control-DB "secret" dict comes from control_connection.resolve_control_connection
or Secrets Manager; key names vary, so lookups are tolerant (username/user,
dbname/database/dbInstanceIdentifier, host, port, password, or a full jdbc url).
"""

import json
import logging
import sys


class get_dl_common_functions:  # noqa: N801 — name preserved from the original import
    _logger = None
    _spark = None

    # ── infrastructure ───────────────────────────────────────────────────────
    @staticmethod
    def initiate_logger():
        cls = get_dl_common_functions
        if cls._logger is None:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                                format="%(asctime)s %(levelname)s %(name)s %(message)s")
            cls._logger = logging.getLogger("dl_common")
        return cls._logger

    @staticmethod
    def initiate_glue_client():
        import boto3
        return boto3.client("glue")

    @staticmethod
    def initiate_spark_session():
        cls = get_dl_common_functions
        if cls._spark is None:
            from pyspark.sql import SparkSession
            cls._spark = SparkSession.builder.getOrCreate()
        return cls._spark

    # ── secrets ──────────────────────────────────────────────────────────────
    @staticmethod
    def get_secret_manager(secret_name, region_name):
        import boto3
        client = boto3.client("secretsmanager", region_name=region_name)
        value = client.get_secret_value(SecretId=secret_name)
        raw = value.get("SecretString") or "{}"
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return {"SecretString": raw}

    # ── control-DB access ────────────────────────────────────────────────────
    @staticmethod
    def _pg_params(secret):
        s = secret or {}
        pick = lambda *ks, d="": next((str(s[k]) for k in ks if s.get(k) not in (None, "")), d)
        url = pick("jdbc_url", "url")
        if url.startswith("jdbc:postgresql://"):
            import re
            m = re.match(r"jdbc:postgresql://([^:/]+):?(\d*)/([^?]+)", url)
            if m:
                return {"host": m.group(1), "port": int(m.group(2) or 5432),
                        "dbname": m.group(3),
                        "user": pick("username", "user"), "password": pick("password")}
        return {
            "host": pick("host", "hostname"),
            "port": int(pick("port", d="5432") or 5432),
            "dbname": pick("dbname", "database", "db", "dbInstanceIdentifier", d="control"),
            "user": pick("username", "user"),
            "password": pick("password"),
        }

    @staticmethod
    def read_from_db(secret=None, tbl_query=""):
        """Run a sub-select ("(...) query_wrap") against the control DB via Spark JDBC."""
        p = get_dl_common_functions._pg_params(secret)
        spark = get_dl_common_functions.initiate_spark_session()
        return (spark.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{p['host']}:{p['port']}/{p['dbname']}")
                .option("dbtable", tbl_query)
                .option("user", p["user"])
                .option("password", p["password"])
                .option("driver", "org.postgresql.Driver")
                .load())

    @staticmethod
    def get_postgres_conn_for_psycopg2(secret=None):
        p = get_dl_common_functions._pg_params(secret)
        try:
            import psycopg2
        except ImportError:  # Glue 4/5 pythonshell fallback name
            import psycopg2_binary as psycopg2  # noqa: F401
        conn = psycopg2.connect(host=p["host"], port=p["port"], dbname=p["dbname"],
                                user=p["user"], password=p["password"])
        return conn, p["user"]

    # ── notifications ────────────────────────────────────────────────────────
    @staticmethod
    def send_sns_notify(TopicArn, Message, Subject):
        """SNS publish — NON-FATAL by design: a missing/dummy topic in the demo
        environment must never fail a pipeline run."""
        log = get_dl_common_functions.initiate_logger()
        try:
            import boto3
            region = None
            if isinstance(TopicArn, str) and TopicArn.count(":") >= 4:
                region = TopicArn.split(":")[3] or None
            sns = boto3.client("sns", region_name=region) if region else boto3.client("sns")
            sns.publish(TopicArn=TopicArn, Message=str(Message), Subject=str(Subject)[:100])
            log.info("SNS notification sent: %s", Subject)
        except Exception as exc:  # noqa: BLE001
            log.warning("SNS notification skipped (%s): %s", Subject, exc)
