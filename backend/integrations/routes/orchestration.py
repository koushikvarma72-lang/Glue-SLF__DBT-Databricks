"""Orchestration routes: Glue Workflows + Airflow DAGs -> Databricks Jobs
(plan/deploy/run/emit) and the workspace push. Behaviour identical."""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify
from backend.integrations.glue_client import GlueConnectionConfig, list_glue_triggers, list_glue_workflows
from backend.integrations.orchestration_migration import (
    build_databricks_job,
    deploy_job,
    job_to_dab_yaml,
    parse_workflow_dag,
)
from backend.integrations.snowflake_glue_migration import build_dbt_project_files
from backend.integrations.routes._shared import body

logger = logging.getLogger("sfglue.routes")

bp = Blueprint("sfglue_orchestration", __name__)


@bp.route('/api/sfglue/workflows/plan', methods=['POST'])
def sfglue_workflows_plan():
    """Phase 1 (gap plan): convert Glue Workflows into Databricks Jobs — plan only.

    Body: {glue, destination?, artifact_map?, notifications?, pipeline_tasks?}.
    artifact_map: {glue_job_name: {kind: notebook|dbt|framework, path/models/notebook}}
    Returns {success, jobs: [{name, dag, job, yaml, placeholders, warnings}]}.
    """
    data = body('glue', message="Connect AWS Glue first.")
    if not data.get('glue'):
        return jsonify({"success": False, "error": "Connect AWS Glue first."}), 400
    glue_config = GlueConnectionConfig.from_payload(data['glue'])
    wf = list_glue_workflows(glue_config)
    if not wf.get('success'):
        return jsonify({"success": False, "error": wf.get('error')}), 400
    trg = list_glue_triggers(glue_config)
    triggers = trg.get('triggers') or [] if trg.get('success') else []
    destination = data.get('destination') or {}
    amap = data.get('artifact_map') or {}
    notif = (data.get('notifications') or {}).get('email_notifications') or {}

    jobs_out = []
    for w in wf.get('workflows') or []:
        dag = parse_workflow_dag(w, triggers)
        built = build_databricks_job(dag, artifact_map=amap, destination=destination,
                                     email_notifications=notif or None,
                                     file_arrival_url=data.get('file_arrival_url'))
        jobs_out.append({
            "name": dag['name'], "dag": dag, "job": built['job'],
            "yaml": job_to_dab_yaml(built['job'], dag['name']),
            "placeholders": built['placeholders'], "warnings": built['warnings'],
        })
    if not jobs_out:
        return jsonify({"success": False,
                        "error": "No Glue Workflows found in this account/region."}), 404
    return jsonify({"success": True, "jobs": jobs_out})

@bp.route('/api/sfglue/workflows/deploy', methods=['POST'])
def sfglue_workflows_deploy():
    """Create/update the planned Databricks Jobs (idempotent by tags.sfglue_source).

    Body: {destination: {workspace_url, personal_access_token/token}, jobs: [job-json]}.
    """
    data = body('jobs', message="No jobs to deploy — run Plan first.")
    destination = data.get('destination') or {}
    jobs = data.get('jobs') or []
    if not jobs:
        return jsonify({"success": False, "error": "No jobs to deploy — run Plan first."}), 400
    url = destination.get('workspace_url') or destination.get('workspaceUrl') or ''
    token = (destination.get('personal_access_token') or destination.get('token')
             or destination.get('access_token') or '')
    # The Jobs API requires ABSOLUTE workspace paths for notebooks; the planned jobs
    # carry bundle-relative paths (valid inside a DAB deploy). Rewrite them under a
    # workspace root here so direct API deploys work too. Override with
    # destination.notebook_root; default keeps the bundle's src/notebooks/ layout.
    root = (destination.get('notebook_root') or '/Shared/sfglue').rstrip('/')
    warehouse = destination.get('sql_warehouse_id') or destination.get('sqlWarehouseId') or ''
    # Runtime parameters for the converted notebooks (they read these via widgets;
    # without them S3_VENDOR_BUCKET is '' and boto3 rejects the empty bucket name).
    pipeline_bucket = destination.get('pipeline_bucket') or ''
    aws_region = destination.get('aws_region') or ''
    results, ok = [], True
    for job in jobs:
        needs_env = False
        for t in job.get('tasks') or []:
            nb = t.get('notebook_task') or {}
            if nb:
                bp = nb.setdefault('base_parameters', {})
                if pipeline_bucket:
                    bp.setdefault('S3_VENDOR_BUCKET', pipeline_bucket)
                    bp.setdefault('S3_DL_BUCKET', pipeline_bucket)
                if aws_region:
                    bp.setdefault('AWS_REGION', aws_region)
                if destination.get('catalog'):
                    bp.setdefault('catalog', destination['catalog'])
            path = nb.get('notebook_path') or ''
            if path and not path.startswith('/'):
                nb['notebook_path'] = f"{root}/{path}"
            dbt = t.get('dbt_task') or {}
            if dbt:
                proj = dbt.get('project_directory') or ''
                if proj and not proj.startswith('/'):
                    dbt['project_directory'] = f"{root}/{proj}"
                # Serverless workspaces require an environment on command tasks, and
                # dbt needs a SQL warehouse to build against.
                t.setdefault('environment_key', 'sfglue_serverless')
                if warehouse:
                    dbt.setdefault('warehouse_id', warehouse)
                needs_env = True
        if needs_env and not job.get('environments'):
            job['environments'] = [{
                'environment_key': 'sfglue_serverless',
                'spec': {'client': '2', 'dependencies': ['dbt-databricks']},
            }]
        r = deploy_job(job, workspace_url=url, token=token)
        ok = ok and r.get('success', False)
        if not r.get('success'):
            logger.warning("sfglue deploy: job %r FAILED: %s",
                           job.get('name'), r.get('error') or r)
        results.append({"name": job.get('name'), **r})
    return jsonify({"success": ok, "results": results,
                    "notebook_root": root}), (200 if ok else 502)

@bp.route('/api/sfglue/workflows/run', methods=['POST'])
def sfglue_workflows_run():
    """The workflow dry-run gate: trigger a deployed Job and wait for the verdict.

    Body: {destination, job_id, timeout_seconds?}. Returns per-task result states —
    a SUCCESS here is the Phase-1 'workflow verified' signal.
    """
    from backend.integrations.workspace_push import run_job_and_wait
    data = body('job_id', message="job_id is required — deploy first.")
    destination = data.get('destination') or {}
    job_id = data.get('job_id')
    if not job_id:
        return jsonify({"success": False, "error": "job_id is required — deploy first."}), 400
    result = run_job_and_wait(
        job_id,
        workspace_url=destination.get('workspace_url') or destination.get('workspaceUrl') or '',
        token=(destination.get('personal_access_token') or destination.get('token')
               or destination.get('access_token') or ''),
        timeout_seconds=int(data.get('timeout_seconds') or 1800))
    return jsonify(result), (200 if result.get('success') else 502)

@bp.route('/api/sfglue/airflow/plan', methods=['POST'])
def sfglue_airflow_plan():
    """Airflow DAGs → Databricks Jobs (plan only) — the Airflow twin of
    /workflows/plan, feeding the same converter/serializers/deploy path.

    Body: {airflow: {base_url?, username?, password?, token?,
                     dag_files?: {name: python_or_yaml_source}},
           destination?, artifact_map?, notifications?}.
    dag_files entries are auto-detected: Python DAG modules go through the AST
    parser, dag-factory YAML definitions through the YAML parser (one YAML file
    may define several DAGs). GlueJobOperator tasks carry the Glue job_name as
    legacy_name, so the same artifact_map used for Glue workflows applies unchanged.
    """
    from backend.integrations.airflow_migration import (
        fetch_airflow_dags, looks_like_yaml_dag, parse_dag_factory_yaml, parse_dag_source)
    data = body()
    af = data.get('airflow') or {}
    dags = []
    if af.get('dag_files'):
        for fname, src in af['dag_files'].items():
            text = str(src or '')
            if looks_like_yaml_dag(str(fname), text):
                dags.extend(parse_dag_factory_yaml(str(fname), text))
            else:
                dags.append(parse_dag_source(str(fname), text))
    elif af.get('base_url'):
        fetched = fetch_airflow_dags(af['base_url'], af.get('username') or '',
                                     af.get('password') or '', af.get('token') or '')
        if not fetched.get('success'):
            return jsonify(fetched), 400
        dags = fetched['dags']
    else:
        return jsonify({"success": False,
                        "error": "Provide airflow.dag_files (paste DAG source) "
                                 "or airflow.base_url (+credentials)."}), 400
    destination = data.get('destination') or {}
    amap = data.get('artifact_map') or {}
    notif = (data.get('notifications') or {}).get('email_notifications') or {}
    jobs_out = []
    for dag in dags:
        built = build_databricks_job(dag, artifact_map=amap, destination=destination,
                                     email_notifications=notif or None,
                                     file_arrival_url=data.get('file_arrival_url'))
        jobs_out.append({
            "name": dag['name'], "dag": dag, "job": built['job'],
            "yaml": job_to_dab_yaml(built['job'], dag['name']),
            "placeholders": built['placeholders'], "warnings": built['warnings'],
            "source": "airflow",
        })
    if not jobs_out:
        return jsonify({"success": False, "error": "No DAGs found/parsed."}), 404
    return jsonify({"success": True, "jobs": jobs_out, "source": "airflow"})

@bp.route('/api/sfglue/airflow/emit', methods=['POST'])
def sfglue_airflow_emit():
    """Emit a TARGET Airflow DAG (dag-factory YAML) that orchestrates the MIGRATED
    pipeline on Databricks + dbt — the mirror of /airflow/plan. Airflow here drives
    Databricks notebook tasks + per-layer dbt tasks (staging/intermediate/marts),
    never the retired Glue jobs.

    Body: {artifacts (conversion), destination, dag_id?, schedule?,
           databricks_conn_id?, notebook_root?, file_arrival_path?}.
    """
    from backend.integrations.airflow_migration import emit_target_airflow_yaml
    data = body()
    artifacts = data.get('artifacts') or {}
    if not (artifacts.get('dbt_models') or artifacts.get('notebooks')):
        return jsonify({"success": False,
                        "error": "No conversion artifacts — run Convert first."}), 400
    out = emit_target_airflow_yaml(
        artifacts, data.get('destination') or {},
        dag_id=data.get('dag_id') or 'cdl_migrated_databricks',
        schedule=data.get('schedule') or '0 2 * * *',
        databricks_conn_id=data.get('databricks_conn_id') or 'databricks_default',
        notebook_root=data.get('notebook_root') or '/Shared/sfglue',
        file_arrival_path=data.get('file_arrival_path'),
        dbt_source=data.get('dbt_source') or 'workspace',
        git_url=data.get('git_url'), git_branch=data.get('git_branch') or 'main',
        dbt_cloud_conn_id=data.get('dbt_cloud_conn_id') or 'dbt_cloud_default',
        dbt_cloud_job_id=data.get('dbt_cloud_job_id'),
        # provider_free=True emits a BashOperator DAG (via run_databricks_task.py)
        # that runs on an Airflow without the databricks provider — the reference
        # 2.10.5 setup. Default stays provider-based for backwards compatibility.
        provider_free=bool(data.get('provider_free')),
        helper_path=data.get('helper_path') or 'run_databricks_task.py',
        env_host_var=data.get('env_host_var') or 'DATABRICKS_HOST',
        env_token_var=data.get('env_token_var') or 'DATABRICKS_TOKEN')
    return jsonify({"success": True, **out})

@bp.route('/api/sfglue/workspace/push', methods=['POST'])
def sfglue_workspace_push():
    """Push the converted notebooks + dbt project into the workspace so the
    deployed orchestration Job can run — automates `workspace import-dir`.

    Body: {destination, artifacts, root?}. Layout matches the bundle/deploy
    convention: <root>/src/notebooks/ + <root>/dbt/.
    """
    from backend.integrations.workspace_push import build_push_plan, push_to_workspace
    data = body()
    destination = data.get('destination') or {}
    artifacts = data.get('artifacts') or {}
    if not (artifacts.get('notebooks') or artifacts.get('dbt_models')):
        return jsonify({"success": False,
                        "error": "No artifacts to push — run Convert first."}), 400
    root = data.get('root') or destination.get('notebook_root') or '/Shared/sfglue'
    dbt_files = build_dbt_project_files(artifacts, destination,
                                        data.get('project_name') or 'sfglue_local_run')
    conf_files = data.get('conf_files') or artifacts.get('conf_files') or {}
    plan = build_push_plan(artifacts, dbt_files, root, conf_files=conf_files)
    result = push_to_workspace(
        plan,
        workspace_url=destination.get('workspace_url') or destination.get('workspaceUrl') or '',
        token=(destination.get('personal_access_token') or destination.get('token')
               or destination.get('access_token') or ''))
    result['root'] = root
    if not result.get('success'):
        for item in (result.get('results') or []):
            if item.get('status') != 'ok':
                logger.warning("sfglue push: %r FAILED: %s",
                               item.get('path') or item.get('name'),
                               item.get('error') or item)
        if result.get('error'):
            logger.warning("sfglue push failed: %s", result['error'])
    return jsonify(result), (200 if result.get('success') else 502)
