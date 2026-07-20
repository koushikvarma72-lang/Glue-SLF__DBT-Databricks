"""sfglue HTTP layer — Flask blueprints.

The route handlers used to live as ~32 nested closures inside one 1,965-line
``register_snowflake_glue_routes`` function. They're now split into cohesive
blueprints by domain (one module each), wired up here. Behaviour is unchanged:
every route keeps its exact path, method, and response shape.

    connections   — snowflake/glue/postgres/databricks test-connection,
                    snowflake/schemas, aws/buckets
    sso           — aws/sso/{start,poll,accounts,credentials}
    lineage       — lineage, lineage/operational
    review        — review, explain, grade
    convert       — convert, precheck, export
    deploy        — deploy, build, seed-bronze, reconcile, run-tests
    orchestration — workspace/push, workflows/*, airflow/*
    dbt_local     — dbt-local/{run-sfglue,status,cancel}

``register_snowflake_glue_routes(app, call_ai)`` stays the public entry point
(imported by ``backend.app``): it stashes ``call_ai`` on ``app.config`` for the
handlers to read, registers the central error/validation handlers, then mounts
every blueprint.
"""

from __future__ import annotations

from backend.integrations.routes import (
    connections,
    convert,
    dbt_local,
    deploy,
    lineage,
    orchestration,
    review,
    sso,
)
from backend.integrations.routes._shared import register_error_handlers

_BLUEPRINTS = (
    connections.bp,
    sso.bp,
    lineage.bp,
    review.bp,
    convert.bp,
    deploy.bp,
    orchestration.bp,
    dbt_local.bp,
)


def register_snowflake_glue_routes(app, call_ai=None):
    """Wire the sfglue HTTP layer onto ``app``.

    Stores ``call_ai`` on ``app.config['CALL_AI']`` (blueprint handlers read it via
    ``_shared.get_call_ai``), installs the safety-net error handlers, and registers
    every domain blueprint. Idempotent-safe for a single app instance.
    """
    app.config["CALL_AI"] = call_ai
    register_error_handlers(app)
    for bp in _BLUEPRINTS:
        app.register_blueprint(bp)
    return app
