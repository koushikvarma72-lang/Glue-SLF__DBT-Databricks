"""Back-compat shim.

The sfglue routes were split from this single ~1,965-line module into per-domain
Flask blueprints under ``backend.integrations.routes``. This module is kept only so
existing imports (``from backend.integrations.snowflake_glue_routes import
register_snowflake_glue_routes``) keep resolving — it re-exports the real entry
point from the routes package. Prefer importing from ``backend.integrations.routes``
directly in new code.
"""

from backend.integrations.routes import register_snowflake_glue_routes

__all__ = ["register_snowflake_glue_routes"]
