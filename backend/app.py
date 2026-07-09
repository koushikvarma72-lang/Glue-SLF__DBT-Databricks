"""Standalone Flask app — sfglue: Snowflake + AWS Glue → Databricks + dbt migration.

Split out from the BI Migration Tool. This app hosts ONLY the sfglue flow: it registers the
sfglue routes and serves the sfglue frontend. The one shared dependency (the AI dispatch) is a
lean local `call_ai` built on the copied provider clients — no coupling to the BI app.
"""
import logging
import os

from flask import Flask, send_from_directory
from flask_cors import CORS

from backend.call_ai import call_ai
from backend.integrations.snowflake_glue_routes import register_snowflake_glue_routes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("sfglue")

_HERE = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIST = os.path.join(os.path.dirname(_HERE), "frontend", "dist")


def create_app():
    app = Flask(__name__, static_folder=None)
    CORS(app)

    register_snowflake_glue_routes(app, call_ai=call_ai)

    @app.route("/api/health")
    def health():
        return {"status": "ok", "app": "sfglue"}

    # Serve the built SPA (frontend/dist). Unknown non-/api paths fall back to index.html
    # so client-side hash routing works.
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def spa(path):
        if path.startswith("api/"):
            return {"error": "not found"}, 404
        candidate = os.path.join(FRONTEND_DIST, path)
        if path and os.path.isfile(candidate):
            return send_from_directory(FRONTEND_DIST, path)
        index = os.path.join(FRONTEND_DIST, "index.html")
        if os.path.isfile(index):
            return send_from_directory(FRONTEND_DIST, "index.html")
        return ("sfglue frontend not built — run `npm install && npm run build` in "
                "sfglue_app/frontend.", 200)

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5060"))
    logger.info("sfglue app on http://0.0.0.0:%d", port)
    app.run(host="0.0.0.0", port=port, debug=True)
