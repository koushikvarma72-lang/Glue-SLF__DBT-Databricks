"""Shared helpers for the sfglue route blueprints.

Holds the pieces every blueprint needs: access to the injected ``call_ai`` dispatch
(stored on ``app.config['CALL_AI']``), the Bedrock-credential binding (``_bind_ai`` +
``_aws_creds_from_glue``), the AI reachability probe (``_ai_preflight``), the JSON
validation layer (``body`` + ``ValidationError``), and the central error-handler
registration. Behaviour of ``_bind_ai`` / ``_aws_creds_from_glue`` / ``_ai_preflight`` is
carried over verbatim from the original monolithic ``snowflake_glue_routes``.
"""

from __future__ import annotations

import logging

from flask import current_app, jsonify, request
from werkzeug.exceptions import HTTPException

logger = logging.getLogger("sfglue.routes")


# -- AI dispatch access -------------------------------------------------------
def get_call_ai():
    """The ``call_ai`` callable injected at app setup (or None if none configured).

    Stored on ``app.config['CALL_AI']`` by ``register_snowflake_glue_routes`` so the
    blueprint handlers don't need it threaded through a closure."""
    return current_app.config.get("CALL_AI")


def _aws_creds_from_glue(glue):
    """Pull AWS credentials out of a Glue connection payload so Bedrock can reuse them.

    Returns a dict {region, profile?, access_key_id?, secret_access_key?, session_token?}
    or None if the payload has nothing usable. A named profile wins over explicit keys
    (matching the Glue client's own precedence)."""
    if not isinstance(glue, dict):
        return None
    pick = lambda *ks: next((glue[k] for k in ks if glue.get(k)), None)
    region = pick("region", "aws_region", "awsRegion")
    profile = pick("profile_name", "profile", "profileName")
    ak = pick("access_key_id", "accessKeyId", "aws_access_key_id")
    sk = pick("secret_access_key", "secretAccessKey", "aws_secret_access_key")
    st = pick("session_token", "sessionToken", "aws_session_token")
    if profile:
        return {"region": region, "profile": profile}
    if ak and sk:
        creds = {"region": region, "access_key_id": ak, "secret_access_key": sk}
        if st:
            creds["session_token"] = st
        return creds
    return {"region": region} if region else None


def _bind_ai(call_ai, glue):
    """Wrap call_ai so Bedrock uses the Glue connection's AWS creds (if any), letting the
    AI work without separately configuring the server environment. No-op when there are no
    creds or no call_ai -- returns the original callable."""
    if not call_ai:
        return call_ai
    creds = _aws_creds_from_glue(glue)
    if not creds:
        return call_ai

    def bound(prompt, *args, **kwargs):
        kwargs.setdefault("aws_creds", creds)
        return call_ai(prompt, *args, **kwargs)
    return bound


def _ai_preflight(call_ai):
    """Is an LLM actually reachable right now? Returns (ok, reason).

    A configured provider can still be unusable at request time -- the classic case is an
    expired AWS SSO token (config looks fine; every call 401s). We probe with one tiny call
    so the migration steps can refuse up front with an actionable notice instead of emitting
    deterministic scaffolds/placeholders that look like real output. Cheap: ~1 token."""
    if not call_ai:
        return False, ("No AI/LLM provider is connected. Connect a provider in Settings, "
                       "then retry -- this step needs an LLM to translate the source logic.")
    try:
        # NB: do NOT pass a tiny max_tokens -- call_ai enforces a minimum output-token budget
        # (MIN_REQUIRED_OUTPUT_TOKENS) and rejects anything below it, which would make a
        # HEALTHY provider look unreachable. max_tokens is only a ceiling; a "reply ok" prompt
        # generates ~2 tokens and stops, so omitting it (use the app default) stays cheap.
        call_ai("Reply with the single word: ok", system_prompt="Reply with exactly: ok",
                temperature=0, task="health")
        return True, None
    except Exception as exc:  # noqa: BLE001 -- probe is best-effort
        logger.warning("sfglue AI preflight failed: %s", exc)
        return False, (f"The configured LLM provider isn't reachable right now: {exc}. "
                       "Fix the connection (for AWS Bedrock, refresh your SSO login: "
                       "`aws sso login`), then retry.")


# -- JSON validation layer ----------------------------------------------------
class ValidationError(Exception):
    """Raised by :func:`body` when a required request field is missing/empty.

    The registered error handler turns it into ``({"error": message}, 400)``."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def body(*required, message=None):
    """Parse the request JSON (like the old ``request.get_json(silent=True) or {}``) and,
    when ``required`` keys are given, raise :class:`ValidationError` (-> 400) if any is
    missing or empty (falsy).

    ``message`` overrides the default ``"'<key>' is required"`` text so a handler can keep
    its exact user-facing wording while still routing the check through the validation
    layer. With no ``required`` args this is a drop-in for the bare permissive parse."""
    data = request.get_json(silent=True) or {}
    for key in required:
        if not data.get(key):
            raise ValidationError(message or f"'{key}' is required")
    return data


# -- Central error / validation handlers --------------------------------------
def register_error_handlers(app):
    """Register the safety-net handlers.

    - :class:`ValidationError` and HTTP 400 -> ``({"error": msg}, 400)``.
    - any other uncaught ``Exception`` -> ``({"error": str(e)}, 500)`` (logged).

    Real ``HTTPException``s (404/405/...) are passed through untouched so the SPA fallback
    and Flask's own routing errors behave exactly as before."""

    @app.errorhandler(ValidationError)
    def _handle_validation(exc):  # noqa: ANN001
        app.logger.info("sfglue validation rejected request: %s", exc.message)
        return jsonify({"error": exc.message}), 400

    @app.errorhandler(400)
    def _handle_bad_request(exc):  # noqa: ANN001
        msg = getattr(exc, "description", None) or "Bad request"
        return jsonify({"error": msg}), 400

    @app.errorhandler(Exception)
    def _handle_unexpected(exc):  # noqa: ANN001
        # Let Flask/werkzeug render genuine HTTP errors (404, 405, the SPA 404, ...).
        if isinstance(exc, HTTPException):
            return exc
        app.logger.exception("sfglue unhandled error on %s %s", request.method, request.path)
        return jsonify({"error": str(exc)}), 500
