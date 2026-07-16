"""AWS IAM Identity Center (SSO) device-authorization login — "Sign in with AWS".

The same flow `aws sso login` uses, embedded in the app:

    start()        RegisterClient + StartDeviceAuthorization
                     → user opens verification URL, approves in the browser
    poll()         CreateToken(device_code) until authorized → SSO access token
    accounts()     ListAccounts + ListAccountRoles → user picks account/role
    credentials()  GetRoleCredentials → ~1h temp access key / secret / session token

Security posture: the long-lived (~8h) SSO access token NEVER leaves this process —
sessions are held in an in-memory dict keyed by a random session id. Only the
short-lived role credentials are returned to the caller (they slot into the same
config fields the app already uses). No app registration on the AWS side is needed:
RegisterClient creates a public OIDC client dynamically.
"""

from __future__ import annotations

import logging
import secrets
import time

logger = logging.getLogger(__name__)

# session_id -> {region, client_id, client_secret, device_code, interval,
#                expires_at, access_token?, token_expires_at?}
_SESSIONS: dict[str, dict] = {}
_MAX_SESSIONS = 20


def _oidc(region: str):
    import boto3
    return boto3.client("sso-oidc", region_name=region)


def _sso(region: str):
    import boto3
    return boto3.client("sso", region_name=region)


def _prune():
    now = time.time()
    dead = [k for k, s in _SESSIONS.items()
            if now > s.get("expires_at", 0) and now > s.get("token_expires_at", 0)]
    for k in dead:
        _SESSIONS.pop(k, None)
    while len(_SESSIONS) > _MAX_SESSIONS:
        _SESSIONS.pop(next(iter(_SESSIONS)), None)


def start(start_url: str, region: str) -> dict:
    """Begin the device flow. Returns the verification URL + user code."""
    start_url = str(start_url or "").strip()
    region = str(region or "").strip() or "us-east-1"
    if not start_url.startswith("https://"):
        return {"success": False,
                "error": "Enter your Identity Center start URL (https://<org>.awsapps.com/start)."}
    _prune()
    try:
        oidc = _oidc(region)
        reg = oidc.register_client(clientName="sfglue-migration-app", clientType="public")
        auth = oidc.start_device_authorization(
            clientId=reg["clientId"], clientSecret=reg["clientSecret"], startUrl=start_url)
        sid = secrets.token_urlsafe(24)
        _SESSIONS[sid] = {
            "region": region,
            "client_id": reg["clientId"], "client_secret": reg["clientSecret"],
            "device_code": auth["deviceCode"],
            "interval": int(auth.get("interval", 5)),
            "expires_at": time.time() + int(auth.get("expiresIn", 600)),
        }
        return {"success": True, "session_id": sid,
                "verification_uri": auth.get("verificationUriComplete") or auth.get("verificationUri"),
                "user_code": auth.get("userCode", ""),
                "interval": int(auth.get("interval", 5)),
                "expires_in": int(auth.get("expiresIn", 600))}
    except Exception as exc:  # noqa: BLE001
        logger.warning("SSO start failed: %s", exc)
        return {"success": False, "error": f"SSO start failed: {exc}"}


def poll(session_id: str) -> dict:
    """One CreateToken attempt → pending | authorized | error. Frontend calls on a timer."""
    s = _SESSIONS.get(str(session_id or ""))
    if not s:
        return {"success": False, "error": "Unknown or expired SSO session — start again."}
    if time.time() > s["expires_at"] and not s.get("access_token"):
        _SESSIONS.pop(session_id, None)
        return {"success": False, "error": "Login window expired — start again."}
    if s.get("access_token"):
        return {"success": True, "status": "authorized"}
    try:
        tok = _oidc(s["region"]).create_token(
            clientId=s["client_id"], clientSecret=s["client_secret"],
            grantType="urn:ietf:params:oauth:grant-type:device_code",
            deviceCode=s["device_code"])
        s["access_token"] = tok["accessToken"]
        s["token_expires_at"] = time.time() + int(tok.get("expiresIn", 8 * 3600))
        return {"success": True, "status": "authorized"}
    except Exception as exc:  # noqa: BLE001
        name = type(exc).__name__
        if "AuthorizationPending" in name or "SlowDown" in name:
            return {"success": True, "status": "pending"}
        if "Expired" in name:
            _SESSIONS.pop(session_id, None)
            return {"success": False, "error": "Login window expired — start again."}
        return {"success": False, "error": f"SSO login failed: {exc}"}


def accounts(session_id: str) -> dict:
    """List accounts + their roles for the authorized session."""
    s = _SESSIONS.get(str(session_id or ""))
    if not s or not s.get("access_token"):
        return {"success": False, "error": "Not authorized yet."}
    try:
        sso = _sso(s["region"])
        out = []
        pager = sso.get_paginator("list_accounts")
        for page in pager.paginate(accessToken=s["access_token"]):
            for acct in page.get("accountList", []):
                roles = []
                rp = sso.get_paginator("list_account_roles")
                for rpage in rp.paginate(accessToken=s["access_token"],
                                         accountId=acct["accountId"]):
                    roles.extend(r["roleName"] for r in rpage.get("roleList", []))
                out.append({"account_id": acct["accountId"],
                            "account_name": acct.get("accountName", ""),
                            "email": acct.get("emailAddress", ""),
                            "roles": roles})
        return {"success": True, "accounts": out}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Listing accounts failed: {exc}"}


def credentials(session_id: str, account_id: str, role_name: str) -> dict:
    """GetRoleCredentials → short-lived keys (the only thing that leaves the server)."""
    s = _SESSIONS.get(str(session_id or ""))
    if not s or not s.get("access_token"):
        return {"success": False, "error": "Not authorized yet."}
    try:
        r = _sso(s["region"]).get_role_credentials(
            accessToken=s["access_token"], accountId=str(account_id), roleName=str(role_name))
        c = r["roleCredentials"]
        return {"success": True,
                "access_key_id": c["accessKeyId"],
                "secret_access_key": c["secretAccessKey"],
                "session_token": c["sessionToken"],
                "expiration_ms": int(c.get("expiration", 0)),
                "region": s["region"]}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Getting role credentials failed: {exc}"}
