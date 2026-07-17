"""Refresh the Databricks `aws_s3` secret scope from your CURRENT local AWS session.

SSO-only setups have no long-lived keys: the migrated ingestion notebooks read AWS
credentials from the `aws_s3` secret scope, and SSO session credentials expire (~1h),
producing `InvalidToken` on S3 reads. This script copies whatever credentials your
local AWS session currently resolves (profile / SSO cache) into the scope — run it
after `aws sso login`, before running the migrated Job.

Usage:
    aws sso login    # if the session is stale
    python3 update_databricks_s3_secrets.py \
        --host https://dbc-xxxx.cloud.databricks.com --token dapiXXXX
"""

import argparse
import json
import urllib.request


def _post(host, token, path, body):
    req = urllib.request.Request(f"{host.rstrip('/')}{path}",
                                 data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r) if r.length else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:300]
        if "RESOURCE_ALREADY_EXISTS" in detail:
            return {"already_exists": True}
        raise RuntimeError(f"{path} failed: {e.code} {detail}") from e


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--scope", default="aws_s3")
    a = ap.parse_args()

    import boto3
    creds = boto3.session.Session().get_credentials()
    if creds is None:
        raise SystemExit("No AWS credentials resolved — run `aws sso login` first "
                         "(and export AWS_PROFILE if needed).")
    frozen = creds.get_frozen_credentials()

    _post(a.host, a.token, "/api/2.0/secrets/scopes/create", {"scope": a.scope})
    pairs = {"aws_access_key_id": frozen.access_key,
             "aws_secret_access_key": frozen.secret_key}
    if frozen.token:
        pairs["aws_session_token"] = frozen.token
    for key, val in pairs.items():
        _post(a.host, a.token, "/api/2.0/secrets/put",
              {"scope": a.scope, "key": key, "string_value": val})
    print(f"✓ scope '{a.scope}' refreshed with current session credentials "
          f"({'incl. session token' if frozen.token else 'long-lived keys'}).")
    print("Note: SSO session creds expire in ~1h — rerun this before each Job run,")
    print("or attach an instance profile / storage credential for a permanent fix.")


if __name__ == "__main__":
    main()
