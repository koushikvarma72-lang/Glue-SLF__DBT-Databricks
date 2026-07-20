"""AWS SSO device-flow login ("Sign in with AWS") routes -- thin pass-throughs to
``backend.integrations.aws_sso_auth``. Behaviour identical to the original handlers."""

from __future__ import annotations

from flask import Blueprint, jsonify
from backend.integrations.routes._shared import body

bp = Blueprint("sfglue_sso", __name__)


@bp.route('/api/aws/sso/start', methods=['POST'])
def aws_sso_start():
    from backend.integrations.aws_sso_auth import start as sso_start
    data = body()
    result = sso_start(data.get('start_url') or '', data.get('region') or '')
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/aws/sso/poll', methods=['POST'])
def aws_sso_poll():
    from backend.integrations.aws_sso_auth import poll as sso_poll
    data = body()
    result = sso_poll(data.get('session_id') or '')
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/aws/sso/accounts', methods=['POST'])
def aws_sso_accounts():
    from backend.integrations.aws_sso_auth import accounts as sso_accounts
    data = body()
    result = sso_accounts(data.get('session_id') or '')
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/aws/sso/credentials', methods=['POST'])
def aws_sso_credentials():
    from backend.integrations.aws_sso_auth import credentials as sso_credentials
    data = body()
    result = sso_credentials(data.get('session_id') or '',
                             data.get('account_id') or '', data.get('role_name') or '')
    return jsonify(result), (200 if result.get('success') else 400)
