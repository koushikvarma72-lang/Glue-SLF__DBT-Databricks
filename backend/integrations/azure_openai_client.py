"""Azure OpenAI Service chat integration.

Azure OpenAI is the Azure-managed equivalent of Amazon Bedrock: a managed,
enterprise LLM service (data residency, private networking, compliance) reached
through an OpenAI-compatible Chat Completions API. It differs from plain OpenAI
in two ways:

  * the model is selected by a *deployment name* baked into the URL, not the
    request body, and
  * auth is an ``api-key`` header (not ``Authorization: Bearer``).

Uses only the Python standard library (urllib) so no extra dependency is needed.
"""

import json
import urllib.error
import urllib.request

from backend.integrations.ai_client import (
    AIClientError,
    _truncate_prompt,
    build_openai_payload,
    extract_openai_text,
    iter_openai_sse,
    raise_openai_http_error,
)

_LABEL = "Azure OpenAI"
_DEFAULT_API_VERSION = "2024-10-21"


def _azure_chat_url(endpoint, deployment, api_version):
    """Build the Azure chat-completions URL from a resource endpoint + deployment.

    Accepts the endpoint with or without a trailing slash or ``/openai`` suffix,
    e.g. ``https://my-resource.openai.azure.com``.
    """
    base = (endpoint or "").strip().rstrip("/")
    if base.endswith("/openai"):
        base = base[: -len("/openai")]
    return f"{base}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"


def call_azure_openai_chat(
    deployment,
    prompt,
    api_key,
    *,
    endpoint,
    api_version=None,
    system_prompt=None,
    temperature=0,
    top_p=1,
    max_tokens=4000,
    max_prompt_chars=None,
    timeout=120,
    stream=False,
):
    """Call an Azure OpenAI deployment through the Chat Completions API.

    Returns the full response text when ``stream=False`` (default). When
    ``stream=True`` it returns a generator yielding incremental text chunks via
    SSE.
    """
    if not api_key:
        raise AIClientError(
            "No Azure OpenAI API key configured. Set AZURE_OPENAI_API_KEY in your .env (or supply one in Settings)."
        )
    if not endpoint:
        raise AIClientError("AZURE_OPENAI_ENDPOINT is not configured (e.g. https://<resource>.openai.azure.com).")
    if not deployment:
        raise AIClientError("AZURE_OPENAI_DEPLOYMENT is not configured (the model deployment name).")

    prompt = _truncate_prompt(prompt, max_prompt_chars)
    url = _azure_chat_url(endpoint, deployment, (api_version or _DEFAULT_API_VERSION).strip())
    # Azure ignores the body "model" field (the deployment in the URL selects the
    # model), but the OpenAI-compatible payload still requires one — send the
    # deployment name for clean logs.
    payload = build_openai_payload(
        deployment, prompt, system_prompt,
        temperature=temperature, top_p=top_p, max_tokens=max_tokens, stream=stream,
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"api-key": api_key, "Content-Type": "application/json"},
    )

    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise_openai_http_error(exc, _LABEL)
    except Exception as exc:  # noqa: BLE001 — surface any transport error cleanly
        raise AIClientError(f"{_LABEL} API call failed: {exc}") from exc

    if stream:
        return iter_openai_sse(response, _LABEL)

    try:
        body = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
    return extract_openai_text(body, _LABEL)
