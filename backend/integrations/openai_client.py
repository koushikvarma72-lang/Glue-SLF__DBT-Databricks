"""Generic OpenAI-compatible chat integration.

Speaks the standard Chat Completions protocol with ``Authorization: Bearer``
auth and a configurable base URL, so a single client serves several major
providers:

  * OpenAI       — base ``https://api.openai.com/v1``
  * Google Gemini — base ``https://generativelanguage.googleapis.com/v1beta/openai``
                    (Google's OpenAI-compatible endpoint)
  * any other OpenAI-compatible gateway (Together, Groq, a local server, …)

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


def call_openai_compatible_chat(
    model,
    prompt,
    api_key,
    *,
    base_url,
    provider_label="OpenAI",
    system_prompt=None,
    temperature=0,
    top_p=1,
    max_tokens=4000,
    max_prompt_chars=None,
    timeout=120,
    stream=False,
    extra_headers=None,
):
    """Call an OpenAI-compatible Chat Completions endpoint.

    ``base_url`` is the API root (e.g. ``https://api.openai.com/v1``);
    ``/chat/completions`` is appended. ``provider_label`` only affects log/error
    text. Returns the full response text when ``stream=False`` (default), or a
    generator yielding incremental text chunks when ``stream=True``.
    """
    if not api_key:
        raise AIClientError(f"No {provider_label} API key configured. Set one in Settings.")
    if not model:
        raise AIClientError(f"No {provider_label} model configured.")
    if not base_url:
        raise AIClientError(f"No {provider_label} base URL configured.")

    prompt = _truncate_prompt(prompt, max_prompt_chars)
    url = base_url.rstrip("/") + "/chat/completions"
    payload = build_openai_payload(
        model, prompt, system_prompt,
        temperature=temperature, top_p=top_p, max_tokens=max_tokens, stream=stream,
    )
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"), method="POST", headers=headers,
    )

    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise_openai_http_error(exc, provider_label)
    except Exception as exc:  # noqa: BLE001 — surface any transport error cleanly
        raise AIClientError(f"{provider_label} API call failed: {exc}") from exc

    if stream:
        return iter_openai_sse(response, provider_label)

    try:
        body = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
    return extract_openai_text(body, provider_label)
