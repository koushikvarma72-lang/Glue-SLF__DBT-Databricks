"""Anthropic Messages API client — a provider fallback for machines without
Amazon Bedrock / AWS credentials.

Uses only the Python standard library (urllib) so no extra dependency is needed.
Configured at runtime via the AI settings (a user-supplied API key), which lets
the app run on a computer that has no Bedrock access.
"""

import json
import urllib.error
import urllib.request

from backend.integrations.ai_client import AIClientError, _truncate_prompt

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_ANTHROPIC_VERSION = "2023-06-01"


def call_anthropic_chat(
    model,
    prompt,
    api_key,
    *,
    system_prompt=None,
    temperature=0,
    top_p=1,
    max_tokens=4000,
    max_prompt_chars=None,
    timeout=120,
    stream=False,
):
    """Call the Anthropic Messages API for a chat completion.

    Returns the full response text when ``stream=False``. When ``stream=True`` it
    returns a generator yielding the text once, to satisfy callers that expect a
    streaming generator (the migration SSE path) without implementing token-level
    SSE parsing.
    """
    if not api_key:
        raise AIClientError("No Anthropic API key configured. Set one in Settings.")
    if not model:
        raise AIClientError("No Anthropic model configured.")

    prompt = _truncate_prompt(prompt, max_prompt_chars)
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "top_p": top_p,
    }
    if system_prompt:
        payload["system"] = system_prompt

    request = urllib.request.Request(
        _ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "x-api-key": api_key,
            "anthropic-version": _ANTHROPIC_VERSION,
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        message = detail
        try:
            parsed = json.loads(detail)
            message = (parsed.get("error") or {}).get("message") or detail
        except (json.JSONDecodeError, AttributeError):
            pass
        raise AIClientError(f"Anthropic API returned {exc.code}: {message}") from exc
    except Exception as exc:  # noqa: BLE001 — surface any transport error cleanly
        raise AIClientError(f"Anthropic API call failed: {exc}") from exc

    text = "".join(
        block.get("text", "")
        for block in (body.get("content") or [])
        if block.get("type") == "text"
    ).strip()
    if not text:
        raise AIClientError("Anthropic API returned an empty response.")

    if stream:
        def _one_shot():
            yield text

        return _one_shot()
    return text
