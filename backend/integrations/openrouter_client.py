"""OpenRouter chat integration (OpenAI-compatible Chat Completions API).

OpenRouter (https://openrouter.ai) is an OpenAI-compatible gateway that fronts
many model providers behind a single API key and endpoint. This client speaks
the Chat Completions protocol over HTTP using only the Python standard library
(urllib), so no extra dependency is required.

It supports both blocking and streaming (SSE) responses, mirroring the call
surface the app's provider dispatcher expects: ``stream=False`` returns the full
response text, ``stream=True`` returns a generator that yields incremental text
chunks as the model produces them (this powers token-by-token SSE rendering on
the frontend).
"""

import json
import logging
import urllib.error
import urllib.request

from backend.integrations.ai_client import AIClientError, _truncate_prompt

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"


def _build_messages(prompt, system_prompt):
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    return messages


def _build_request(url, payload, api_key, *, referer, title):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Optional attribution headers OpenRouter surfaces on its dashboard/rankings.
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title
    return urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )


def _raise_http_error(exc):
    """Translate an HTTPError into a clean AIClientError carrying the API message."""
    detail = exc.read().decode("utf-8", errors="replace")
    message = detail
    try:
        parsed = json.loads(detail)
        message = (parsed.get("error") or {}).get("message") or detail
    except (json.JSONDecodeError, AttributeError):
        pass
    raise AIClientError(f"OpenRouter API returned {exc.code}: {message}") from exc


def _iter_openrouter_stream(response, model):
    """Yield incremental text chunks from an OpenRouter SSE response.

    OpenRouter streams Server-Sent Events: ``data: {json}`` lines carrying a
    chat-completion chunk whose ``choices[0].delta.content`` holds the
    incremental text. It also emits comment lines (starting with ``:``, e.g.
    ``: OPENROUTER PROCESSING``) as keep-alives, and a terminal ``data: [DONE]``.
    We surface only the text deltas and log the trailing usage to match the
    non-streaming path's "OpenRouter response OK" line.
    """
    emitted_any = False
    usage = {}
    finish_reason = None
    try:
        for raw in response:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line or line.startswith(":"):
                continue  # blank line or SSE comment / keep-alive
            if not line.startswith("data:"):
                continue
            data = line[len("data:"):].strip()
            if data == "[DONE]":
                break
            try:
                chunk = json.loads(data)
            except json.JSONDecodeError:
                continue
            choices = chunk.get("choices") or []
            if choices:
                text = (choices[0].get("delta") or {}).get("content")
                if text:
                    emitted_any = True
                    yield text
                if choices[0].get("finish_reason"):
                    finish_reason = choices[0]["finish_reason"]
            if chunk.get("usage"):
                usage = chunk["usage"]
    except (urllib.error.URLError, OSError) as exc:
        # Mid-stream transport failure — translate so the caller surfaces a clean
        # error to the SSE stream rather than a raw urllib exception.
        raise AIClientError(f"OpenRouter stream failed: {exc}") from exc
    finally:
        response.close()

    logger.info(
        "OpenRouter stream OK model=%s prompt_tokens=%s completion_tokens=%s finish_reason=%s",
        model,
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        finish_reason,
    )
    if not emitted_any:
        raise AIClientError("OpenRouter returned an empty stream (no content deltas).")


def call_openrouter_chat(
    model,
    prompt,
    api_key,
    *,
    base_url=None,
    system_prompt=None,
    temperature=0,
    top_p=1,
    max_tokens=4000,
    max_prompt_chars=None,
    timeout=120,
    stream=False,
    referer=None,
    title=None,
):
    """Call an OpenRouter model through the OpenAI-compatible Chat Completions API.

    Returns the full response text when ``stream=False`` (default). When
    ``stream=True`` it returns a generator yielding incremental text chunks via
    SSE.
    """
    if not api_key:
        raise AIClientError(
            "No OpenRouter API key configured. Set OPENROUTER_API_KEY in your .env (or supply one in Settings)."
        )
    if not model:
        raise AIClientError("OPENROUTER_MODEL_ID is not configured.")

    prompt = _truncate_prompt(prompt, max_prompt_chars)
    url = (base_url or _DEFAULT_BASE_URL).rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": _build_messages(prompt, system_prompt),
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "stream": bool(stream),
    }
    request = _build_request(url, payload, api_key, referer=referer, title=title)

    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        _raise_http_error(exc)
    except Exception as exc:  # noqa: BLE001 — surface any transport error cleanly
        raise AIClientError(f"OpenRouter API call failed: {exc}") from exc

    if stream:
        # _iter_openrouter_stream owns the response and closes it when drained.
        return _iter_openrouter_stream(response, model)

    try:
        body = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()

    choices = body.get("choices") or []
    text = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
    text = text.strip()
    if not text:
        raise AIClientError(f"Unexpected OpenRouter response shape: {str(body)[:500]}")

    usage = body.get("usage") or {}
    logger.info(
        "OpenRouter response OK model=%s prompt_tokens=%s completion_tokens=%s finish_reason=%s",
        model,
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        choices[0].get("finish_reason") if choices else None,
    )
    return text
