"""Shared AI-client primitives.

The app supports several AI providers (Amazon Bedrock, the Anthropic API, and a
family of OpenAI-compatible gateways — OpenRouter, OpenAI, Azure OpenAI, and
Google Gemini). This module holds the small helpers shared across those
integrations: the error type, prompt truncation, and — since OpenAI, Azure
OpenAI, Google Gemini and OpenRouter all speak the same Chat Completions wire
format — a shared request/response/SSE protocol used by their clients.
"""

import json
import logging
import urllib.error

logger = logging.getLogger(__name__)


class AIClientError(Exception):
    """Raised when an AI provider call fails."""

    pass


def _truncate_prompt(prompt, max_prompt_chars):
    if max_prompt_chars and len(prompt) > max_prompt_chars:
        truncation_notice = (
            f"\n\n[NOTE: prompt was truncated from {len(prompt):,} to "
            f"{max_prompt_chars:,} characters to fit the model context window.]"
        )
        return prompt[:max_prompt_chars] + truncation_notice
    return prompt


# ─── OpenAI-compatible Chat Completions protocol ─────────────────────────────
# OpenAI, Azure OpenAI, Google Gemini (OpenAI-compat endpoint) and OpenRouter all
# accept the same request body and return the same response/SSE shape, differing
# only in URL and auth headers. These helpers centralise the wire handling so
# each provider client is just URL + headers + a call to these.


def build_openai_messages(prompt, system_prompt):
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    return messages


def build_openai_payload(model, prompt, system_prompt, *, temperature, top_p, max_tokens, stream):
    return {
        "model": model,
        "messages": build_openai_messages(prompt, system_prompt),
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "stream": bool(stream),
    }


def raise_openai_http_error(exc, provider_label):
    """Translate an HTTPError into a clean AIClientError carrying the API message."""
    detail = exc.read().decode("utf-8", errors="replace")
    message = detail
    try:
        parsed = json.loads(detail)
        message = (parsed.get("error") or {}).get("message") or detail
    except (json.JSONDecodeError, AttributeError):
        pass
    raise AIClientError(f"{provider_label} API returned {exc.code}: {message}") from exc


def extract_openai_text(body, provider_label):
    """Pull the assistant text from a non-streaming Chat Completions response."""
    choices = body.get("choices") or []
    text = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
    text = text.strip()
    if not text:
        raise AIClientError(f"Unexpected {provider_label} response shape: {str(body)[:500]}")
    usage = body.get("usage") or {}
    logger.info(
        "%s response OK model=%s prompt_tokens=%s completion_tokens=%s finish_reason=%s",
        provider_label,
        body.get("model"),
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        choices[0].get("finish_reason") if choices else None,
    )
    return text


def iter_openai_sse(response, provider_label):
    """Yield incremental text chunks from an OpenAI-style SSE response.

    The stream is a sequence of ``data: {json}`` lines whose
    ``choices[0].delta.content`` holds the incremental text. Comment lines
    (starting with ``:``) are keep-alives and are skipped; the stream ends with a
    terminal ``data: [DONE]``. The response is closed when the generator drains.
    """
    emitted_any = False
    usage = {}
    finish_reason = None
    try:
        for raw in response:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line or line.startswith(":") or not line.startswith("data:"):
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
        raise AIClientError(f"{provider_label} stream failed: {exc}") from exc
    finally:
        response.close()

    logger.info(
        "%s stream OK prompt_tokens=%s completion_tokens=%s finish_reason=%s",
        provider_label,
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        finish_reason,
    )
    if not emitted_any:
        raise AIClientError(f"{provider_label} returned an empty stream (no content deltas).")
