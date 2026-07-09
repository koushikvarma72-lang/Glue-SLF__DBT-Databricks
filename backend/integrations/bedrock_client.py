"""Amazon Bedrock chat integration using the boto3 credential provider chain."""

import logging

from backend.integrations.ai_client import AIClientError, _truncate_prompt

logger = logging.getLogger(__name__)


def _bedrock_session(profile_name=None, region_name=None):
    try:
        import boto3
    except ImportError as exc:
        raise AIClientError(
            "Amazon Bedrock requires boto3. Install the project requirements and restart the server."
        ) from exc

    session_kwargs = {}
    if profile_name:
        session_kwargs["profile_name"] = profile_name
    if region_name:
        session_kwargs["region_name"] = region_name
    return boto3.Session(**session_kwargs)


def _converse_with_sampling_fallback(client, request, *, stream=False):
    """Call Converse (or ConverseStream), dropping sampling params the model rejects.

    Different Bedrock models accept different sampling fields. When a model
    reports that ``temperature`` or ``topP`` is deprecated/unsupported (or that
    both can't be set together), strip the offending field from
    ``inferenceConfig`` and retry, until the call succeeds or there's nothing
    left to drop.

    With ``stream=True`` this calls ``converse_stream`` instead of ``converse``.
    ValidationExceptions surface synchronously from the ``converse_stream`` call
    (before the event stream is iterated), so the same retry loop applies.
    """
    from botocore.exceptions import ClientError

    method = client.converse_stream if stream else client.converse
    sampling_fields = ("temperature", "topP")
    while True:
        try:
            return method(**request)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            if error.get("Code") != "ValidationException":
                raise
            message = (error.get("Message") or "").lower()
            cfg = request.get("inferenceConfig", {})
            present = [f for f in sampling_fields if f in cfg]
            if not present or not any(kw in message for kw in ("temperature", "top_p", "topp")):
                raise
            # Drop the field(s) the model named; if it named none specifically
            # (e.g. "cannot both be specified"), drop the lowest-priority one.
            named = [f for f in present if f.lower() in message or (f == "topP" and "top_p" in message)]
            to_drop = named or [present[-1]]
            for field in to_drop:
                cfg.pop(field, None)


def _iter_converse_stream(stream_response, model):
    """Yield incremental text chunks from a ConverseStream EventStream.

    ConverseStream emits a sequence of typed events; ``contentBlockDelta``
    carries the incremental text. We ignore the structural events
    (messageStart/contentBlockStart/Stop/messageStop) and surface only text,
    logging usage from the trailing ``metadata`` event to match the
    non-streaming code path's "Amazon Bedrock response OK" line.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    emitted_any = False
    stop_reason = None
    usage = {}
    try:
        for event in stream_response.get("stream", []):
            if "contentBlockDelta" in event:
                text = (event["contentBlockDelta"].get("delta") or {}).get("text")
                if text:
                    emitted_any = True
                    yield text
            elif "messageStop" in event:
                stop_reason = event["messageStop"].get("stopReason")
            elif "metadata" in event:
                usage = event["metadata"].get("usage") or {}
    except (ClientError, BotoCoreError) as exc:
        # Mid-stream transport failure — translate so the caller surfaces a
        # clean error to the SSE stream rather than a raw boto exception.
        raise AIClientError(f"Amazon Bedrock stream failed: {exc}") from exc

    logger.info(
        "Amazon Bedrock stream OK model=%s input_tokens=%s output_tokens=%s stop_reason=%s",
        model,
        usage.get("inputTokens"),
        usage.get("outputTokens"),
        stop_reason,
    )
    if not emitted_any:
        raise AIClientError("Amazon Bedrock returned an empty stream (no text deltas).")


def call_bedrock_chat(
    model,
    prompt,
    *,
    region_name,
    profile_name=None,
    system_prompt=None,
    temperature=0,
    top_p=1,
    max_tokens=None,
    max_prompt_chars=60_000,
    timeout=120,
    stream=False,
):
    """Call a Bedrock model through the provider-neutral Converse API.

    When ``stream=False`` (default) this blocks and returns the full response
    text. When ``stream=True`` it uses the ConverseStream API and returns a
    generator that yields incremental text chunks as the model produces them —
    this is what powers token-by-token SSE rendering on the frontend.
    """
    if not model:
        raise AIClientError("BEDROCK_MODEL_ID is not configured.")
    if not region_name:
        raise AIClientError("AWS_REGION or AWS_DEFAULT_REGION is not configured.")

    prompt = _truncate_prompt(prompt, max_prompt_chars)

    try:
        from botocore.config import Config
        from botocore.exceptions import (
            BotoCoreError,
            ClientError,
            NoCredentialsError,
            ProfileNotFound,
        )
    except ImportError as exc:
        raise AIClientError(
            "Amazon Bedrock requires boto3 and botocore. Install the project requirements and restart the server."
        ) from exc

    try:
        session = _bedrock_session(profile_name=profile_name, region_name=region_name)
        client = session.client(
            "bedrock-runtime",
            config=Config(
                connect_timeout=min(timeout, 30),
                read_timeout=timeout,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        request = {
            "modelId": model,
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {},
        }
        if system_prompt:
            request["system"] = [{"text": system_prompt}]
        if max_tokens is not None:
            request["inferenceConfig"]["maxTokens"] = max_tokens
        # Sampling-param support varies across Bedrock models: older models accept
        # temperature + topP together, some reject specifying both, and newer
        # reasoning models (e.g. Opus 4.8) deprecate them entirely. Prefer
        # temperature (the primary knob); only fall back to topP when no
        # temperature was requested. _converse_with_sampling_fallback then drops
        # whichever field the model rejects and retries, so determinism-oriented
        # callers still work everywhere.
        if temperature is not None:
            request["inferenceConfig"]["temperature"] = temperature
        elif top_p is not None:
            request["inferenceConfig"]["topP"] = top_p

        if stream:
            stream_response = _converse_with_sampling_fallback(client, request, stream=True)
            return _iter_converse_stream(stream_response, model)

        response = _converse_with_sampling_fallback(client, request)
    except ProfileNotFound as exc:
        raise AIClientError(
            f"AWS profile {profile_name!r} was not found. Run `aws configure sso --profile {profile_name}` first."
        ) from exc
    except NoCredentialsError as exc:
        raise AIClientError(
            "AWS credentials were not found. Run `aws sso login --profile <profile>` "
            "or export temporary AWS credentials before starting the server."
        ) from exc
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "ClientError")
        message = error.get("Message", str(exc))
        logger.warning("Amazon Bedrock call failed model=%s code=%s: %s", model, code, message)
        raise AIClientError(f"Amazon Bedrock {code}: {message}") from exc
    except BotoCoreError as exc:
        logger.warning("Amazon Bedrock request failed model=%s: %s", model, exc)
        raise AIClientError(f"Amazon Bedrock request failed: {exc}") from exc

    content = ((response.get("output") or {}).get("message") or {}).get("content") or []
    text = "".join(block.get("text", "") for block in content if isinstance(block, dict))
    if not text:
        raise AIClientError(f"Unexpected Amazon Bedrock response shape: {str(response)[:500]}")

    usage = response.get("usage") or {}
    logger.info(
        "Amazon Bedrock response OK model=%s input_tokens=%s output_tokens=%s stop_reason=%s",
        model,
        usage.get("inputTokens"),
        usage.get("outputTokens"),
        response.get("stopReason"),
    )
    return text
