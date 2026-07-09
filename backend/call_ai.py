"""Multi-provider AI dispatch for the standalone sfglue app.

Signature the sfglue routes/engine expect:
    call_ai(prompt, system_prompt=None, max_tokens=4000, temperature=0, task=None) -> str

**Bedrock is the default** — same as the BI Migration Tool — so on a machine with ambient AWS
credentials + Bedrock model access it just works with no config. Provider resolution:
  1. SFGLUE_AI_PROVIDER (bedrock | anthropic | openai) if set — explicit override.
  2. else ANTHROPIC_API_KEY set  -> Anthropic API.
  3. else OPENAI_API_KEY set     -> OpenAI-compatible.
  4. else                        -> Bedrock (boto3 default credential chain).
Raises a clear error only if the chosen provider is misconfigured (the routes surface it).
"""
import os

from backend.integrations.anthropic_client import call_anthropic_chat
from backend.integrations.bedrock_client import call_bedrock_chat
from backend.integrations.openai_client import call_openai_compatible_chat

# Bedrock per-tier models — match the BI app's defaults; override any via env.
_BEDROCK_TIERS = {
    "fast": os.environ.get("BEDROCK_MODEL_FAST", "us.anthropic.claude-haiku-4-5-20251001-v1:0"),
    "standard": (os.environ.get("BEDROCK_MODEL_STANDARD") or os.environ.get("BEDROCK_MODEL")
                 or os.environ.get("BEDROCK_MODEL_ID") or "us.anthropic.claude-sonnet-4-6"),
    "premium": os.environ.get("BEDROCK_MODEL_PREMIUM", "us.anthropic.claude-opus-4-8"),
}
# sfglue tasks → tier. Conversion runs on 'standard' (Sonnet); everything defaults to standard.
_TASK_TIERS = {"sfglue_migration": "standard"}
_DEFAULT_TIER = "standard"


def _bedrock_model(task):
    return _BEDROCK_TIERS.get(_TASK_TIERS.get(task, _DEFAULT_TIER), _BEDROCK_TIERS["standard"])


def _region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def _provider():
    p = os.environ.get("SFGLUE_AI_PROVIDER", "").strip().lower()
    if p:
        return p
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    return "bedrock"


def _text(out):
    if isinstance(out, str):
        return out
    if isinstance(out, dict):
        return out.get("text") or out.get("content") or out.get("output") or ""
    return str(out or "")


def call_ai(prompt, system_prompt=None, max_tokens=4000, temperature=0, task=None, aws_creds=None):
    """Dispatch a single chat completion to the configured provider (Bedrock by default).

    aws_creds (optional dict): AWS credentials to use for Bedrock — typically the ones the
    user entered for the Glue connection, so the AI works without separately configuring the
    server's environment. Keys: region, profile, access_key_id, secret_access_key,
    session_token. Ignored for the Anthropic/OpenAI providers.
    """
    provider = _provider()
    mt = max_tokens or 4000

    if provider == "anthropic":
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("SFGLUE_AI_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set.")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        return _text(call_anthropic_chat(
            model, prompt, key, system_prompt=system_prompt,
            temperature=temperature, max_tokens=mt))

    if provider == "openai":
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("SFGLUE_AI_PROVIDER=openai but OPENAI_API_KEY is not set.")
        model = os.environ.get("OPENAI_MODEL", "gpt-4o")
        base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
        return _text(call_openai_compatible_chat(
            model, prompt, key, base_url=base_url, system_prompt=system_prompt,
            temperature=temperature, max_tokens=mt))

    # Default: Amazon Bedrock via the Converse API. Prefer credentials passed in from the
    # request (the Glue connection's creds); otherwise fall back to the server environment /
    # boto3 default credential chain.
    creds = aws_creds or {}
    return _text(call_bedrock_chat(
        _bedrock_model(task), prompt,
        region_name=creds.get("region") or _region(),
        profile_name=creds.get("profile") or os.environ.get("AWS_PROFILE"),
        aws_access_key_id=creds.get("access_key_id"),
        aws_secret_access_key=creds.get("secret_access_key"),
        aws_session_token=creds.get("session_token"),
        system_prompt=system_prompt, temperature=temperature, max_tokens=mt))
