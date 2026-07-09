# sfglue — Snowflake + AWS Glue → Databricks + dbt

Standalone migration app, split out of the combined BI Migration Tool (`qvf_decoder 2`). It hosts
**only** the sfglue flow: connect Snowflake/Glue → check lineage → review → Databricks agent →
dbt agent → report, with AI conversion + a reconciliation gate + a runnable-dbt-project export.

Fully self-contained — no imports from `qvf_decoder 2`.

```
sfglue_app/
  backend/
    app.py            Flask app (port 5060) + SPA static serving
    call_ai.py        multi-provider AI dispatch — Bedrock by default (Anthropic / OpenAI opt-in)
    integrations/     sfglue engine, provider clients, + a tiny databricks-introspect shim
    migration/        dialect_normalizer + duckdb_execution (only what the engine needs)
  qvd_to_databricks/  Databricks connectivity toolkit (databricks_connection/executor, …)
  frontend/           sfglue-only SPA (Vite); proxy → :5060
  server.py, requirements.txt
```

## Run

**Backend** (port 5060):
```bash
cd sfglue_app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# AI works on Amazon Bedrock by default — just have active AWS creds + Bedrock model access:
aws sso login --profile Cognitive-Tech-495688866359
export AWS_PROFILE=Cognitive-Tech-495688866359
export AWS_REGION=us-west-2
python server.py                        # http://localhost:5060
```

**Frontend** (Vite dev, proxies /api → :5060):
```bash
cd sfglue_app/frontend
npm install
npm run dev                             # http://localhost:5173
# or: npm run build   → backend serves frontend/dist at :5060
```

## Notes
- **AI defaults to Amazon Bedrock** (same as the BI Migration Tool) — no keys or provider env
  needed. With active AWS credentials (`AWS_PROFILE` + `AWS_REGION`, or any boto3 credential source)
  and Bedrock model access enabled, it works out of the box. Model tiers match the BI app:
  conversion runs on `us.anthropic.claude-sonnet-4-6` (override with `BEDROCK_MODEL` /
  `BEDROCK_MODEL_STANDARD`).
- **To use an API key instead**, set `ANTHROPIC_API_KEY` (+ optional `ANTHROPIC_MODEL`) or
  `OPENAI_API_KEY` (+ `OPENAI_BASE_URL`/`OPENAI_MODEL`) — a key auto-selects that provider. Force a
  specific provider with `SFGLUE_AI_PROVIDER=bedrock|anthropic|openai`.
- Each migration step runs a ~1-token preflight probe; if the provider isn't reachable (e.g. an
  expired SSO token) the route returns `needsAiConfig` with an actionable message
  (`aws sso login …`) instead of emitting placeholder output.
- Databricks/Snowflake/Glue credentials are supplied per-request from the UI (the `destination` /
  connection payloads) — nothing is baked in. Source-agnostic: works for any Glue+Snowflake flow.
