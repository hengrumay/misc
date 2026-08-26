"""Wave 0 — Unity AI Gateway (PHI containment). Idempotent, REST-based.

Stands up the ``ads-ai-gateway`` serving endpoint that fronts a foundation model
with AI Gateway controls:
  * PII/PHI-aware guardrails (MASK on input + output),
  * inference/usage logging -> cfg().inference_table (payload table),
  * rate limit (cfg gateway.rate_limit_qpm),
and stores the routing token in a **secret scope** (no hardcoded secrets).

IMPORTANT — this endpoint is an OPTIONAL pattern placeholder and is NOT on the
query path. It routes an ``external_model`` to a *pay-per-token* foundation model,
and Databricks returns 403 when you query such a route (``external_model`` targets
external providers / custom served models, not the workspace's own PPT FMs).
The **functional, governed** gateway for PPT FMs is
the application-layer wrapper ``lib/pipeline/gateway.py``, which PHI-masks, calls
the FM serving endpoint **directly**, and logs cost/usage — that is what every
agent/benchmark call uses. This function provisions the native endpoint +
guardrail config so the pattern is documented and in place; it is kept
idempotent and optional. Point a real query at it only once it fronts a
custom served model (not a PPT FM).

REST-based so it runs on the serverless job runtime (SDK ``ai_gateway`` types vary by
version). Names resolve from demo.config.yaml via lib/config.py.
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.config import cfg  # noqa: E402

SECRET_SCOPE = "ads-ai-gateway"
SECRET_KEY = "sp_token"


def _client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def ensure_secret(w) -> bool:
    """Create the secret scope + a routing PAT if absent. Idempotent."""
    try:
        scopes = {s.name for s in w.secrets.list_scopes()}
        if SECRET_SCOPE not in scopes:
            w.secrets.create_scope(scope=SECRET_SCOPE)
        # only mint a token if the key is missing
        try:
            keys = {k.key for k in w.secrets.list_secrets(scope=SECRET_SCOPE)}
        except Exception:
            keys = set()
        if SECRET_KEY not in keys:
            tok = w.tokens.create(comment="ads-ai-gateway FM routing (RWE ADS)",
                                  lifetime_seconds=7776000).token_value
            w.secrets.put_secret(scope=SECRET_SCOPE, key=SECRET_KEY, string_value=tok)
            print(f"[gateway] secret {SECRET_SCOPE}/{SECRET_KEY} created")
        else:
            print(f"[gateway] secret {SECRET_SCOPE}/{SECRET_KEY} present")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[gateway] secret setup skipped/failed: {repr(e)[:160]}")
        return False


def configure_gateway(c=None):
    c = c or cfg()
    g = c.get("gateway", {})
    print(f"=== Wave 0 gateway: {c.gateway_endpoint} ===")
    try:
        w = _client()
    except Exception as e:  # pragma: no cover
        print(f"[gateway] SKIP — SDK unavailable: {e}")
        return

    if not ensure_secret(w):
        print("[gateway] cannot proceed without the routing secret")
        return

    # idempotent: skip if the endpoint already exists
    try:
        w.api_client.do("GET", f"/api/2.0/serving-endpoints/{c.gateway_endpoint}")
        print("[gateway] endpoint already exists")
        return
    except Exception:
        pass

    inf = c.inference_table.split(".")   # catalog.schema.table
    route_model = c.default_model
    body = {
        "name": c.gateway_endpoint,
        "config": {"served_entities": [{
            "name": "reasoning",
            "external_model": {
                "name": route_model, "provider": "databricks-model-serving", "task": "llm/v1/chat",
                "databricks_model_serving_config": {
                    "databricks_workspace_url": w.config.host,
                    "databricks_api_token": f"{{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}"}}}]},
        "ai_gateway": {
            "guardrails": {"input": {"pii": {"behavior": "MASK"}}, "output": {"pii": {"behavior": "MASK"}}},
            "inference_table_config": {"catalog_name": inf[0], "schema_name": inf[1],
                                       "table_name_prefix": inf[2] + "_native", "enabled": True},
            "usage_tracking_config": {"enabled": True},
            "rate_limits": [{"calls": int(g.get("rate_limit_qpm", 300)), "renewal_period": "minute"}]},
    }
    try:
        r = w.api_client.do("POST", "/api/2.0/serving-endpoints", body=body)
        print(f"[gateway] created endpoint {r.get('name')} with PII/PHI MASK guardrails, "
              f"inference logging -> {c.inference_table}, rate limit {g.get('rate_limit_qpm')}/min")
        print("[gateway] NOTE: external_model->pay-per-token-FM may 403 at query time; the "
              "functional gateway for PPT FMs is lib/pipeline/gateway.py (mask+log+cost).")
    except Exception as e:  # noqa: BLE001
        print(f"[gateway] create failed: {repr(e)[:300]}")

    # egress policy + cost cap are documented controls (enforced via workspace policy)
    print(f"[gateway] egress policy (required): {g.get('egress_policy')}; "
          f"cost tag {g.get('cost_attribution_tag')}; spend cap ${g.get('spend_cap_usd_month')}/mo")


if __name__ == "__main__":
    configure_gateway()
