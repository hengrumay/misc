"""Unity AI Gateway client wrapper — the single controlled path for model calls.

Every model call in the ADS system goes through ``gateway_call``, which:
  1. **PHI-masks** the input (``lib.phi.mask_phi``) before it leaves the process,
  2. routes **directly** to the pay-per-token foundation-model serving endpoint
     (``POST /serving-endpoints/{model}/invocations``), and
  3. **logs** the (masked) request, response, token usage and estimated cost to
     ``ads_audit.gateway_inference`` for cost attribution + auditability.

This is the application-layer enforcement of the charter's PHI-containment +
egress-logging controls: the PHI-mask + audit-logging happen in-process here,
and this is the real governed path for every model call.

**MLflow tracing (non-fatal, but never silent).** Each ``gateway_call`` opens an
MLflow span (``span_type=LLM``) recording the masked input, output, token
counts, latency and estimated cost. Every span logs to ONE experiment for the
whole initiative, resolved at RUNTIME by ``experiment_name()`` to a path under
the current run identity's home directory (``/Users/<identity>/<initiative>``).

Why that path (and not what was here before):
  * NOT a hardcoded ``/Shared/...`` path — the job identity may lack write there,
    so the experiment silently failed to persist (traces went nowhere). A home
    directory is always writable by the identity running the job, so the
    experiment reliably persists on a serverless job.
  * NOT a literal user path — that breaks portability + golden-rule #7. The
    identity is resolved live via the Databricks SDK, so ANY customer's run
    identity works, and a local verify run (same profile) resolves the same
    experiment because the DAB runs jobs as ``run_as: current_user``.

We deliberately do NOT bind the experiment to a Unity Catalog Delta trace store:
that binding raised on this serverless runtime and its swallow-and-fall-back is
exactly what left tracing looking fine while persisting nothing. Governed
UC-Delta trace storage is a documented FUTURE enhancement (see ``experiment_name``
and ``_ensure_tracing``), not the default.

The ``import mlflow`` is guarded (absent runtime -> tracing off). Experiment init
is guarded too, but NON-SILENTLY: a genuine failure prints
``[mlflow] TRACING INIT FAILED: <err>`` and disables tracing for the run — the
wave still completes (tracing is non-fatal), but it never ends up in a state that
looks fine while persisting nothing. ``mask_phi`` and the ``gateway_inference``
log row are unaffected by tracing.

The native ``ads-ai-gateway`` serving endpoint (``cfg().gateway_endpoint``,
provisioned by wave0) is **NOT** on the query path. It is an ``external_model``
route that targets a pay-per-token Foundation Model, and Databricks returns 403
when you query such a route (``external_model`` is for external providers /
custom served models, not the workspace's own PPT FMs). It is kept only as a
documented pattern placeholder for the guardrail/logging config it carries.

All names resolve via lib/config.py. No hardcoded secrets.
"""
from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass

from lib.config import cfg
from lib.phi import mask_phi

# --- MLflow tracing (guarded import; degrades to no-op if unavailable) -------
try:
    import mlflow
    from mlflow.entities import SpanType as _SpanType
    _MLFLOW_IMPORT_OK = True
except Exception:  # noqa: BLE001 - mlflow missing on this runtime → tracing off
    mlflow = None
    _SpanType = None
    _MLFLOW_IMPORT_OK = False

_TRACING_READY: bool | None = None  # tri-state cache: None=not tried yet


def experiment_name(w=None) -> str:
    """Runtime-resolved, portable MLflow experiment path for this initiative.

    Returns ``/Users/<current-identity>/<initiative>`` — the ONE experiment that
    both the gateway LLM spans (here) and the wave-4 tracking run log to, so
    traces + runs + metrics live together and are found by
    ``scripts/verify_tracing.py``.

    The identity is resolved LIVE via the Databricks SDK (``current_user.me()``),
    not hardcoded: a home directory is always writable by the identity running
    the job (so the experiment reliably persists on serverless), and it is
    portable to any customer's run identity. Callers pass their existing
    ``WorkspaceClient`` so we reuse the job's auth and never build a second one.
    """
    from databricks.sdk import WorkspaceClient
    w = w or WorkspaceClient()
    identity = w.current_user.me().user_name
    return f"/Users/{identity}/{cfg().initiative}"


def _ensure_tracing(w=None) -> bool:
    """Point MLflow at the databricks tracking server + the shared experiment ONCE.

    Returns True if MLflow is usable for tracing, else False. NON-FATAL but
    NON-SILENT: if init genuinely fails it prints ``[mlflow] TRACING INIT
    FAILED`` and returns False (the pipeline still completes) — it never returns
    True while persisting nothing.

    FUTURE ENHANCEMENT (intentionally NOT the default): to persist traces in
    governed Unity Catalog Delta tables, pass the UC trace-location argument to
    ``set_experiment`` (catalog / schema / table-prefix). That binding raised on
    this serverless runtime and its silent fall-back is exactly what broke
    persistence, so it is left off; the user-home experiment below is the
    reliable, portable default.
    """
    global _TRACING_READY
    if _TRACING_READY is not None:
        return _TRACING_READY
    if not _MLFLOW_IMPORT_OK:
        _TRACING_READY = False
        return False
    try:
        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_name(w))
        _TRACING_READY = True
        return True
    except Exception as e:  # noqa: BLE001 - non-fatal, but NEVER silent
        print(f"[mlflow] TRACING INIT FAILED: {e!r} — gateway spans disabled for this run")
        _TRACING_READY = False
        return False


def _gateway_span(w=None):
    """Return a context manager for one gateway-call span, or a no-op nullcontext.

    Guarded so a tracing-layer failure never propagates into the model call, but
    a span-start failure after a successful init is printed (not swallowed).
    """
    if _ensure_tracing(w):
        try:
            return mlflow.start_span(name="gateway_call", span_type=_SpanType.LLM)
        except Exception as e:  # noqa: BLE001 - non-fatal, but surfaced
            print(f"[mlflow] start_span failed: {e!r}")
            return contextlib.nullcontext()
    return contextlib.nullcontext()

# Rough pay-per-token rates ($ per 1M tokens: input, output) for cost estimation.
# ESTIMATES / placeholders for the modern candidate set — refine against the live
# pay-per-token price list. Unlisted models fall back to the default below.
_RATES = {
    "databricks-claude-sonnet-5": (3.00, 15.00),
    "databricks-gpt-5-5": (1.25, 10.00),
    "databricks-gemini-3-7-flash": (0.30, 2.50),
}


@dataclass
class GatewayResult:
    content: str
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    latency_s: float = 0.0
    model: str = ""


def _extract_content(resp: dict) -> str:
    ch = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "")
    if isinstance(ch, list):  # some models return content as a list of typed parts
        ch = " ".join(p.get("text", "") or p.get("summary_text", "") for p in ch if isinstance(p, dict))
    return ch or ""


def gateway_call(model: str, system: str, user_msg: str, *, w=None, log_rows: list | None = None,
                 max_tokens: int = 400, temperature: float = 0.0) -> GatewayResult:
    """Masked + logged model call, routed directly to the pay-per-token FM endpoint."""
    from databricks.sdk import WorkspaceClient
    w = w or WorkspaceClient()
    c = cfg()
    masked = mask_phi(user_msg)                      # <-- PHI never leaves unmasked

    # Route DIRECTLY to the pay-per-token FM serving endpoint. The native
    # ads-ai-gateway endpoint is an external_model route to a PPT FM, which 403s
    # at query time, so it is never on the query path. PHI-mask (above) + audit
    # logging (below) are the governed path.
    route = model

    # Build the request body. We deliberately DO NOT send `temperature`: modern
    # reasoning models (claude-sonnet-5, gemini-3-7-flash, gpt-5-5) reject it —
    # temperature=0.0 does not buy determinism on them and returns a 400 — so we
    # let the endpoint apply its own default. `max_tokens` is accepted today. The
    # `temperature` kwarg is kept in the signature for caller compatibility but is
    # not forwarded in the body.
    body = {"messages": [{"role": "system", "content": system},
                         {"role": "user", "content": masked}],
            "max_tokens": max_tokens}

    def _post_invocations(req_body: dict) -> dict:
        """POST to the invocations endpoint, robust to reasoning models.

        On a 400/BAD_REQUEST whose message names an unsupported optional
        parameter (e.g. 'does not support the temperature parameter',
        'unsupported_value', 'does not support ... with this model'), strip that
        parameter from the body and retry ONCE. Any other error is re-raised
        unchanged. `messages` is required and never stripped.
        """
        try:
            return w.api_client.do(
                "POST", f"/serving-endpoints/{route}/invocations", body=req_body)
        except Exception as e:  # noqa: BLE001 - inspect for unsupported-param 400, else re-raise
            msg = str(e).lower()
            unsupported = ("does not support" in msg or "unsupported_value" in msg
                           or "unsupported parameter" in msg or "unsupported_parameter" in msg)
            removable = [p for p in ("temperature", "max_tokens")
                         if p in req_body and p in msg]
            if not (unsupported and removable):
                raise
            retry_body = {k: v for k, v in req_body.items() if k not in removable}
            return w.api_client.do(
                "POST", f"/serving-endpoints/{route}/invocations", body=retry_body)

    # Open an MLflow span around the call (no-op if tracing unavailable). The
    # model call stays inside the span so span duration ≈ real latency; a
    # tracing-layer hiccup can never re-issue the call or break the pipeline.
    t0 = time.time()
    with _gateway_span(w) as span:
        resp = _post_invocations(body)
        dt = time.time() - t0
        content = _extract_content(resp)
        u = resp.get("usage", {})
        ti, to = int(u.get("prompt_tokens", 0)), int(u.get("completion_tokens", 0))
        ri, ro = _RATES.get(model, (0.5, 1.5))
        cost = ti / 1e6 * ri + to / 1e6 * ro
        if span is not None:  # record masked I/O + usage on the span, guarded
            try:
                span.set_inputs({"model": model, "system": system[:500],
                                 "user_masked": masked[:1000]})
                span.set_outputs({"content": str(content)[:1000]})
                span.set_attributes({"model": model, "tokens_in": ti, "tokens_out": to,
                                     "latency_s": round(dt, 3), "cost_usd": round(cost, 6)})
            except Exception:  # noqa: BLE001 - never let span I/O break the call
                pass

    if log_rows is not None:  # caller batches these into cfg().inference_table
        log_rows.append((c.gateway_endpoint, model, masked[:1000], str(content)[:1000],
                         ti, to, float(cost), "rwe-ads-automation", "epi-rwds"))
    return GatewayResult(content, ti, to, cost, round(dt, 2), model)
