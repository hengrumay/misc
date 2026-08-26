"""Unity AI Gateway cost attribution and spend analysis.

PRIMARY source — the in-process gateway inference log (``cfg().inference_table``,
a Delta table written by ``lib/pipeline/gateway.py``). Every governed model call
appends a row with real token counts and the cost we computed at call time, so
this source is IMMEDIATE, portable, and precise to *our* calls. We aggregate it
per model (authoritative) for the spend summary and cost breakdown.

BEST-EFFORT enrichment — Databricks **system tables**:
  * ``system.serving.endpoint_usage`` (joined to ``system.serving.served_entities``
    on ``served_entity_id`` for the entity name) — authoritative platform token
    counts (``input_token_count`` / ``output_token_count``) filtered by requester +
    a recent ``request_time`` window, and
  * ``system.billing.usage`` (joined to ``system.billing.list_prices``) — actual $.

INGESTION-DELAY CAVEAT (why the split): system tables are AUTHORITATIVE BUT
LAGGED. They are populated by an asynchronous platform pipeline with ingestion
latency (minutes to hours), so **a run cannot read its own rows immediately** —
the calls it just made will not be in ``system.serving.endpoint_usage`` /
``system.billing.usage`` yet. That is exactly why the in-process
``inference_table`` is the primary/immediate source and the system tables are
best-effort enrichment layered on top when (and only when) they are granted,
enabled, and caught up.

Everything that reaches out to Spark or a system table is wrapped so the report
degrades to whatever it can compute rather than failing: if Spark is
unavailable, a table isn't granted, or a system schema isn't enabled, the
corresponding section is simply skipped with a logged note.

Writes a real ``cost_report.md`` with the spend breakdown + recommendations and
checks it against ``cfg().gateway.spend_cap_usd_month``.
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec(); recover __file__ so the
# report can be written next to this module.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Make the repo root importable (lib.config, lib.pipeline.gateway) whether run as
# a serverless spark_python_task or locally — mirrors run_benchmark_live.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger(__name__)

# Cost-estimate rates are the SINGLE SOURCE OF TRUTH in lib/pipeline/gateway.py
# (``_RATES``: $ per 1M tokens, (input, output)). Import them rather than
# duplicating a pricing table here. These are ESTIMATE/placeholder rates for the
# current candidate models — refine against the live pay-per-token price list.
# They are used ONLY as a fallback per-PoC estimate when a model has no logged
# cost yet; the per-model spend numbers come from real ``cost_usd`` rows.
try:
    from lib.pipeline.gateway import _RATES as _GATEWAY_RATES
except Exception:  # noqa: BLE001 - keep a local estimate table if the import fails
    _GATEWAY_RATES = {
        "databricks-claude-sonnet-5": (3.00, 15.00),   # estimate ($/1M tok in, out)
        "databricks-gpt-5-5": (1.25, 10.00),           # estimate
        "databricks-gemini-3-7-flash": (0.30, 2.50),   # estimate
    }
_DEFAULT_RATE = (0.5, 1.5)  # estimate for any model not in the table


@dataclass
class ModelCost:
    """Authoritative per-model cost, aggregated from the gateway inference log."""
    model: str
    input_tokens: int
    output_tokens: int
    total_cost_usd: float
    num_calls: int
    avg_cost_per_call_usd: float


@dataclass
class PocCost:
    """Best-effort per-PoC cost. The inference log has no poc_id column (and the
    gateway log schema is intentionally unchanged), so PoC token counts come from
    the benchmark table and $ is ESTIMATED via each model's blended $/token."""
    poc_id: str
    tokens: int
    est_cost_usd: float
    models: list = field(default_factory=list)


def generate_cost_report() -> dict:
    """Generate and write the cost report.

    Returns a dict with: status, total_cost_usd, spend_cap_usd,
    remaining_budget_usd, pct_spent, report_path, breakdown_by_model,
    breakdown_by_poc, recommendations, num_inference_calls.
    """
    try:
        from lib.config import cfg
        c = cfg()
    except Exception as e:  # noqa: BLE001 - no config (missing yaml/file) → degrade, don't crash
        return {"status": "error", "message": f"config unavailable: {e}"}

    spend_cap = float(c.get("gateway.spend_cap_usd_month", 2000) or 2000)
    logger.info("=== Unity AI Gateway Cost Report ===")
    logger.info(f"Spend cap: ${spend_cap:.2f}/month")

    spark = _get_spark()
    if spark is None:
        # Degrade cleanly: no Spark (e.g. local run) → nothing to aggregate.
        logger.warning("Spark unavailable — cost report degraded to an empty/no-data report.")
        report_path = _write_cost_report(c, [], [], None, 0.0, spend_cap,
                                          ["⚠️ Spark session unavailable — no inference data read."])
        return {"status": "degraded", "message": "spark unavailable",
                "total_cost_usd": 0.0, "spend_cap_usd": spend_cap,
                "remaining_budget_usd": spend_cap, "pct_spent": 0.0,
                "report_path": report_path, "breakdown_by_model": {},
                "breakdown_by_poc": {}, "recommendations": [], "num_inference_calls": 0}

    # PRIMARY: authoritative per-model spend from the in-process inference log.
    model_costs = _read_model_costs(spark, c)
    total_cost = sum(mc.total_cost_usd for mc in model_costs)
    num_calls = sum(mc.num_calls for mc in model_costs)

    # BEST-EFFORT: per-PoC token counts from the benchmark table, $ estimated via
    # each model's blended $/token derived from the authoritative per-model spend.
    blended = _blended_rate_per_model(model_costs)
    poc_costs = _read_poc_costs(spark, c, blended)

    # BEST-EFFORT: system-table enrichment (authoritative but lagged; may be absent).
    enrichment = _enrich_from_system_tables(spark, c)

    remaining = spend_cap - total_cost
    pct_used = (total_cost / spend_cap * 100) if spend_cap > 0 else 0.0
    logger.info(f"Total spend (inference log): ${total_cost:.2f} ({pct_used:.1f}% of cap)")
    logger.info(f"Remaining budget: ${remaining:.2f}")

    recommendations = _generate_recommendations(model_costs, total_cost, spend_cap)
    report_path = _write_cost_report(c, model_costs, poc_costs, enrichment,
                                     total_cost, spend_cap, recommendations)

    by_model = {mc.model: {"total_cost_usd": round(mc.total_cost_usd, 6),
                           "num_calls": mc.num_calls,
                           "input_tokens": mc.input_tokens,
                           "output_tokens": mc.output_tokens,
                           "avg_cost_per_call_usd": round(mc.avg_cost_per_call_usd, 6)}
                for mc in model_costs}
    by_poc = {pc.poc_id: {"tokens": pc.tokens,
                          "est_cost_usd": round(pc.est_cost_usd, 6),
                          "models": pc.models}
              for pc in poc_costs}

    return {
        "status": "success",
        "total_cost_usd": total_cost,
        "spend_cap_usd": spend_cap,
        "remaining_budget_usd": remaining,
        "pct_spent": pct_used,
        "report_path": report_path,
        "breakdown_by_model": by_model,
        "breakdown_by_poc": by_poc,
        "recommendations": recommendations,
        "num_inference_calls": num_calls,
    }


def _get_spark():
    """Return an active SparkSession, or None (guarded — never raises)."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Spark unavailable: {e}")
        return None


def _read_model_costs(spark, c) -> list[ModelCost]:
    """Authoritative per-model aggregation of cfg().inference_table.

    Columns (see lib/pipeline/gateway.py + run_benchmark_live.py write schema):
    request_ts, endpoint, model, input_masked, output, tokens_in, tokens_out,
    cost_usd, initiative, team. Guarded: a missing/empty table returns []."""
    try:
        rows = spark.sql(f"""
            SELECT model,
                   COALESCE(SUM(tokens_in), 0)  AS input_tokens,
                   COALESCE(SUM(tokens_out), 0) AS output_tokens,
                   COALESCE(SUM(cost_usd), 0.0) AS total_cost_usd,
                   COUNT(*)                     AS num_calls
            FROM {c.inference_table}
            GROUP BY model
            ORDER BY total_cost_usd DESC
        """).collect()
    except Exception as e:  # noqa: BLE001 - table may not exist yet
        logger.warning(f"Could not read inference table {c.inference_table}: {e}")
        return []

    out = []
    for r in rows:
        calls = int(r["num_calls"]) or 0
        total = float(r["total_cost_usd"] or 0.0)
        out.append(ModelCost(
            model=r["model"],
            input_tokens=int(r["input_tokens"] or 0),
            output_tokens=int(r["output_tokens"] or 0),
            total_cost_usd=total,
            num_calls=calls,
            avg_cost_per_call_usd=(total / calls) if calls else 0.0,
        ))
    return out


def _blended_rate_per_model(model_costs: list[ModelCost]) -> dict:
    """$/token per model. Prefer the real rate (cost/token from the log); fall
    back to a _RATES-based estimate for models with no logged cost yet."""
    rate = {}
    for mc in model_costs:
        toks = mc.input_tokens + mc.output_tokens
        if toks > 0 and mc.total_cost_usd > 0:
            rate[mc.model] = mc.total_cost_usd / toks
    return rate


def _estimate_rate(model: str, blended: dict) -> float:
    """$/token for a model: real blended rate if known, else a _RATES estimate."""
    if model in blended:
        return blended[model]
    ri, ro = _GATEWAY_RATES.get(model, _DEFAULT_RATE)
    return ((ri + ro) / 2.0) / 1e6  # estimate: avg of in/out $/1M tok


def _read_poc_costs(spark, c, blended: dict) -> list[PocCost]:
    """Best-effort per-PoC cost from kb.bench_results (poc_id, model, tokens).

    The inference log carries no poc_id (and its schema is intentionally
    unchanged), so PoC attribution uses the benchmark table's per-(poc, model)
    token counts times each model's blended $/token. Guarded: no bench table →
    []. Cost here is an ESTIMATE; per-model totals above are authoritative."""
    try:
        bench_table = c.table("kb", "bench_results")
        rows = spark.sql(f"""
            SELECT poc_id, model, COALESCE(SUM(tokens), 0) AS tokens
            FROM {bench_table}
            GROUP BY poc_id, model
        """).collect()
    except Exception as e:  # noqa: BLE001 - bench table may not exist yet
        logger.warning(f"Per-PoC breakdown skipped (bench_results unavailable): {e}")
        return []

    agg: dict[str, dict] = {}
    for r in rows:
        poc = r["poc_id"]
        toks = int(r["tokens"] or 0)
        est = toks * _estimate_rate(r["model"], blended)
        a = agg.setdefault(poc, {"tokens": 0, "est": 0.0, "models": []})
        a["tokens"] += toks
        a["est"] += est
        if r["model"] not in a["models"]:
            a["models"].append(r["model"])
    return sorted(
        [PocCost(poc_id=p, tokens=v["tokens"], est_cost_usd=v["est"], models=v["models"])
         for p, v in agg.items()],
        key=lambda x: x.est_cost_usd, reverse=True)


def _enrich_from_system_tables(spark, c) -> dict | None:
    """Best-effort enrichment from Databricks system tables (authoritative but
    lagged). Returns {'usage': [...], 'billing_usd': float|None} or None. Every
    query is independently guarded so a missing grant/schema skips cleanly."""
    result: dict = {}

    # Requester = the identity running this job (used to scope endpoint_usage).
    requester = None
    try:
        requester = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Could not resolve current_user(): {e}")

    # system.serving.endpoint_usage + served_entities (authoritative token counts).
    try:
        where_req = f"AND u.requester = '{requester}'" if requester else ""
        rows = spark.sql(f"""
            SELECT e.entity_name AS model,
                   COALESCE(SUM(u.input_token_count), 0)  AS input_tokens,
                   COALESCE(SUM(u.output_token_count), 0) AS output_tokens,
                   COUNT(*)                                AS num_calls
            FROM system.serving.endpoint_usage u
            LEFT JOIN system.serving.served_entities e
              ON u.served_entity_id = e.served_entity_id
            WHERE u.request_time >= current_timestamp() - INTERVAL 7 DAYS
              {where_req}
            GROUP BY e.entity_name
            ORDER BY num_calls DESC
        """).collect()
        result["usage"] = [{"model": r["model"], "input_tokens": int(r["input_tokens"] or 0),
                            "output_tokens": int(r["output_tokens"] or 0),
                            "num_calls": int(r["num_calls"] or 0)} for r in rows]
    except Exception as e:  # noqa: BLE001 - table not granted / not enabled
        logger.info(f"system.serving.endpoint_usage enrichment skipped: {e}")

    # system.billing.usage x list_prices (actual $ over a recent window).
    try:
        row = spark.sql("""
            SELECT COALESCE(SUM(u.usage_quantity * lp.pricing.default), 0.0) AS est_usd
            FROM system.billing.usage u
            JOIN system.billing.list_prices lp
              ON u.sku_name = lp.sku_name
             AND u.usage_end_time >= lp.price_start_time
             AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
            WHERE u.billing_origin_product = 'MODEL_SERVING'
              AND u.usage_date >= current_date() - INTERVAL 7 DAYS
        """).collect()[0]
        result["billing_usd"] = float(row["est_usd"] or 0.0)
    except Exception as e:  # noqa: BLE001 - table not granted / not enabled
        logger.info(f"system.billing.usage enrichment skipped: {e}")

    return result or None


def _generate_recommendations(model_costs: list[ModelCost], total: float, cap: float) -> list[str]:
    """Generate cost optimization recommendations from authoritative per-model spend."""
    recs: list[str] = []
    if not model_costs:
        recs.append("ℹ️ No inference rows found yet — run wave4 to populate the gateway log.")
        return recs

    if cap > 0 and total > cap * 0.8:
        recs.append("⚠️ Approaching spend cap: consider cheaper models or fewer tokens per call.")

    costs = {mc.model: mc.total_cost_usd for mc in model_costs}
    cheapest, priciest = min(costs, key=costs.get), max(costs, key=costs.get)
    if priciest != cheapest and costs[priciest] > 0:
        savings = (costs[priciest] - costs[cheapest]) / costs[priciest] * 100
        recs.append(f"💰 {priciest} is the most expensive; {cheapest} is cheapest "
                    f"(~{savings:.0f}% lower) — switch if quality is acceptable.")

    if all(mc.num_calls > 0 and mc.avg_cost_per_call_usd < 0.01 for mc in model_costs):
        recs.append("✅ Per-call cost is low across all models; current allocation looks efficient.")
    return recs


def _write_cost_report(c, model_costs, poc_costs, enrichment, total, cap, recommendations) -> str:
    """Write the markdown cost report next to this module. Guarded: falls back to
    /tmp if the module directory is read-only (e.g. workspace files)."""
    remaining = cap - total
    util = (total / cap * 100) if cap > 0 else 0.0

    md = "# Unity AI Gateway Cost Report\n\n"
    md += f"Generated: {datetime.now().isoformat()}\n\n"
    md += ("Source: **inference log** (`" + c.inference_table + "`) — immediate/authoritative "
           "per-model spend. System-table figures (if shown) are authoritative but **lagged**.\n\n")

    md += "## Spend Summary\n\n"
    md += f"- **Total spend (inference log)**: ${total:.2f}\n"
    md += f"- **Monthly cap**: ${cap:.2f}\n"
    md += f"- **Remaining budget**: ${remaining:.2f}\n"
    md += f"- **Utilization**: {util:.1f}%\n\n"

    md += "## Cost Breakdown by Model (authoritative)\n\n"
    if model_costs:
        md += "| Model | Input Tokens | Output Tokens | Calls | Total Cost | Avg/Call |\n"
        md += "|-------|--------------|---------------|-------|-----------|----------|\n"
        for mc in model_costs:
            md += (f"| {mc.model} | {mc.input_tokens:,} | {mc.output_tokens:,} | {mc.num_calls} | "
                   f"**${mc.total_cost_usd:.4f}** | ${mc.avg_cost_per_call_usd:.4f} |\n")
    else:
        md += "_No inference rows found._\n"

    md += "\n## Cost Breakdown by PoC (estimated)\n\n"
    md += ("_The inference log has no `poc_id` column, so PoC $ is estimated: benchmark "
           "per-PoC token counts × each model's blended $/token._\n\n")
    if poc_costs:
        md += "| PoC | Tokens | Est. Cost | Models |\n"
        md += "|-----|--------|-----------|--------|\n"
        for pc in poc_costs:
            md += f"| {pc.poc_id} | {pc.tokens:,} | ${pc.est_cost_usd:.4f} | {', '.join(pc.models)} |\n"
    else:
        md += "_No benchmark rows found (kb.bench_results) — per-PoC breakdown unavailable._\n"

    if enrichment:
        md += "\n## System-Table Enrichment (authoritative but lagged)\n\n"
        md += ("_System tables have ingestion latency, so a run cannot see its own calls "
               "immediately; treat these as a lagging cross-check, not real-time._\n\n")
        usage = enrichment.get("usage")
        if usage:
            md += "**`system.serving.endpoint_usage`** (last 7 days, this requester):\n\n"
            md += "| Model | Input Tokens | Output Tokens | Calls |\n"
            md += "|-------|--------------|---------------|-------|\n"
            for u in usage:
                md += (f"| {u['model']} | {u['input_tokens']:,} | {u['output_tokens']:,} | "
                       f"{u['num_calls']} |\n")
            md += "\n"
        if enrichment.get("billing_usd") is not None:
            md += (f"**`system.billing.usage` × `list_prices`** (Model Serving, last 7 days): "
                   f"~${enrichment['billing_usd']:.2f}\n\n")

    md += "\n## Recommendations\n\n"
    for rec in (recommendations or ["_No recommendations._"]):
        md += f"- {rec}\n"

    # Write next to this module; fall back to /tmp if that path is read-only.
    primary = Path(__file__).with_name("cost_report.md")
    for target in (primary, Path("/tmp/cost_report.md")):
        try:
            target.write_text(md)
            logger.info(f"Wrote cost report to {target}")
            return str(target)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Could not write cost report to {target}: {e}")
    logger.warning("Cost report could not be written to any location.")
    return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = generate_cost_report()
    print(json.dumps({k: v for k, v in result.items()
                      if k not in ("breakdown_by_model", "breakdown_by_poc")},
                     indent=2, default=str))
