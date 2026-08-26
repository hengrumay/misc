"""Wave 4 — live model benchmark; model calls go through the in-process gateway_call wrapper (PII mask + audit log), NOT the named ads-ai-gateway endpoint (which 403s on pay-per-token FMs).

For each PoC study x candidate model, ask the model (via lib.pipeline.gateway, which
PHI-masks + logs every call to ads_audit.gateway_inference) to select the ordered
approved KB snippet_ids to compose the ADS, then score:
  kb_grounding, hallucination_rate, faithfulness, sql_validity, latency.

Writes ads_kb.bench_results (+ appends the gateway inference log) and prints
the selection. Cost validation is derived from the inference table (see cost_report.py).

Serverless job entrypoint. All names via lib/config.py.
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import contextlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.config import cfg  # noqa: E402
from lib.pipeline.gateway import gateway_call  # noqa: E402

SYSTEM = ("You are an ADS-planning assistant. From the APPROVED SQL snippet catalog, choose the "
          "ordered snippet_ids needed to build the analysis-ready dataset for the study. Return ONLY "
          "a JSON array of snippet_id strings. Never invent snippet ids that are not in the catalog.")


def _mlflow_init(c, w=None):
    """Return the mlflow module with the SHARED initiative experiment set, or None.

    Logs to the SAME experiment as the gateway LLM spans — both resolve it via
    ``lib.pipeline.gateway.experiment_name`` (``/Users/<identity>/<initiative>``,
    runtime-resolved + portable) — so this run's params/metrics and the gateway
    traces land together and are found by ``scripts/verify_tracing.py``.

    NON-FATAL but NON-SILENT: if mlflow is absent or the experiment can't be set,
    it prints ``[mlflow] TRACING INIT FAILED`` and returns None (the benchmark
    still runs + writes bench_results); it never looks fine while logging nothing.
    """
    try:
        import mlflow
        from lib.pipeline.gateway import experiment_name  # single source of the exp path
    except Exception as e:  # noqa: BLE001 - mlflow/helper unavailable → no tracking
        print(f"[mlflow] TRACING INIT FAILED: {e!r} — wave4 tracking disabled")
        return None
    try:
        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_name(w))
        return mlflow
    except Exception as e:  # noqa: BLE001 - experiment unavailable → no tracking (loud)
        print(f"[mlflow] TRACING INIT FAILED: {e!r} — wave4 tracking disabled")
        return None


def _log_model_metrics(mlf, bench, log_rows):
    """Log per-model benchmark quality + cost metrics to the active MLflow run.

    bench rows:   (poc_id, complexity, model, n_snippets, kb_grounding,
                   hallucination_rate, faithfulness, sql_validity, tokens, latency_s)
    log_rows:     (endpoint, model, input_masked, output, tokens_in, tokens_out,
                   cost_usd, initiative, team)  — cost is only in the gateway log.
    """
    agg = defaultdict(lambda: {"g": [], "h": [], "f": [], "v": [], "lat": [], "tok": 0, "cost": 0.0})
    for r in bench:
        a = agg[r[2]]
        a["g"].append(r[4]); a["h"].append(r[5]); a["f"].append(r[6])
        a["v"].append(r[7]); a["lat"].append(r[9]); a["tok"] += r[8]
    for lr in log_rows:                       # cost lives only in the gateway log rows
        agg[lr[1]]["cost"] += float(lr[6])

    def _mean(xs):
        return float(sum(xs) / len(xs)) if xs else 0.0

    metrics, total_cost = {}, 0.0
    for m, a in agg.items():
        metrics[f"{m}/avg_kb_grounding"] = _mean(a["g"])
        metrics[f"{m}/hallucination_rate"] = _mean(a["h"])
        metrics[f"{m}/faithfulness"] = _mean(a["f"])
        metrics[f"{m}/sql_validity"] = _mean(a["v"])
        metrics[f"{m}/avg_latency_s"] = _mean(a["lat"])
        metrics[f"{m}/total_tokens"] = float(a["tok"])
        metrics[f"{m}/total_cost_usd"] = round(a["cost"], 6)
        total_cost += a["cost"]
    metrics["total_cost_usd"] = round(total_cost, 6)
    metrics["total_gateway_calls"] = float(len(log_rows))
    mlf.log_metrics(metrics)


def main():
    from pyspark.sql import SparkSession
    from databricks.sdk import WorkspaceClient
    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()
    c = cfg()

    kb = spark.sql(f"SELECT snippet_id, category, description FROM {c.kb_table} "
                   f"WHERE status='approved'").collect()
    approved = {r.snippet_id for r in kb}
    catalog_txt = "\n".join(f"- {r.snippet_id} [{r.category}]: {r.description}" for r in kb)

    # MLflow tracking run (guarded): logs candidate models / PoCs / KB size as
    # params and per-model quality + cost as metrics. nullcontext when mlflow is
    # unavailable, so the wave still runs + writes bench_results + selection.
    mlf = _mlflow_init(c, w)
    run_cm = mlf.start_run(run_name=f"{c.initiative}-wave4-bench") if mlf else contextlib.nullcontext()
    with run_cm:
        if mlf:
            try:
                mlf.log_params({
                    "candidate_models": ",".join(c.model_candidates),
                    "n_candidates": len(c.model_candidates),
                    "poc_ids": ",".join(p["id"] for p in c.poc_studies),
                    "n_pocs": len(c.poc_studies),
                    "kb_snippets": len(approved),
                })
            except Exception as e:  # noqa: BLE001 - tracking must never break the wave
                print(f"[bench] mlflow log_params skipped: {e}")

        log_rows, bench = [], []
        for poc in c.poc_studies:
            for model in c.model_candidates:
                prompt = (f"Study: {poc['title']} (complexity {poc['complexity']}).\n"
                          f"APPROVED SNIPPET CATALOG:\n{catalog_txt}\n"
                          f"Return ordered snippet_ids (JSON array): cohort -> inclusion/exclusion -> "
                          f"derivation -> outcome -> assembly.")
                try:
                    res = gateway_call(model, SYSTEM, prompt, w=w, log_rows=log_rows)
                    m = re.search(r"\[.*?\]", res.content, re.S)
                    picks = [str(x) for x in (json.loads(m.group(0)) if m else [])]
                    lat = res.latency_s
                    toks = res.tokens_in + res.tokens_out
                except Exception as e:  # noqa: BLE001
                    picks, lat, toks = [], 0.0, 0
                    print(f"[bench] {model}/{poc['id']} ERROR: {e}")
                grounded = [p for p in picks if p in approved]
                halluc = [p for p in picks if p not in approved]
                grounding = len(grounded) / len(picks) if picks else 0.0
                halluc_rate = len(halluc) / len(picks) if picks else 1.0
                cats = {r.category for r in kb if r.snippet_id in grounded}
                faithful = len({"cohort", "inclusion", "outcome"} & cats) / 3.0
                validity = 1.0 if ({"coh_base_prevalence", "coh_new_user"} & set(grounded)) else 0.0
                bench.append((poc["id"], poc["complexity"], model, len(picks), round(grounding, 3),
                              round(halluc_rate, 3), round(faithful, 3), validity, int(toks), round(lat, 2)))

        bcols = ["poc_id", "complexity", "model", "n_snippets", "kb_grounding", "hallucination_rate",
                 "faithfulness", "sql_validity", "tokens", "latency_s"]
        spark.createDataFrame(bench, bcols).write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(c.table("kb", "bench_results"))

        if mlf:
            try:
                _log_model_metrics(mlf, bench, log_rows)
            except Exception as e:  # noqa: BLE001 - tracking must never break the wave
                print(f"[bench] mlflow log_metrics skipped: {e}")

        _write_gateway_log(spark, c, log_rows)
    print("[bench] wrote", len(bench), "rows;", len(log_rows), "gateway calls logged")


def _write_gateway_log(spark, c, log_rows):
    """Append the batched (masked) gateway inference rows to cfg().inference_table."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)
    if log_rows:
        schema = StructType([
            StructField("endpoint", StringType()), StructField("model", StringType()),
            StructField("input_masked", StringType()), StructField("output", StringType()),
            StructField("tokens_in", IntegerType()), StructField("tokens_out", IntegerType()),
            StructField("cost_usd", DoubleType()), StructField("initiative", StringType()),
            StructField("team", StringType())])
        (spark.createDataFrame(log_rows, schema).withColumn("request_ts", F.current_timestamp())
            .select("request_ts", "endpoint", "model", "input_masked", "output",
                    "tokens_in", "tokens_out", "cost_usd", "initiative", "team")
            .write.mode("append").saveAsTable(c.inference_table))


if __name__ == "__main__":
    main()
