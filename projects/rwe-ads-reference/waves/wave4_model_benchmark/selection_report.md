# AI Agent / Model Benchmarking & Selection Report — RWE ADS Automation

**Generated** on the synthetic PoCs via the Unity AI Gateway wrapper (`lib/pipeline/gateway.py`):
every call is PHI-masked, routed, and logged to `ads_audit.gateway_inference` for cost attribution.
Task: given a study protocol + the **approved-SQL snippet catalog**, select the ordered snippet_ids to
compose the ADS. Scored across the three PoC studies (low/medium/high complexity).

## Results (avg across the 3 PoCs)

| Model | KB grounding | Hallucination | Faithfulness | SQL validity | Latency (s) | Cost (3 PoCs) |
|---|---|---|---|---|---|---|
| **databricks-meta-llama-3-3-70b-instruct** ✅ selected | **1.00** | **0.00** | **1.00** | **1.00** | **1.11** | **$0.00083** |
| databricks-gpt-oss-120b | 1.00 | 0.00 | 1.00 | 1.00 | 1.36 | $0.00118 |
| databricks-llama-4-maverick | 0.33 | 0.67 | 0.33 | 0.33 | 4.82 | $0.00283 |

## Scoring definitions
- **KB grounding** — fraction of selected snippet_ids that exist in the *approved* KB (higher is better).
- **Hallucination rate** — fraction of selected snippet_ids NOT in the approved KB (lower is better; this is
  the charter's key risk — an unapproved snippet must never enter composition).
- **Faithfulness** — coverage of the required stages (cohort + inclusion + outcome).
- **SQL validity** — whether a valid cohort snippet was selected (composition can proceed to EXPLAIN validation).
- **Cost** — actual gateway-logged token cost at pay-per-token rates.

## Selection
**`databricks-meta-llama-3-3-70b-instruct`** — perfect grounding + zero hallucination, fastest, and cheapest.
`gpt-oss-120b` ties on quality (viable fallback). `llama-4-maverick` hallucinated snippet IDs (grounding 0.33)
and is slowest/most expensive — **not recommended** for the KB-grounded selection task.

## Cost validation
Total benchmark spend: **≈ $0.005** for 9 calls / ~6,000 tokens — negligible against the configured
**$2,000/month** hard cap (`gateway.spend_cap_usd_month`). At the selected model's rate, the model cost of
generating one ADS plan is a fraction of a cent; the dominant cost is the serverless SQL that materializes and
validates the ADS, not the LLM.

## Notes
- Guardrail proof: across all gateway calls, a synthetic MRN (`123-45-6789`) and SSN (`456-78-9012`) placed in
  the prompt were **masked** — 0 leaked rows in `ads_audit.gateway_inference` (`lib/phi.py` + gateway wrapper).
- Only pay-per-token-enabled foundation models on this workspace were benchmarked. Deprecated/disabled models
  (e.g. `claude-sonnet-4`) are correctly surfaced as unusable (the gateway/benchmark catches them).
- Re-run: `python waves/wave4_model_benchmark/run_benchmark_live.py` (writes `ads_kb.bench_results` +
  appends `ads_audit.gateway_inference`).
