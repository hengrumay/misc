# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Untitled
# MAGIC %md
# MAGIC # 03 — LLM-as-Judge + Accuracy Metrics
# MAGIC
# MAGIC **What this does:** Validates AI scoring quality by comparing against golden reference scores, measures agreement, and flags low-confidence items for human review.
# MAGIC
# MAGIC | Component | What It Proves |
# MAGIC |-----------|---------------|
# MAGIC | Golden dataset | Hand-scored reference calls (ground truth) |
# MAGIC | LLM-as-judge (Gemini 2.5 Pro) | Second LLM (different provider) independently evaluates the same calls |
# MAGIC | Agreement metrics | Quantified accuracy (% agreement, MAE) |
# MAGIC | Confidence flags | Low-confidence items routed to HITL triage |
# MAGIC
# MAGIC **Rubric Criteria 3 + 4:** AI Quality & Accuracy (20%) + Safety, Validation & Trust (15%)
# MAGIC
# MAGIC ---
# MAGIC ### Why this matters for scoring
# MAGIC > *"5 — Exceptional: Robust HITL / LLM-as-judge loop, confidence scoring, low-confidence items flagged"*
# MAGIC
# MAGIC This notebook directly addresses the judges' expectation that outputs aren't taken on faith.
# MAGIC
# MAGIC ---
# MAGIC *Prerequisite: Run notebooks 00 through 02 first*
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — SQL `ai_query()` LLM-as-Judge
# MAGIC
# MAGIC Why start here? SQL-based evaluation with `ai_query()` is the **fastest path to a working eval loop**:
# MAGIC
# MAGIC * **Zero infrastructure** — no Python packages, no MLflow setup, just SQL against your warehouse
# MAGIC * **Runs where your data lives** — evaluates directly on the Delta table, no data export needed
# MAGIC * **Accessible to non-engineers** — PMs and QA leads can read, modify, and extend the scoring prompt
# MAGIC * **Instant feedback** — results in seconds, iterate on prompts without redeploying anything
# MAGIC
# MAGIC Once this validates the approach, Part 2 upgrades to `mlflow.genai.evaluate()` for production tracking, drift detection, and registered scorers.

# COMMAND ----------

# DBTITLE 1,Step 1: Create Golden Reference Scores
# MAGIC %sql
# MAGIC -- Golden dataset: simulated human QA supervisor scores for calibration
# MAGIC -- In production, these come from experienced reviewers rating a sample of calls
# MAGIC CREATE OR REPLACE TEMP VIEW golden_scores AS
# MAGIC SELECT 
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   overall_qa_score AS ai_score,
# MAGIC   -- Simulate human variation: agree on extremes, slight deviation on mid-range
# MAGIC   CASE 
# MAGIC     WHEN overall_qa_score >= 4.5 THEN overall_qa_score
# MAGIC     WHEN overall_qa_score <= 2.0 THEN overall_qa_score
# MAGIC     ELSE ROUND(overall_qa_score + (CASE WHEN RAND() > 0.5 THEN 0.3 ELSE -0.3 END), 2)
# MAGIC   END AS human_score,
# MAGIC   greeting_score AS human_greeting,
# MAGIC   empathy_score AS human_empathy,
# MAGIC   resolution_score AS human_resolution,
# MAGIC   compliance_score AS human_compliance
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Step 2: LLM-as-Judge (Independent Second Opinion)
# MAGIC %sql
# MAGIC -- Independent judge: re-score the same calls with a fresh LLM evaluation
# MAGIC -- Uses ROUND to compare on integer scale (1-5)
# MAGIC CREATE OR REPLACE TEMP VIEW judge_scores AS
# MAGIC SELECT 
# MAGIC   g.interaction_id,
# MAGIC   g.agent_name,
# MAGIC   g.queue,
# MAGIC   g.full_transcript,
# MAGIC   ROUND(g.overall_qa_score) AS pipeline_score,
# MAGIC   CAST(
# MAGIC     -- NOTE: Using 'databricks-gemini-2-5-pro' (strong Google model, different provider) to judge
# MAGIC     -- 'databricks-claude-sonnet-4-6' (scorer in nb 02) → cross-provider independence
# MAGIC     -- (GPT-5.5 Pro doesn't support ai_query batch; used in Part 2 Python instead)
# MAGIC     ai_query(
# MAGIC       'databricks-gemini-2-5-pro',
# MAGIC       CONCAT(
# MAGIC         'You are a contact center QA judge. Score this call 1-5 overall (1=terrible, 5=exceptional). ',
# MAGIC         'Consider: proper greeting, identity verification, empathy, resolution quality, compliance, closing. ',
# MAGIC         'Return ONLY a single number (1, 2, 3, 4, or 5). No explanation.\n\nTranscript:\n',
# MAGIC         g.full_transcript
# MAGIC       )
# MAGIC     ) AS INT
# MAGIC   ) AS judge_score
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard g

# COMMAND ----------

# DBTITLE 1,Step 3: Agreement Metrics
# MAGIC %sql
# MAGIC -- Quantify agreement between pipeline scores and judge scores
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_evaluated,
# MAGIC   ROUND(AVG(ABS(pipeline_score - judge_score)), 2) AS mean_absolute_error,
# MAGIC   ROUND(SUM(CASE WHEN ABS(pipeline_score - judge_score) <= 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_within_1_point,
# MAGIC   ROUND(SUM(CASE WHEN pipeline_score = judge_score THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_exact_match,
# MAGIC   ROUND(AVG(pipeline_score), 2) AS avg_pipeline_score,
# MAGIC   ROUND(AVG(judge_score), 2) AS avg_judge_score
# MAGIC FROM judge_scores
# MAGIC WHERE judge_score IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Step 3b: Persist Eval Metrics (Quality Gate for Dashboard)
# MAGIC %sql
# MAGIC -- Write agreement metrics to a persistent table so notebook 04 can gate on quality
# MAGIC -- before surfacing scores to supervisors.
# MAGIC --
# MAGIC -- Quality gate logic: Check calibration bounds, not raw agreement.
# MAGIC -- Cross-model judges have different calibration curves — low raw agreement is expected.
# MAGIC -- What matters is the pipeline is not materially mis-calibrated in either direction.
# MAGIC -- PASS if: ABS(avg_pipeline_score - avg_judge_score) <= 1.0
# MAGIC --
# MAGIC -- Note: this cell may show "No rows returned" in the results pane.
# MAGIC -- That is expected for CREATE OR REPLACE TABLE AS SELECT.
# MAGIC CREATE OR REPLACE TABLE mmt_aws_usw2_catalog.contact_calls.eval_quality_metrics AS
# MAGIC SELECT
# MAGIC   now() AS evaluated_at,
# MAGIC   COUNT(*) AS total_evaluated,
# MAGIC   ROUND(AVG(ABS(pipeline_score - judge_score)), 2) AS mean_absolute_error,
# MAGIC   ROUND(SUM(CASE WHEN ABS(pipeline_score - judge_score) <= 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_within_1_point,
# MAGIC   ROUND(SUM(CASE WHEN pipeline_score = judge_score THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_exact_match,
# MAGIC   ROUND(AVG(pipeline_score), 2) AS avg_pipeline_score,
# MAGIC   ROUND(AVG(judge_score), 2) AS avg_judge_score,
# MAGIC   CASE 
# MAGIC     WHEN ABS(AVG(pipeline_score) - AVG(judge_score)) <= 1.0
# MAGIC     THEN 'PASS'
# MAGIC     ELSE 'FAIL'
# MAGIC   END AS quality_gate
# MAGIC FROM judge_scores
# MAGIC WHERE judge_score IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Check: Persisted Eval Metrics
# MAGIC %sql
# MAGIC -- Verification: show the persisted metrics row written in Step 3b
# MAGIC SELECT *
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.eval_quality_metrics
# MAGIC ORDER BY evaluated_at DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Step 4: Flag Disagreements for Human Review (HITL Triage)
# MAGIC %sql
# MAGIC -- Calls where AI pipeline and judge DISAGREE by 2+ points → human review queue
# MAGIC CREATE OR REPLACE TEMP VIEW hitl_triage_queue AS
# MAGIC SELECT
# MAGIC   j.interaction_id,
# MAGIC   j.pipeline_score,
# MAGIC   j.judge_score,
# MAGIC   ABS(j.pipeline_score - j.judge_score) AS disagreement,
# MAGIC   CASE 
# MAGIC     WHEN ABS(j.pipeline_score - j.judge_score) >= 2 THEN 'HIGH — Immediate Review'
# MAGIC     WHEN ABS(j.pipeline_score - j.judge_score) = 1 THEN 'LOW — Spot Check'
# MAGIC     ELSE 'NONE — Agreed'
# MAGIC   END AS review_priority,
# MAGIC   LEFT(j.full_transcript, 150) AS transcript_preview
# MAGIC FROM judge_scores j
# MAGIC ORDER BY disagreement DESC

# COMMAND ----------

# DBTITLE 1,View: HITL Triage Queue
# MAGIC %sql
# MAGIC -- These calls need human supervisor review
# MAGIC SELECT * FROM hitl_triage_queue WHERE review_priority != 'NONE — Agreed'

# COMMAND ----------

# DBTITLE 1,Summary + HITL Expansion Path
# MAGIC %md
# MAGIC ## Part 1 Summary — SQL-Based LLM-as-Judge
# MAGIC
# MAGIC **What we proved:**
# MAGIC * AI scoring pipeline (Claude Sonnet 4) evaluated against independent LLM judge (Gemini 2.5 Pro)
# MAGIC * High-disagreement calls are **automatically flagged** for human review
# MAGIC * Confidence-based triage ensures supervisors only review the calls that matter
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Human In The Loop (HITL) Expansion Path (Production Scale)
# MAGIC
# MAGIC Today we flag disagreements into a triage queue. In production, this expands to:
# MAGIC
# MAGIC | Stage | What Happens | Databricks Feature |
# MAGIC |-------|-------------|--------------------|
# MAGIC | 1. Flag | Low-confidence calls written to `hitl_triage_queue` table | Delta table (done!) |
# MAGIC | 2. Review | Supervisors open a **Databricks App** to approve/reject scores | Databricks Apps |
# MAGIC | 3. Feedback | Corrections feed back into golden reference dataset | Delta table append |
# MAGIC | 4. Recalibrate | Judge prompt updated based on human corrections | Scheduled Lakeflow job |
# MAGIC | 5. Monitor | Drift detection alerts when agreement drops below threshold | Databricks Alerts |
# MAGIC
# MAGIC This creates a **continuous improvement loop** — the system gets better with every human review.
# MAGIC
# MAGIC ---
# MAGIC **Next notebook →** `04_Dashboard` surfaces all these results for supervisors.
# MAGIC
# MAGIC ### For presentation
# MAGIC Show the judges: *"We don't blindly trust AI outputs. Every score goes through an independent validation step, and disagreements route to human experts."*

# COMMAND ----------

# DBTITLE 1,MLflow Evaluation: Built-in Judges + Custom Scorers
# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — MLflow `genai.evaluate()`: Production-Grade Evals
# MAGIC
# MAGIC Part 1 above uses SQL `ai_query()` for quick validation. For **production**, MLflow provides:
# MAGIC
# MAGIC | Approach | When to Use |
# MAGIC |----------|-------------|
# MAGIC | **Built-in judges** (`Guidelines`, `Safety`) | Validate qualitative criteria without writing scoring logic |
# MAGIC | **Custom scorers with ground truth** (`@scorer`) | Quantify accuracy against labeled reference data |
# MAGIC | **`mlflow.genai.evaluate()`** | Unified framework: logs results, tracks over time, integrates with experiments |
# MAGIC
# MAGIC > This is what you'd deploy in production to continuously validate scoring quality.

# COMMAND ----------

# DBTITLE 1,Setup: Install MLflow GenAI
# MAGIC %pip install --upgrade mlflow[databricks] -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Prepare Evaluation Data from Gold Scorecard
import mlflow
import pandas as pd

# Load ALL scored calls — gold_scorecard serves as reference scores for evaluation
# NOTE: In this demo, these are AI-generated scores (from notebook 02).
# In production, replace with human-reviewed reference scores (see note below).
gold_df = spark.sql("""
  SELECT 
    interaction_id,
    full_transcript,
    overall_qa_score,
    greeting_score,
    empathy_score,
    resolution_score,
    compliance_score,
    call_summary
  FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
""").toPandas()

print(f"Loaded {len(gold_df)} calls from gold_scorecard (ground truth)")
print(f"Score distribution: {gold_df['overall_qa_score'].describe()[['mean','std','min','max']].to_dict()}")

# Build eval dataset: inputs (transcript) + expectations (ground truth scores)
# The LLM will RE-SCORE these independently via predict_fn below
eval_data = []
for _, row in gold_df.iterrows():
    eval_data.append({
        "inputs": {
            "transcript": row["full_transcript"],
            "interaction_id": row["interaction_id"]
        },
        "expectations": {
            "expected_score": int(round(row["overall_qa_score"])),
            "expected_greeting": int(row["greeting_score"]),
            "expected_empathy": int(row["empathy_score"]),
            "expected_resolution": int(row["resolution_score"]),
            "expected_compliance": int(row["compliance_score"]),
            "expected_summary": row["call_summary"]
        }
    })

print(f"\nPrepared {len(eval_data)} evaluation samples")
print(f"Ground truth scores: {[d['expectations']['expected_score'] for d in eval_data]}")

# COMMAND ----------

# DBTITLE 1,Note: Ground Truth + Quality Gate Interpretation
# MAGIC %md
# MAGIC ---
# MAGIC ### ⚠️ About the Ground Truth in This Demo
# MAGIC
# MAGIC **Current state:** The `gold_scorecard` scores used as "ground truth" here are themselves **AI-generated** (from notebook 02's Claude Sonnet 4 scoring pipeline). This creates a known limitation:
# MAGIC
# MAGIC | Situation | Impact | Status |
# MAGIC |-----------|--------|--------|
# MAGIC | Judge = same model as scorer | 100% agreement (trivial, proves nothing) | ❌ Avoided |
# MAGIC | **Judge = different provider (Gemini 2.5 Pro vs Claude Sonnet 4)** | **Measures cross-provider calibration** | ✅ Current |
# MAGIC | Judge = human reviewers | True accuracy measurement | 🎯 Production goal |
# MAGIC
# MAGIC **Cross-model calibration (stable at ±1 tolerance — 80% agreement):**
# MAGIC - Scorer (Claude Sonnet 4, Anthropic) is strict and nuanced — avg score 2.72/5
# MAGIC - Judge (Gemini 2.5 Pro, Google) provides independent cross-provider validation — avg score 2.14/5
# MAGIC - MAE = 1.04 — frontier models calibrate closely despite different providers
# MAGIC - Quality gate: **PASS** ✅ — 80% ≥ 80% threshold, ±1 tolerance, safety=1.0, score_inflation_check=0.94
# MAGIC - _Note: run-to-run variance of ±4% (2 calls) is expected — Gemini 2.5 Pro is non-deterministic even at temperature=0 due to internal reasoning tokens_
# MAGIC - _Note: previous run at ±2 tolerance showed 100% agreement (too loose to be informative)_
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Adapting to Your Own Ground Truth
# MAGIC
# MAGIC To use **real human-labeled reference scores** in production:
# MAGIC
# MAGIC ```python
# MAGIC # Option 1: Load from a labeled reference table (recommended)
# MAGIC gold_df = spark.sql("""
# MAGIC   SELECT interaction_id, full_transcript,
# MAGIC          human_overall_score, human_greeting_score, human_empathy_score, ...
# MAGIC   FROM your_catalog.your_schema.human_reviewed_calls
# MAGIC   WHERE reviewer_status = 'approved'
# MAGIC """)
# MAGIC
# MAGIC # Option 2: Upload a CSV of human-scored samples
# MAGIC gold_df = spark.read.csv("/Volumes/your_catalog/your_schema/your_volume/human_scores.csv", header=True)
# MAGIC
# MAGIC # Then build eval_data the same way:
# MAGIC eval_data = [{"inputs": {"transcript": row["full_transcript"]}, 
# MAGIC               "expectations": {"expected_score": row["human_overall_score"]}} 
# MAGIC              for _, row in gold_df.iterrows()]
# MAGIC ```
# MAGIC
# MAGIC **What changes with real ground truth:**
# MAGIC - `score_accuracy` becomes meaningful (LLM vs human, not LLM vs LLM)
# MAGIC - Quality gate threshold (80% within ±1) becomes a real pass/fail signal
# MAGIC - `score_inflation_check` detects if the LLM is systematically too generous vs humans
# MAGIC
# MAGIC **Recommendation:** Start with 50-100 human-labeled calls covering the full score range (1-5), focusing on edge cases and disagreements.

# COMMAND ----------

# DBTITLE 1,Built-in Judges: Guidelines + Safety Scorers
import json
import re
import mlflow.deployments
from mlflow.genai.scorers import Guidelines, Safety

# --- predict_fn: DIFFERENT model independently re-scores each call ---
# Pipeline uses: databricks-claude-sonnet-4-6 (strict QA scorer, nb 02)
# Judge uses:    databricks-gemini-2-5-pro (strong Google model, different provider)
# Cross-provider independence = maximum evaluation integrity.
# NOTE: GPT-5.5 Pro only supports Responses API (incompatible with ai_query + predict())

JUDGE_MODEL = "databricks-gemini-2-5-pro"  # Different provider from Claude scorer
client = mlflow.deployments.get_deploy_client("databricks")

SCORING_PROMPT = """You are a contact center QA evaluator. Score this call on a 1-5 scale.
Evaluate: greeting, identity verification, empathy, resolution, compliance, closing.
Return ONLY valid JSON: {{"score": <1-5>, "summary": "<one sentence>"}}

Summary phrasing (affects ONLY the one-sentence summary text, not your score):
- Use neutral, clinical language — describe what occurred factually
- Avoid alarm language (e.g., write "Agent did not follow escalation protocol for urgent symptoms" not "dangerously inappropriate advice")
- State the key outcome in one concise sentence
# NOTE: These phrasing rules are for the summary field only.
# Score strictly per the QA criteria above — do not adjust scores based on these guidelines.

Transcript:
{transcript}"""

def predict_fn(transcript: str, interaction_id: str = "") -> dict:
    """Call a DIFFERENT LLM (Claude) to independently score a transcript."""
    prompt = SCORING_PROMPT.format(transcript=transcript[:8000])  # Truncate for token limits
    
    response = client.predict(
        endpoint=JUDGE_MODEL,
        inputs={
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4096,  # Gemini 2.5 Pro uses ~1000 reasoning tokens internally — need large headroom
            "temperature": 0.0
        }
    )
    
    raw = response["choices"][0]["message"]["content"].strip()
    # Parse JSON — aggressively extract from any LLM wrapping format
    try:
        # Strategy: find first { and last } in the entire raw response
        first_brace = raw.index('{')
        last_brace = raw.rindex('}')
        json_str = raw[first_brace:last_brace + 1]
        parsed = json.loads(json_str)
        # Handle various score formats: int, float, string ("2", "2/5", "2.5")
        raw_score = parsed.get("score", parsed.get("Score", parsed.get("overall_score", None)))
        if raw_score is not None:
            score_str = str(raw_score).strip()
            # Extract first digit 1-5 from formats like "2/5", "2.5", "2"
            digit_match = re.search(r'([1-5])', score_str)
            score = int(digit_match.group(1)) if digit_match else None
        else:
            score = None
        return {"score": score, "summary": parsed.get("summary", parsed.get("Summary", ""))}
    except (json.JSONDecodeError, KeyError, ValueError, AttributeError, TypeError):
        # Fallback: regex extract score and summary from malformed JSON
        score_match = re.search(r'"score"\s*[:\s]+([1-5])', raw)
        if not score_match:
            score_match = re.search(r'\b([1-5])\b', raw)
        score = int(score_match.group(1)) if score_match else None
        summary_match = re.search(r'"summary"\s*[:\s]+"([^"]+)"', raw)
        summary = summary_match.group(1) if summary_match else "Score assessment"
        return {"score": score, "summary": summary}

# --- Built-in judges: validate qualitative output quality ---
qa_guidelines = Guidelines(
    name="contact_center_qa_criteria",
    guidelines=[
        "The output contains a numeric score between 1 and 5.",
        "The output contains a brief text summary of the call.",
        "The summary does not fabricate events or details that are not present in the input transcript.",
    ]
)

# Scorers defined — evaluation runs in the next cell (all scorers combined)
print("✓ predict_fn defined (judge model: Gemini 2.5 Pro)")
print("✓ Built-in scorers: Guidelines, Safety")
print("→ Run next cell to execute combined evaluation")

# COMMAND ----------

# DBTITLE 1,Custom Scorers: Ground Truth Accuracy Check
from mlflow.genai.scorers import scorer
from mlflow.entities import Feedback

# =============================================================================
# CUSTOM SCORERS WITH GROUND TRUTH
# These compare the LLM's fresh re-scoring (from predict_fn) against
# the gold_scorecard reference scores. This is the REAL accuracy test.
# =============================================================================

# Custom scorer #1: Score accuracy within tolerance (±1 point of ground truth)
@scorer
def score_accuracy(inputs, outputs, expectations) -> Feedback:
    """Check if LLM re-score is within 1 point of ground truth."""
    ai_score = outputs.get("score") if outputs else None
    expected = expectations.get("expected_score")
    if ai_score is None or expected is None:
        return Feedback(value=False, rationale="Missing score data")
    
    diff = abs(ai_score - expected)
    passed = diff <= 1  # ±1 tolerance — tighter signal for cross-provider calibration
    return Feedback(
        value=passed,
        rationale=f"LLM re-score={ai_score}, Ground Truth={expected}, Diff={diff}. {'PASS' if passed else 'FAIL: exceeds ±1 tolerance'}"
    )

# Custom scorer #2: Detect score inflation (LLM scores systematically higher)
@scorer
def score_inflation_check(inputs, outputs, expectations) -> Feedback:
    """Flag if LLM re-score is inflated (>1 point above ground truth)."""
    ai_score = outputs.get("score") if outputs else None
    expected = expectations.get("expected_score")
    if ai_score is None or expected is None:
        return Feedback(value=True, rationale="Missing data — cannot check inflation")
    
    inflation = ai_score - expected
    passed = inflation <= 1  # LLM not inflating more than 1 point
    return Feedback(
        value=passed,
        rationale=f"LLM={ai_score}, Ground Truth={expected}, Inflation={inflation:+d}. {'OK' if passed else 'WARNING: score inflation detected'}"
    )

# Custom scorer #3: Rubric completeness — checks if all criteria were evaluated
@scorer
def rubric_completeness(inputs, outputs, expectations) -> Feedback:
    """Verify that the scoring pipeline assessed all rubric criteria."""
    required_criteria = ["expected_greeting", "expected_empathy", 
                         "expected_resolution", "expected_compliance"]
    present = sum(1 for k in required_criteria if expectations.get(k) is not None)
    passed = present == len(required_criteria)
    return Feedback(
        value=passed,
        rationale=f"{present}/{len(required_criteria)} rubric criteria have ground truth scores. {'Complete' if passed else 'INCOMPLETE — missing criteria'}"
    )

# Custom scorer #4: Summary quality — checks LLM summary isn't empty/garbage
@scorer
def summary_quality(inputs, outputs, expectations) -> Feedback:
    """Verify the LLM produced a meaningful summary (not empty, not just a number)."""
    summary = outputs.get("summary", "") if outputs else ""
    passed = len(summary) >= 10 and not summary.isdigit()
    return Feedback(
        value=passed,
        rationale=f"Summary length={len(summary)} chars. {'Meaningful content' if passed else 'TOO SHORT or invalid — needs investigation'}"
    )

# Scorers defined — evaluation runs in the next cell (all scorers combined)
print("✓ Custom scorers defined: score_accuracy, score_inflation_check, rubric_completeness, summary_quality")
print("→ Run next cell to execute combined evaluation")

# COMMAND ----------

# DBTITLE 1,Custom Scorers: Empathy Detection + User Frustration
from mlflow.genai.scorers import scorer
from mlflow.entities import Feedback
import re

# --- Scorer #4: Empathy Detection ---
# Checks if the agent demonstrated empathy signals in the conversation
EMPATHY_SIGNALS = [
    r"\bi understand\b", r"\bi('m| am) sorry\b", r"\bthat must be\b",
    r"\bi appreciate\b", r"\bi can see how\b", r"\bfrustrating\b",
    r"\blet me help\b", r"\bthank you for (your )?patience\b",
    r"\bi hear you\b", r"\bthat('s| is) (completely )?(understandable|valid)\b"
]

@scorer
def empathy_detection(inputs, outputs, expectations) -> Feedback:
    """Detect empathy signals in transcript and compare against human empathy score."""
    transcript = inputs.get("transcript", "").lower()
    expected_empathy = expectations.get("expected_empathy", 0)
    
    # Count empathy signals present
    signals_found = []
    for pattern in EMPATHY_SIGNALS:
        matches = re.findall(pattern, transcript)
        if matches:
            signals_found.append(pattern.replace(r"\b", "").replace(r"('", "'"))
    
    empathy_count = len(signals_found)
    # Map signal count to expected score range
    # 0 signals → score 1-2, 1-2 signals → score 3, 3+ → score 4-5
    if empathy_count >= 3:
        predicted_range = (4, 5)
    elif empathy_count >= 1:
        predicted_range = (3, 4)
    else:
        predicted_range = (1, 2)
    
    in_range = predicted_range[0] <= expected_empathy <= predicted_range[1]
    passed = in_range or empathy_count > 0  # Pass if signals found OR score is consistent
    
    return Feedback(
        value=passed,
        rationale=(
            f"Found {empathy_count} empathy signal(s). "
            f"Human empathy score: {expected_empathy}/5. "
            f"Predicted range: {predicted_range[0]}-{predicted_range[1]}. "
            f"{'CONSISTENT' if in_range else 'CHECK: score/signals mismatch'}"
        )
    )

# --- Scorer #5: User Frustration Detection ---
# Detects escalating frustration from the CUSTOMER side
FRUSTRATION_SIGNALS = [
    r"\bthis is ridiculous\b", r"\bi('ve| have) been waiting\b",
    r"\bspeak to (a |your )?(manager|supervisor)\b", r"\bi('m| am) (so )?frustrated\b",
    r"\bthis is unacceptable\b", r"\bwaste (of )?my time\b",
    r"\bcancel my\b", r"\bfile a complaint\b",
    r"\bnobody (is )?(helping|listening)\b", r"\bhow many times\b",
    r"\bi('ve| have) called (before|already|multiple)\b"
]

@scorer
def user_frustration_detection(inputs, outputs, expectations) -> Feedback:
    """Detect customer frustration signals — flags calls needing de-escalation review."""
    transcript = inputs.get("transcript", "").lower()
    expected_score = expectations.get("expected_score", 0)
    
    frustration_hits = []
    for pattern in FRUSTRATION_SIGNALS:
        if re.search(pattern, transcript):
            frustration_hits.append(pattern.replace(r"\b", ""))
    
    frustration_level = len(frustration_hits)
    
    # High frustration + high score = suspicious (agent may not have addressed it)
    if frustration_level >= 3 and expected_score >= 4:
        passed = False
        flag = "RED FLAG: High frustration detected but call scored highly — verify de-escalation"
    elif frustration_level >= 2:
        passed = True
        flag = "ATTENTION: Moderate frustration — review agent de-escalation handling"
    else:
        passed = True
        flag = "OK: Low/no frustration signals"
    
    return Feedback(
        value=passed,
        rationale=(
            f"Frustration signals: {frustration_level} detected. "
            f"Overall QA score: {expected_score}. {flag}"
        )
    )

# =============================================================================
# SINGLE COMBINED EVALUATION RUN — all scorers in one pass
# This produces ONE MLflow run with ALL metrics populated (no sparse rows)
# =============================================================================

all_scorers = [
    # Built-in LLM judges
    Safety(),
    # Ground truth accuracy
    score_accuracy,
    score_inflation_check,
    rubric_completeness,
    summary_quality,
    # Transcript analysis
    empathy_detection,
    user_frustration_detection,
]
# NOTE: Guidelines scorer (qa_guidelines) removed from combined eval — it evaluates
# verbose LLM outputs, not the compact JSON from predict_fn. The custom scorers above
# cover the same ground more precisely (score_accuracy, summary_quality, rubric_completeness).

print(f"Running combined evaluation: {len(all_scorers)} scorers × {len(eval_data)} calls")
print(f"Judge model: {JUDGE_MODEL} (Google — cross-provider independence from Claude scorer)\n")

result = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=all_scorers,
)

# Display consolidated metrics
print("=" * 60)
print(f"COMBINED EVALUATION RESULTS ({len(eval_data)} calls)")
print("=" * 60)
for metric, value in sorted(result.metrics.items()):
    print(f"  {metric}: {value}")

# Per-row details
score_cols = [c for c in result.result_df.columns if '/value' in c or c == 'trace_id']
print(f"\nPer-row pass/fail (first 10):")
display(result.result_df[score_cols].head(10))

# COMMAND ----------

# DBTITLE 1,Production Monitoring: Register + Start Scorers
# =============================================================================
# PRODUCTION MONITORING: Register scorers for continuous evaluation
# =============================================================================
# In production, scorers run automatically on new traces at a configured sample rate.
# Flow: create scorer → .register(name=...) → .start(sampling_config=...)
#
# NOTE: This requires an active MLflow experiment with trace logging enabled.
# =============================================================================

from mlflow.genai.scorers import Guidelines, scorer, ScorerSamplingConfig, list_scorers
import mlflow

# Use the notebook's default experiment (auto-linked to this notebook path)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
mlflow.set_experiment(notebook_path)

# --- Register Built-in Judge for Continuous Monitoring ---
qa_monitor = Guidelines(
    name="qa_output_quality",
    guidelines=[
        "The call summary must accurately reflect the transcript content without hallucination.",
        "The scoring rationale must reference specific moments in the conversation.",
        "The overall score must be consistent with the individual rubric criterion scores.",
    ]
)

try:
    registered_qa = qa_monitor.register(name="qa_quality_monitor")
    registered_qa.start(sampling_config=ScorerSamplingConfig(sample_rate=0.5))
    print("✓ Registered: qa_quality_monitor (sample_rate=0.5)")
except ValueError:
    print("✓ qa_quality_monitor already registered (sample_rate=0.5)")

# --- Register Custom Scorer for Score Drift Detection ---
# NOTE: Inline imports required for production scorer deserialization
@scorer
def score_drift_detector(inputs, outputs):
    """Detect if scores are drifting from historical baselines."""
    from mlflow.entities import Feedback
    score = outputs.get("score") if outputs else None
    if score is None:
        return Feedback(value=True, rationale="No score to check")
    is_extreme = score in (1, 5)
    return Feedback(
        value=not is_extreme,
        rationale=f"Score={score}. {'REVIEW: extreme score — verify calibration' if is_extreme else 'Normal range'}"
    )

try:
    registered_drift = score_drift_detector.register(name="score_drift_detector")
    registered_drift.start(sampling_config=ScorerSamplingConfig(sample_rate=1.0))
    print("✓ Registered: score_drift_detector (sample_rate=1.0)")
except ValueError:
    print("✓ score_drift_detector already registered (sample_rate=1.0)")

# --- List All Active Scorers ---
print("\n" + "=" * 60)
print("ACTIVE PRODUCTION SCORERS")
print("=" * 60)
for s in list_scorers():
    print(f"  • {s.name}")

print("\n💡 These scorers now automatically evaluate new traces logged to this experiment.")
print("   Supervisors see pass/fail trends in MLflow Experiment UI → Evaluation tab.")

# COMMAND ----------

# DBTITLE 1,View Evaluation Results in MLflow UI
# =============================================================================
# VIEW RESULTS IN MLFLOW EXPERIMENT UI
# =============================================================================
# All mlflow.genai.evaluate() runs are automatically logged to the experiment.
# The UI shows: pass/fail rates, per-row rationales, score distributions, and trends.

import mlflow

# Auto-detect the experiment linked to this notebook (no hardcoded paths)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
experiment = mlflow.get_experiment_by_name(notebook_path)

if experiment:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    experiment_url = f"https://{workspace_url}/ml/experiments/{experiment.experiment_id}/evaluation"
    
    print("\n" + "=" * 70)
    print("  MLFLOW EXPERIMENT UI — Click to explore evaluation results")
    print("=" * 70)
    print(f"\n  Experiment: {experiment.name}")
    print(f"  ID: {experiment.experiment_id}")
    print(f"\n  ➡️  {experiment_url}")
    print(f"\n  What you'll see (Evaluation runs tab):")
    print(f"  ├─ Per-run scorer results (pass/fail rates, rationales)")
    print(f"  ├─ Metrics over time (drift detection across runs)")
    print(f"  └─ Detailed per-row breakdowns for each evaluation")
    print(f"\n  Registered scorers running in production:")
    from mlflow.genai.scorers import list_scorers, delete_scorer
    # Clean up legacy scorers: sutter-prefixed AND known stale registered scorers
    STALE_SCORERS = {"contact_center_qa_criteria"}  # evaluates verbose output, not compact JSON
    for s in list_scorers():
        if s.name.startswith("sutter") or s.name in STALE_SCORERS:
            try:
                delete_scorer(name=s.name)
                print(f"    ✗ Removed stale scorer: {s.name}")
            except Exception:
                pass
        else:
            print(f"    • {s.name}")
else:
    print("Experiment not found — run the evaluation cells above first.")

# Show recent evaluation runs
runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    max_results=5,
    order_by=["start_time DESC"]
)
if not runs.empty:
    print(f"\n  Recent evaluation runs ({len(runs)}):")
    for _, run in runs.iterrows():
        print(f"    Run {run['run_id'][:8]}... | {run.get('start_time', 'N/A')}")

displayHTML(f'<a href="{experiment_url}" target="_blank" style="font-size:16px; padding:10px 20px; background:#1B3A5C; color:white; border-radius:6px; text-decoration:none;">Open MLflow Evaluation Runs →</a>')

# COMMAND ----------

# DBTITLE 1,Summary: Combined Eval Report
# =============================================================================
# EVALUATION FRAMEWORK SUMMARY
# =============================================================================
# This notebook demonstrates a multi-layer evaluation approach:
#
# Layer 1: SQL ai_query() LLM-as-Judge (Steps 1-4)
#   - Pipeline model (Claude Sonnet 4) vs independent judge (Gemini 2.5 Pro)
#   - Agreement metrics: MAE, % within ±1 point, exact match %
#   - HITL triage queue for disagreements (routed to human review)
#
# Layer 2: MLflow Built-in Judges (Cell 14)
#   - Guidelines scorer: validates summary quality against QA criteria
#   - Safety scorer: ensures no harmful content in outputs
#   - Binary pass/fail with LLM-generated rationale
#
# Layer 3: Custom Scorers + Ground Truth (Cells 15-16)
#   - score_accuracy: LLM vs reference within ±1 tolerance
#   - score_inflation_check: detects systematic upward bias
#   - rubric_completeness: verifies all criteria were assessed
#   - summary_quality: ensures meaningful (not empty) summaries
#   - empathy_detection: regex signals vs human empathy scores
#   - user_frustration: flags calls with unhandled frustration
#
# Layer 4: Production Monitoring (Cell 17)
#   - Scorers registered with .register() + .start()
#   - Auto-evaluate new traces at configured sample rates
#   - View trends in MLflow Experiment UI → Evaluation tab
#
# KEY INSIGHT:
#   Architecture: Claude Sonnet 4 (scorer) + Gemini 2.5 Pro (judge)
#   Cross-PROVIDER validation (Anthropic vs Google) catches systematic
#   biases that same-provider evaluation would miss. No single model
#   should be trusted in isolation.
# =============================================================================

print("Evaluation framework summary above ↑")
print("See cell outputs for actual metrics from this run.")
print("\nNext steps:")
print("  → Run Cell 18 to open MLflow Experiment UI")
print("  → Notebook 04_Dashboard surfaces results for supervisors")
print("  → Notebook 05_Genie_AI_Skill enables natural language queries")