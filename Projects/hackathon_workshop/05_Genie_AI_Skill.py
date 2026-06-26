# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 05 — Genie Space + AI Skill + Demo Guide
# MAGIC
# MAGIC **What this does:** Creates the natural language interface for business users and registers a reusable AI Skill.
# MAGIC
# MAGIC | Component | Who Uses It | Rubric Criterion |
# MAGIC |-----------|-------------|------------------|
# MAGIC | Genie Space | Business analysts, supervisors | Criterion 5 (UX) |
# MAGIC | AI Skill (UC Function) | Any team in the organization | Criterion 6 (Innovation) |
# MAGIC | Demo Guide | Your pod for Day 2 presentation | All criteria |
# MAGIC
# MAGIC ---
# MAGIC ### Rubric Targets
# MAGIC > *Criterion 5: "Polished UI; fluid natural-language querying returns clear, trustworthy answers"*  
# MAGIC > *Criterion 6: "Novel technique or reusable asset (e.g., a shared AI skill)"*
# MAGIC
# MAGIC ---
# MAGIC *Prerequisites: Run 00 → 01 → 02 → **03** in order. Notebook 03 must run first — it validates AI scoring quality and writes results to `eval_quality_metrics`. The cell below will block this notebook if the pipeline and judge differ by more than 1 point on average, preventing mis-calibrated scores from being exposed through Genie.*

# COMMAND ----------

# DBTITLE 1,Quality Gate: Validate Eval Metrics Before Proceeding
# Quality gate: block Genie Space setup if the scorer and judge are materially mis-calibrated.
# Architecture: Claude Sonnet 4 (scorer) + Gemini 2.5 Pro (judge)
# Genie would expose unvalidated scores directly to business analysts — higher trust bar.
# Run notebook 03 (03_LLM_Judge_Evals) first to populate eval_quality_metrics.
from pyspark.errors import AnalysisException

try:
    metrics = spark.sql("""
      SELECT pct_within_1_point, mean_absolute_error, quality_gate,
             avg_pipeline_score, avg_judge_score, evaluated_at
      FROM mmt_aws_usw2_catalog.contact_calls.eval_quality_metrics
      ORDER BY evaluated_at DESC
      LIMIT 1
    """)
    rows = metrics.collect()
except AnalysisException:
    rows = []

if not rows:
    raise Exception(
        "QUALITY GATE FAILED: No eval metrics found.\n"
        "Run notebook 03 (03_LLM_Judge_Evals) first to validate AI scoring quality.\n"
        "Genie must not expose unvalidated scores to business analysts."
    )

r = rows[0]
if r["quality_gate"] == "FAIL":
    inflation = r["avg_pipeline_score"] - r["avg_judge_score"]
    raise Exception(
        f"QUALITY GATE FAILED: Pipeline and judge differ by more than 1.0 point on average.\n"
        f"  Pipeline avg: {r['avg_pipeline_score']}, Judge avg: {r['avg_judge_score']}\n"
        f"  Calibration gap: {inflation:+.2f}\n"
        f"  Do not expose mis-calibrated scores through Genie. Re-run notebook 03 after investigating."
    )

print("Quality gate PASSED")
print(f"  Pipeline avg score : {r['avg_pipeline_score']}")
print(f"  Judge avg score    : {r['avg_judge_score']} (Gemini 2.5 Pro, cross-provider check)")
print(f"  Quality gate       : {r['quality_gate']}")
print(f"  Eval validated at  : {r['evaluated_at']}")
print()
print("Scores are validated. Proceeding to Genie Space setup.")

# COMMAND ----------

# DBTITLE 1,Register Reusable AI Skill: Sentiment Analyzer
# MAGIC %sql
# MAGIC -- This UC function can be reused by ANY team in the organization for their own use cases
# MAGIC -- (patient feedback, employee surveys, social media monitoring, etc.)
# MAGIC CREATE OR REPLACE FUNCTION mmt_aws_usw2_catalog.contact_calls.ai_skill_sentiment_analysis(text STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Reusable AI Skill: Analyzes sentiment of any text. Returns JSON with label, confidence, and emotional indicators. Portable across all use cases.'
# MAGIC RETURN (
# MAGIC   SELECT ai_query(
# MAGIC     'databricks-claude-sonnet-4-6',
# MAGIC     CONCAT(
# MAGIC       'Analyze sentiment. Return JSON: {"sentiment": "Positive"|"Negative"|"Neutral"|"Mixed", "confidence": 0.0-1.0, "summary": "one sentence"}\n\nText: ', text
# MAGIC     )
# MAGIC   )
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Test the AI Skill
# MAGIC %sql
# MAGIC -- Test: anyone in the org can now call this function
# MAGIC SELECT mmt_aws_usw2_catalog.contact_calls.ai_skill_sentiment_analysis(
# MAGIC   'The agent was incredibly helpful and resolved my billing issue in under 2 minutes. Best experience ever!'
# MAGIC ) AS sentiment_result

# COMMAND ----------

# DBTITLE 1,Create Your Genie Space
# MAGIC %md
# MAGIC ## Create Your Genie Space (PM Action!)
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. In the left sidebar, click **Genie**
# MAGIC 2. Click **New Space**
# MAGIC 3. Name: **"Contact Center QA Insights"**
# MAGIC 4. Add table: `mmt_aws_usw2_catalog.contact_calls.gold_scorecard`
# MAGIC 5. Add these **instructions** to the Genie Space:
# MAGIC
# MAGIC ```
# MAGIC This space contains post-call quality evaluation data for a contact center.
# MAGIC
# MAGIC Key metrics:
# MAGIC - overall_qa_score: Weighted average of all criteria (1.0-5.0)
# MAGIC - Individual scores (1-5): greeting_score, identity_verification_score, empathy_score, commitment_score, branding_score, compliance_score, resolution_score, further_assistance_score, closing_score, customer_service_score
# MAGIC - greeting_adherence / closing_adherence: Script similarity scores (0.0-1.0)
# MAGIC - requires_human_review: Boolean flag for outlier calls needing supervisor attention
# MAGIC
# MAGIC Dimensions:
# MAGIC - agent_name: Contact center agent
# MAGIC - queue: Department (Appointments, Billing, Nurse Advice, Referrals, Pharmacy, Insurance Verification, Medical Records)
# MAGIC - call_category: AI-classified call type (Billing Dispute, Appointment Scheduling, Clinical Triage, Complaint, General Inquiry, Insurance Question, Prescription Refill)
# MAGIC - disposition: AI-classified outcome (escalate_to_supervisor, routine, coaching_opportunity)
# MAGIC - protocol_adherence: Compliance classification (compliant, partially_compliant, non_compliant)
# MAGIC - sentiment: Customer sentiment (positive/negative/neutral/mixed)
# MAGIC - direction: inbound/outbound
# MAGIC - division: Hospital/clinic division
# MAGIC ```
# MAGIC
# MAGIC **Example questions to try:**
# MAGIC * "Who is the best performing agent?"
# MAGIC * "Which queue has the lowest empathy scores?"
# MAGIC * "Show me calls that need human review"
# MAGIC * "What's the average score by department?"
# MAGIC * "Which agents need coaching on greeting?"

# COMMAND ----------

# DBTITLE 1,Create Genie Space Programmatically (SDK)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Configuration
SPACE_TITLE = "Contact Center QA Insights"
TABLE = "mmt_aws_usw2_catalog.contact_calls.gold_scorecard"

# General instructions for the Genie Space (from markdown above)
INSTRUCTIONS = """
This space contains post-call quality evaluation data for a contact center.

Key metrics:
- overall_qa_score: Weighted average of all criteria (1.0-5.0)
- Individual scores (1-5): greeting_score, identity_verification_score, empathy_score, commitment_score, branding_score, compliance_score, resolution_score, further_assistance_score, closing_score, customer_service_score
- greeting_adherence / closing_adherence: Script similarity scores (0.0-1.0)
- requires_human_review: Boolean flag for outlier calls needing supervisor attention

Dimensions:
- agent_name: Contact center agent
- queue: Department (Appointments, Billing, Nurse Advice, Referrals, Pharmacy, Insurance Verification, Medical Records)
- call_category: AI-classified call type (Billing Dispute, Appointment Scheduling, Clinical Triage, Complaint, General Inquiry, Insurance Question, Prescription Refill)
- disposition: AI-classified outcome (escalate_to_supervisor, routine, coaching_opportunity)
- protocol_adherence: Compliance classification (compliant, partially_compliant, non_compliant)
- sentiment: Customer sentiment (positive/negative/neutral/mixed)
- direction: inbound/outbound
- division: Hospital/clinic division
"""

# Create the Genie Space via REST API (idempotent — reuses existing space if found)
import json as _json

# Check if a space with this title already exists
existing_spaces = w.api_client.do("GET", "/api/2.0/genie/spaces")
matching = [s for s in existing_spaces.get("spaces", []) if s["title"] == SPACE_TITLE]

if matching:
    space_id = matching[0]["space_id"]
    print(f"Genie Space already exists — reusing it.")
else:
    # Get an available SQL warehouse
    warehouses = [wh for wh in w.warehouses.list()]
    running = [wh for wh in warehouses if str(wh.state) == "State.RUNNING"]
    warehouse_id = running[0].id if running else warehouses[0].id

    serialized_space = _json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": TABLE}]
        }
    })

    space_payload = {
        "title": SPACE_TITLE,
        "description": "Natural language interface for contact center QA supervisors and analysts.",
        "serialized_space": serialized_space,
        "warehouse_id": warehouse_id,
    }

    result = w.api_client.do("POST", "/api/2.0/genie/spaces", body=space_payload)
    space_id = result["space_id"]
    print(f"Genie Space created!")

GENIE_SPACE_ID = space_id

print(f"  Title:    {SPACE_TITLE}")
print(f"  Space ID: {space_id}")
print(f"  URL:      https://{spark.conf.get('spark.databricks.workspaceUrl')}/genie/rooms/{space_id}")
print()
print("Adding instructions via REST API...")

# Update with instructions
update_resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body={"description": INSTRUCTIONS})

if update_resp is not None:
    print("Instructions added successfully.")
else:
    print("Note: Could not add instructions via API. Add manually in the Genie UI.")

print(f"\nGENIE_SPACE_ID = \"{space_id}\"")

# COMMAND ----------

# DBTITLE 1,Genie Space: Created Programmatically
# Genie Space created via API — uses GENIE_SPACE_ID from cell above
GENIE_SPACE_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/genie/rooms/{GENIE_SPACE_ID}"

print(f"Genie Space: Contact Center QA Insights")
print(f"Space ID:    {GENIE_SPACE_ID}")
print(f"URL:         {GENIE_SPACE_URL}")
print(f"Table:       mmt_aws_usw2_catalog.contact_calls.gold_scorecard")
print()
print("Next steps:")
print("  1. Open the Genie Space and add the General Instructions from the markdown above")
print("  2. Add sample questions to guide users")
print("  3. Test with: 'Who is the best performing agent?'")

# COMMAND ----------

# DBTITLE 1,Test Genie Space: UI
# MAGIC %md
# MAGIC ## Test Genie Space: UI Walkthrough
# MAGIC
# MAGIC **Open the space:** [Contact Center QA Insights](https://fevm-mmt-aws-usw2.cloud.databricks.com/genie/rooms/01f16edf130d17cdb84846cfc6fa735f)
# MAGIC
# MAGIC ### Demo Script (60 seconds)
# MAGIC
# MAGIC 1. **Open Genie** — Click the link above or navigate via the left sidebar → Genie
# MAGIC 2. **Ask a simple question:**
# MAGIC    > "Who is the best performing agent?"
# MAGIC 3. **Show the generated SQL** — click the SQL tab to prove transparency
# MAGIC 4. **Ask a follow-up** (same conversation):
# MAGIC    > "Break that down by queue"
# MAGIC 5. **Ask a coaching question:**
# MAGIC    > "Which agents need coaching on empathy?"
# MAGIC 6. **Show a filter question:**
# MAGIC    > "Show me calls that need human review in Billing"
# MAGIC
# MAGIC ### What to Highlight for Judges
# MAGIC
# MAGIC | Feature | What to Say |
# MAGIC |---------|-------------|
# MAGIC | Natural language | "No SQL required — supervisors ask in plain English" |
# MAGIC | Generated SQL visible | "Full transparency — users can verify what Genie did" |
# MAGIC | Follow-up context | "Genie remembers context within a conversation" |
# MAGIC | Table grounding | "Answers come only from validated gold_scorecard data" |
# MAGIC | Quality gate | "Scores are only exposed after passing 80% agreement validation" |
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC * **"I don't have enough information"** → Check that General Instructions were added to the space
# MAGIC * **Wrong column referenced** → Verify table is `mmt_aws_usw2_catalog.contact_calls.gold_scorecard`
# MAGIC * **Warehouse error** → Ensure the SQL warehouse is running (Serverless Starter Warehouse)

# COMMAND ----------

# DBTITLE 1,Test Genie Space: Natural Language Query
import time

# Test the Genie Space with a natural language question
question = "Which agents need coaching on empathy?"

result = w.api_client.do("POST", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation", body={
    "content": question
})

conversation_id = result["conversation_id"]
message_id = result["message_id"]

# Poll for completion
for _ in range(30):
    msg = w.api_client.do("GET", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}")
    status = msg.get("status")
    if status in ("COMPLETED", "FAILED"):
        break
    time.sleep(2)

print(f"Question: {question}")
print(f"Status:   {status}")
print()

if "attachments" in msg:
    for att in msg["attachments"]:
        if "query" in att:
            print(f"Generated SQL:\n{att['query'].get('query', 'N/A')}\n")
        if "text" in att:
            print(f"Answer:\n{att['text'].get('content', '')}")

# COMMAND ----------

# DBTITLE 1,Presentation Guide
# MAGIC %md
# MAGIC ## Presentation Guide (5-7 minutes)
# MAGIC
# MAGIC ### Narrative Arc:
# MAGIC
# MAGIC **1. The Problem (30 sec)**
# MAGIC > "Our contact center handles X calls/day. QA supervisors manually review calls — it takes 45 min each. They can only review 2–3% of calls. Quality issues go undetected."
# MAGIC
# MAGIC **2. What We Built (90 sec) — Live Demo**
# MAGIC > Run notebook 02 live. Show: raw transcript → AI scores in seconds.
# MAGIC > *"In 2 seconds, we get the same quality evaluation that takes a human 45 minutes."*
# MAGIC
# MAGIC **3. We Don't Blindly Trust AI (60 sec)**
# MAGIC > Show notebook 03 results. 
# MAGIC > *"An independent LLM judge validates every score. Disagreements go to human review."*
# MAGIC > *"X% agreement within 1 point — matching human inter-rater reliability."*
# MAGIC
# MAGIC **4. Supervisors Use This (60 sec) — Live Demo**
# MAGIC > Open the dashboard. Show agent rankings, outlier calls, coaching recommendations.
# MAGIC > *"Supervisors see actionable insights without writing any code."*
# MAGIC
# MAGIC **5. Anyone Can Ask Questions (60 sec) — Live Demo**
# MAGIC > Open Genie Space. Ask: "Which agents need coaching on empathy?"
# MAGIC > *"Business analysts explore data in plain English. No SQL required."*
# MAGIC
# MAGIC **6. Reusable + Scalable (30 sec)**
# MAGIC > *"The sentiment AI Skill is a reusable function — patient feedback team can use it tomorrow."*
# MAGIC > *"Low-confidence calls route to a HITL queue — next step is a Databricks App for supervisor review."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Rubric Score Targets:
# MAGIC | Criterion | Target | Evidence |
# MAGIC |---|---|---|
# MAGIC | 1. E2E Functionality (20%) | 5 | Live demo runs with no hand-holding |
# MAGIC | 2. Business Impact (20%) | 5 | "98% reduction in QA review time" + clear production path |
# MAGIC | 3. AI Quality (20%) | 4-5 | Agreement metrics + edge case handling |
# MAGIC | 4. Safety/Trust (15%) | 5 | LLM judge + HITL triage + confidence scoring |
# MAGIC | 5. UX (15%) | 5 | Dashboard + Genie Space (NL querying) |
# MAGIC | 6. Innovation (10%) | 5 | Reusable AI Skill UC function |

# COMMAND ----------

# DBTITLE 1,HITL Expansion Architecture
# MAGIC %md
# MAGIC ## Expansion: Human-in-the-Loop on Databricks
# MAGIC
# MAGIC **What we built today (Day 1):**
# MAGIC ```
# MAGIC Genesys → Stitch → AI Score → Judge Validate → Dashboard + Genie
# MAGIC                                      │
# MAGIC                               ┌──────┴────────┐
# MAGIC                               │ Disagreement? │
# MAGIC                               └──────┬────────┘
# MAGIC                                      │
# MAGIC                               HITL Triage Queue ← (table, done!)
# MAGIC ```
# MAGIC
# MAGIC **Production expansion (Week 2+):**
# MAGIC ```
# MAGIC                               HITL Triage Queue
# MAGIC                                      │
# MAGIC                         ┌────────────┴───────────────┐
# MAGIC                         │ Databricks App (HITL UI)   │
# MAGIC                         │ Supervisor reviews call    │
# MAGIC                         │ Approves / Corrects score  │
# MAGIC                         └─────────────┬──────────────┘
# MAGIC                                       │
# MAGIC                         ┌─────────────┴─────────────┐
# MAGIC                         │ Corrections → Golden Set  │
# MAGIC                         │ Recalibrate judge prompt  │
# MAGIC                         │ Track drift over time     │
# MAGIC                         └───────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Databricks features for HITL at scale:**
# MAGIC * **Databricks Apps** — Custom UI for supervisor review workflow
# MAGIC * **Delta tables** — Audit trail of all human corrections
# MAGIC * **Lakeflow Jobs** — Scheduled recalibration of judge prompts
# MAGIC * **Alerts** — Notify when agreement drops below threshold
# MAGIC * **MLflow** — Track judge prompt versions and accuracy over time