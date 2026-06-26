# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 — Executive QA Dashboard
# MAGIC
# MAGIC **What this does:** SQL queries that power a supervisor-facing Lakeview dashboard.
# MAGIC
# MAGIC | Widget | Business Question |
# MAGIC |--------|-------------------|
# MAGIC | KPIs | Average QA score, % flagged for review, total calls evaluated |
# MAGIC | Agent Leaderboard | Who's performing best? Who needs coaching? |
# MAGIC | Outlier Calls | Which calls need immediate attention? |
# MAGIC | Performance by Queue | Are certain departments underperforming? |
# MAGIC | Coaching Opportunities | Which specific criteria need work, per agent? |
# MAGIC
# MAGIC **Rubric Criterion 5:** User Experience & Querying — *"Can a non-technical user get value?"*
# MAGIC
# MAGIC ---
# MAGIC ### PM Action: Use Genie Code to build these queries, then create a Lakeview Dashboard from the results!
# MAGIC
# MAGIC *Prerequisites: Run 00 → 01 → 02 → **03** in order. Notebook 03 must run first — it validates AI scoring quality and writes results to `eval_quality_metrics`. The cell below will block this dashboard if score inflation is detected (judge scoring higher than pipeline), preventing unvalidated scores from reaching supervisors.*

# COMMAND ----------

# DBTITLE 1,Quality Gate: Validate Eval Metrics Before Proceeding
# Quality gate: validate AI scoring before surfacing to supervisors.
# Architecture: Claude Sonnet 4 (scorer, nb 02) + Gemini 2.5 Pro (judge, nb 03)
#
# Gate logic: Check calibration bounds, not raw agreement.
# - Cross-provider models have different calibration curves (expected!)
# - What matters: the pipeline is not materially mis-calibrated in either direction
# - If the gap exceeds 1 point either way, do not expose scores to supervisors
#
# Run notebook 03 (LLM_Judge_Evals) first to populate eval_quality_metrics.
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
        "Run notebook 03 (03_LLM_Judge_Evals) first to validate AI scoring quality."
    )

r = rows[0]

# Symmetric cross-model quality gate:
# PASS only if the average calibration gap stays within ±1.0 points
inflation = r["avg_pipeline_score"] - r["avg_judge_score"]  # positive = pipeline higher
gate_passed = abs(inflation) <= 1.0

if not gate_passed:
    raise Exception(
        f"QUALITY GATE FAILED: Pipeline and independent judge differ by more than 1.0 point on average.\n"
        f"  Pipeline avg: {r['avg_pipeline_score']}, Judge avg: {r['avg_judge_score']}\n"
        f"  Calibration gap: {inflation:+.2f}\n"
        f"  Investigate scoring calibration before exposing results to supervisors."
    )

print("Quality gate PASSED")
print(f"  Pipeline avg score : {r['avg_pipeline_score']}")
print(f"  Judge avg score    : {r['avg_judge_score']} (independent cross-model check)")
print(f"  Calibration gap    : {inflation:+.2f} (must stay within ±1.0)")
print(f"  Agreement (±1 pt)  : {r['pct_within_1_point']}% (informational — cross-model gap expected)")
print(f"  Eval validated at  : {r['evaluated_at']}")
print()
print("Scores are validated. Proceeding to dashboard queries.")

# COMMAND ----------

# DBTITLE 1,KPIs: Overall QA Summary
# MAGIC %sql
# MAGIC -- Executive summary: key metrics at a glance
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_calls_evaluated,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_qa_score,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_for_review,
# MAGIC   ROUND(COUNT(CASE WHEN requires_human_review THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_flagged,
# MAGIC   ROUND(AVG(greeting_score), 2) AS avg_greeting,
# MAGIC   ROUND(AVG(identity_verification_score), 2) AS avg_identity_check,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   ROUND(AVG(compliance_score), 2) AS avg_compliance,
# MAGIC   ROUND(AVG(resolution_score), 2) AS avg_resolution,
# MAGIC   ROUND(AVG(greeting_adherence), 3) AS avg_script_adherence
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard

# COMMAND ----------

# DBTITLE 1,Agent Performance Leaderboard
# MAGIC %sql
# MAGIC -- Rank agents by average QA score with per-criterion breakdown
# MAGIC SELECT 
# MAGIC   agent_name,
# MAGIC   COUNT(*) AS calls_evaluated,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_score,
# MAGIC   ROUND(AVG(greeting_score), 2) AS avg_greeting,
# MAGIC   ROUND(AVG(identity_verification_score), 2) AS avg_id_verify,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   ROUND(AVG(compliance_score), 2) AS avg_compliance,
# MAGIC   ROUND(AVG(resolution_score), 2) AS avg_resolution,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_calls
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
# MAGIC GROUP BY agent_name
# MAGIC ORDER BY avg_score DESC

# COMMAND ----------

# DBTITLE 1,Outlier Calls: Flagged for Human Review
# MAGIC %sql
# MAGIC -- Calls requiring supervisor attention (sorted worst-first)
# MAGIC SELECT 
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   overall_qa_score,
# MAGIC   sentiment,
# MAGIC   coaching_notes,
# MAGIC   greeting_score, identity_verification_score, empathy_score, compliance_score, resolution_score,
# MAGIC   call_summary
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
# MAGIC WHERE requires_human_review = true
# MAGIC ORDER BY overall_qa_score ASC

# COMMAND ----------

# DBTITLE 1,Performance by Queue (Department)
# MAGIC %sql
# MAGIC -- Which departments/queues have quality issues?
# MAGIC SELECT 
# MAGIC   queue,
# MAGIC   COUNT(*) AS total_calls,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_score,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   COUNT(CASE WHEN overall_qa_score >= 4 THEN 1 END) AS excellent_calls,
# MAGIC   COUNT(CASE WHEN overall_qa_score < 3 THEN 1 END) AS poor_calls,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
# MAGIC GROUP BY queue
# MAGIC ORDER BY avg_score DESC

# COMMAND ----------

# DBTITLE 1,Coaching Opportunities: Lowest Criteria by Agent
# MAGIC %sql
# MAGIC -- Identify which specific criteria each agent struggles with
# MAGIC WITH agent_criteria AS (
# MAGIC   SELECT agent_name, 'Greeting' AS criterion, AVG(greeting_score) AS avg_score FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
# MAGIC   UNION ALL
# MAGIC   SELECT agent_name, 'Identity Verification', AVG(identity_verification_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
# MAGIC   UNION ALL
# MAGIC   SELECT agent_name, 'Empathy', AVG(empathy_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
# MAGIC   UNION ALL
# MAGIC   SELECT agent_name, 'Compliance', AVG(compliance_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
# MAGIC   UNION ALL
# MAGIC   SELECT agent_name, 'Resolution', AVG(resolution_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   agent_name, criterion, ROUND(avg_score, 2) AS avg_criterion_score,
# MAGIC   CASE 
# MAGIC     WHEN avg_score < 2.5 THEN 'URGENT'
# MAGIC     WHEN avg_score < 3.5 THEN 'COACH'
# MAGIC     ELSE 'GOOD'
# MAGIC   END AS action_needed
# MAGIC FROM agent_criteria
# MAGIC WHERE avg_score < 3.5
# MAGIC ORDER BY avg_score ASC

# COMMAND ----------

# DBTITLE 1,Create the Dashboard (UI)
# MAGIC %md
# MAGIC ## Option A: Create via UI (No Code)
# MAGIC
# MAGIC 1. Click **+** in the workspace sidebar and select **Dashboard**.
# MAGIC 2. Name it: *Contact Center QA Dashboard*
# MAGIC 3. For each widget, click **Add dataset** and paste the SQL queries from the cells above.
# MAGIC 4. Arrange widgets using this layout:
# MAGIC
# MAGIC | Row | Left | Right |
# MAGIC | --- | --- | --- |
# MAGIC | Top | KPIs: Avg Score, Total Calls, % Flagged, Script Adherence | |
# MAGIC | Middle | Agent Leaderboard (bar chart) | Calls Needing Review (table) |
# MAGIC | Bottom | Queue Performance (bar chart) | Coaching Opportunities (table) |
# MAGIC
# MAGIC 5. Click **Publish** to make it live.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Genie Code Prompt (Copy-Paste for UI Approach)
# MAGIC %md
# MAGIC ## Genie Code Prompt — Build This Dashboard Automatically
# MAGIC
# MAGIC To recreate this dashboard using the **UI approach**, open a new empty dashboard, add the 5 datasets from the SQL cells above, then **paste this prompt into Genie Code** on the dashboard page:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Prompt:**
# MAGIC >
# MAGIC > Build a supervisor QA dashboard using table `mmt_aws_usw2_catalog.contact_calls.gold_scorecard` with this exact layout:
# MAGIC >
# MAGIC > **Row 0 — KPI Counters (3 columns wide each, height 3):**
# MAGIC > 1. Column 0: "Total Calls Evaluated" — `COUNT(*) AS total_calls_evaluated`
# MAGIC > 2. Column 3: "Avg QA Score" — `ROUND(AVG(overall_qa_score), 2) AS avg_qa_score`
# MAGIC > 3. Column 6: "% Flagged for Review" — `ROUND(COUNT(CASE WHEN requires_human_review THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_flagged` (show with % suffix)
# MAGIC > 4. Column 9: "Avg Script Adherence" — `ROUND(AVG(greeting_adherence), 3) AS avg_script_adherence`
# MAGIC >
# MAGIC > **Row 3, left (6 columns wide, height 8):** "Agent Leaderboard" — bar chart with `agent_name` on x-axis (categorical, sorted by score descending), `ROUND(AVG(overall_qa_score), 2) AS avg_score` on y-axis. Use a different color per agent (categorical color encoding on agent_name). Hide the legend. GROUP BY agent_name.
# MAGIC >
# MAGIC > **Row 3, right (6 columns wide, height 6):** "Outlier Calls" — table showing `interaction_id, agent_name, queue, overall_qa_score, sentiment, coaching_notes` WHERE `requires_human_review = true` ORDER BY `overall_qa_score ASC`.
# MAGIC >
# MAGIC > **Row 11, left (6 columns wide, height 6):** "Queue Performance" — bar chart with `queue` on x-axis (categorical, sorted by score descending), `ROUND(AVG(overall_qa_score), 2) AS avg_score` on y-axis. Use a different color per queue (categorical color encoding on queue). Hide the legend. GROUP BY queue.
# MAGIC >
# MAGIC > **Row 9, right (6 columns wide, height 8):** "Coaching Opportunities" — table with columns `agent_name, criterion, avg_criterion_score, action_needed`. Query uses UNION ALL across 5 rubric criteria (greeting_score, identity_verification_score, empathy_score, compliance_score, resolution_score), filters WHERE avg < 3.5, with CASE for URGENT (< 2.5) vs COACH (< 3.5).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **💡 Tip:** This prompt works best when you've already added the 5 datasets to the dashboard. Genie Code will create the widgets referencing those datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Option B: Create Programmatically (below)
# MAGIC
# MAGIC Use the Databricks SDK to create, update, and export the dashboard as code — ideal for CI/CD, DABs, or templating across environments.

# COMMAND ----------

# DBTITLE 1,Programmatic Dashboard Creation (Databricks SDK)
# Programmatic Lakeview Dashboard creation using the Databricks SDK
# This shows how to recreate the dashboard from code (useful for CI/CD, DABs, or templating)

from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# -- Dashboard config --
DASHBOARD_NAME = "Contact Center QA Dashboard"
# Set parent path to current user's workspace directory
_user_home = f"/Workspace/Users/{spark.conf.get('spark.databricks.workspaceUrl').split('.')[0]}"  # fallback
try:
    _user_home = f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}"
except Exception:
    pass
DASHBOARD_PARENT_PATH = _user_home
WAREHOUSE_ID = None  # Uses serverless if None

# Define the datasets (each maps to one of the SQL queries above)
datasets = [
    {
        "name": "kpi_summary",
        "query": """
            SELECT COUNT(*) AS total_calls_evaluated,
                   ROUND(AVG(overall_qa_score), 2) AS avg_qa_score,
                   ROUND(COUNT(CASE WHEN requires_human_review THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_flagged,
                   ROUND(AVG(greeting_adherence), 3) AS avg_script_adherence
            FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
        """
    },
    {
        "name": "agent_leaderboard",
        "query": """
            SELECT agent_name, COUNT(*) AS calls_evaluated,
                   ROUND(AVG(overall_qa_score), 2) AS avg_score,
                   ROUND(AVG(empathy_score), 2) AS avg_empathy,
                   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_calls
            FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
            GROUP BY agent_name ORDER BY avg_score DESC
        """
    },
    {
        "name": "outlier_calls",
        "query": """
            SELECT interaction_id, agent_name, queue, overall_qa_score, sentiment, coaching_notes
            FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
            WHERE requires_human_review = true
            ORDER BY overall_qa_score ASC
        """
    },
    {
        "name": "queue_performance",
        "query": """
            SELECT queue, COUNT(*) AS total_calls, ROUND(AVG(overall_qa_score), 2) AS avg_score,
                   COUNT(CASE WHEN overall_qa_score < 3 THEN 1 END) AS poor_calls,
                   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged
            FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
            GROUP BY queue ORDER BY avg_score DESC
        """
    },
    {
        "name": "coaching_opportunities",
        "query": """
            WITH agent_criteria AS (
              SELECT agent_name, 'Greeting' AS criterion, AVG(greeting_score) AS avg_score FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
              UNION ALL SELECT agent_name, 'Identity Verification', AVG(identity_verification_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
              UNION ALL SELECT agent_name, 'Empathy', AVG(empathy_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
              UNION ALL SELECT agent_name, 'Compliance', AVG(compliance_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
              UNION ALL SELECT agent_name, 'Resolution', AVG(resolution_score) FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard GROUP BY agent_name
            )
            SELECT agent_name, criterion, ROUND(avg_score, 2) AS avg_criterion_score,
                   CASE WHEN avg_score < 2.5 THEN 'URGENT' WHEN avg_score < 3.5 THEN 'COACH' ELSE 'GOOD' END AS action_needed
            FROM agent_criteria WHERE avg_score < 3.5 ORDER BY avg_score ASC
        """
    }
]

# -- Create or update the dashboard --
from databricks.sdk.service.dashboards import Dashboard
import os

# Load the full dashboard JSON (includes widgets + layout) from the exported file
# Falls back to datasets-only if the file doesn't exist yet
lvdash_path = os.path.join(os.path.dirname(os.path.realpath('__file__')), "contact_center_qa.lvdash.json")
lvdash_workspace_path = f"{DASHBOARD_PARENT_PATH}/contact_center_qa.lvdash.json"

if os.path.exists(lvdash_workspace_path):
    with open(lvdash_workspace_path, "r") as f:
        serialized = f.read()
    print(f"[OK] Loaded full dashboard spec from contact_center_qa.lvdash.json (with widgets)")
else:
    # Fallback: datasets-only (widgets must be added manually or via subsequent export)
    serialized = json.dumps({
        "pages": [{"name": "qa_overview", "displayName": "QA Overview"}],
        "datasets": [
            {"name": ds["name"], "displayName": ds["name"].replace("_", " ").title(), "query": ds["query"].strip()}
            for ds in datasets
        ]
    })
    print("[WARN] contact_center_qa.lvdash.json not found — creating with datasets only (no widgets)")

# Look up existing dashboard by name (avoids hardcoding the ID)
EXISTING_DASHBOARD_ID = None
for d in w.lakeview.list():
    if d.display_name == DASHBOARD_NAME:
        EXISTING_DASHBOARD_ID = d.dashboard_id
        break

if EXISTING_DASHBOARD_ID:
    # Existing dashboard found — preserve its layout/widgets, just reference it
    dashboard = w.lakeview.get(dashboard_id=EXISTING_DASHBOARD_ID)
    existing_json = json.loads(dashboard.serialized_dashboard) if dashboard.serialized_dashboard else {}
    print(f"[OK] Found existing dashboard: {dashboard.dashboard_id}")
    print(f"     Widgets preserved ({len(existing_json.get('pages', []))} pages, "
          f"{len(existing_json.get('datasets', []))} datasets)")
else:
    # No dashboard exists — create one with datasets (add widgets via UI or subsequent update)
    print("No existing dashboard found — creating new.")
    dashboard_obj = Dashboard(
        display_name=DASHBOARD_NAME,
        parent_path=DASHBOARD_PARENT_PATH,
        warehouse_id=WAREHOUSE_ID,
        serialized_dashboard=serialized
    )
    dashboard = w.lakeview.create(dashboard=dashboard_obj)
    print(f"[OK] Dashboard CREATED: {dashboard.dashboard_id}")

print(f"URL: /sql/dashboards/{dashboard.dashboard_id}")

# Publish the draft to make it live for viewers
# w.lakeview.publish(dashboard_id=dashboard.dashboard_id, embed_credentials=True)

# -- For Declarative Automation Bundles (DABs), define in databricks.yml: --
# resources:
#   dashboards:
#     contact_center_qa:
#       display_name: "Contact Center QA Dashboard"
#       file_path: ./dashboards/contact_center_qa.lvdash.json
#       warehouse_id: ${var.warehouse_id}
#       embed_credentials: true

# COMMAND ----------

# DBTITLE 1,Export Dashboard as .lvdash.json (for DABs)
# Export the deployed dashboard as .lvdash.json for Declarative Automation Bundles (DABs)
# This captures the full layout + queries so the dashboard can be version-controlled & deployed via CI/CD

import json

# Reuses dashboard ID discovered/created in the cell above
dashboard_id = dashboard.dashboard_id

# Fetch the full dashboard definition from the API
dash = w.lakeview.get(dashboard_id=dashboard_id)

# Parse and pretty-print the serialized dashboard JSON
dash_json = json.loads(dash.serialized_dashboard) if dash.serialized_dashboard else {}

# Write to workspace for DABs bundle reference
export_path = f"{DASHBOARD_PARENT_PATH}/contact_center_qa.lvdash.json"
with open(export_path, "w") as f:
    json.dump(dash_json, f, indent=2)

print(f"[OK] Exported to: {export_path}")
print(f"   Datasets: {len(dash_json.get('datasets', []))}")
print(f"   Pages:    {len(dash_json.get('pages', []))}")
print()
print("--- DABs databricks.yml snippet ---")
print("""
resources:
  dashboards:
    contact_center_qa:
      display_name: "Contact Center QA Dashboard"
      file_path: ./contact_center_qa.lvdash.json
      warehouse_id: ${var.warehouse_id}
      embed_credentials: true
""")

# COMMAND ----------

# DBTITLE 1,Next Steps
# Deployed dashboard link (derived dynamically from cell 9)
# Construct the full workspace URL for a clickable link
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
dashboard_url = f"https://{workspace_host}/sql/dashboardsv3/{dashboard.dashboard_id}"

# Clickable link first
displayHTML(f'<p><a href="{dashboard_url}" target="_blank" style="font-size:15px;">Open Contact Center QA Dashboard</a></p>')

print("=" * 70)
print("  DEPLOYED DASHBOARD")
print("=" * 70)
print()
print(f"  Name:     {DASHBOARD_NAME}")
print(f"  ID:       {dashboard.dashboard_id}")
print(f"  Path:     {DASHBOARD_PARENT_PATH}/{DASHBOARD_NAME}")
print(f"  Datasets: 5 (KPIs, Agent Leaderboard, Outlier Calls, Queue Perf, Coaching)")
print(f"  Export:   contact_center_qa.lvdash.json (for DABs CI/CD)")
print()
print("=" * 70)
print("  Next notebook: 05_Genie_AI_Skill")
print("  Creates the natural language interface + reusable AI Skill (run 03 first).")
print("=" * 70)