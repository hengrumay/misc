# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Contact Center — 05 Genie Space & AI Skill
# MAGIC
# MAGIC This notebook sets up:
# MAGIC 1. **Genie Space** — Natural language query interface over gold QA data for business analysts
# MAGIC 2. **AI Skill** — Reusable sentiment analysis and QA scoring function that can be applied to other use cases
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Notebooks 01-03 have been run successfully
# MAGIC - Gold table (`gold_qa_evaluations`) is populated with evaluated calls

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
dbutils.widgets.text("catalog", "chada_demos", "Unity Catalog")
dbutils.widgets.text("schema", "contact_center_qa", "Schema")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")

# COMMAND ----------

# DBTITLE 1,Register Reusable AI Skill: Sentiment Analyzer
# Register a reusable sentiment analysis function as an AI Skill
# This can be used by other teams/use cases beyond the contact center

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.ai_skill_sentiment_analysis")
spark.sql(f"""
CREATE FUNCTION {FQ}.ai_skill_sentiment_analysis(text STRING)
RETURNS STRING
COMMENT 'Reusable AI Skill: Analyzes sentiment of any text input. Returns JSON with sentiment label (Positive/Negative/Neutral/Mixed), confidence score (0.0-1.0), and key emotional indicators. Can be applied to customer reviews, support tickets, social media posts, chat transcripts, etc.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'Analyze the sentiment of the following text. Return ONLY a JSON object with these fields:\\n',
      '  "sentiment": one of "Positive", "Negative", "Neutral", or "Mixed"\\n',
      '  "confidence": float 0.0-1.0\\n',
      '  "emotional_indicators": array of key emotional words/phrases detected\\n',
      '  "summary": one sentence summary of the emotional tone\\n\\n',
      'Text to analyze:\\n', text
    )
  )
)
""")
print(f"AI Skill registered: {FQ}.ai_skill_sentiment_analysis")
print("This function can be called from any SQL context, notebook, or Genie Space.")

# COMMAND ----------

# DBTITLE 1,Register Reusable AI Skill: QA Scorer
# Register a reusable QA scoring function
# Scores any agent-customer interaction against the standard checklist

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.ai_skill_qa_scorer")
spark.sql(f"""
CREATE FUNCTION {FQ}.ai_skill_qa_scorer(transcript STRING)
RETURNS STRING
COMMENT 'Reusable AI Skill: Scores a customer service interaction against a standard QA checklist. Returns JSON with per-criterion scores (1-5) for greeting, empathy, accuracy, escalation, and compliance, plus an overall weighted score, compliance flags, and coaching recommendations. Works on any agent-customer transcript (calls, chats, emails).'
RETURN (
  SELECT {FQ}.assess_rubric_rag(transcript)
)
""")
print(f"AI Skill registered: {FQ}.ai_skill_qa_scorer")
print("This function wraps the full rubric assessment and can be reused across use cases.")

# COMMAND ----------

# DBTITLE 1,Create Genie Space (Programmatic)
# Create a Genie Space for business analysts to query QA data in natural language
# The Genie Space provides a no-code interface over the gold_qa_evaluations table

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Tables to include in the Genie Space
genie_tables = [
    f"{FQ}.gold_qa_evaluations",
    f"{FQ}.qa_rubric",
    f"{FQ}.silver_transcriptions",
]

print("\n" + "="*60)
print("GENIE SPACE SETUP")
print("="*60)
print(f"\nTo create the Genie Space manually:")
print(f"1. Go to the Genie tab in the Databricks sidebar")
print(f"2. Click 'New Space'")
print(f"3. Name: 'Contact Center QA Insights'")
print(f"4. Add these tables:")
for t in genie_tables:
    print(f"   - {t}")
print(f"\nSuggested instructions for the Genie Space:")
print(f"""---
This space contains post-call quality evaluation data for an enterprise contact center.

Key tables:
- gold_qa_evaluations: Full QA evaluation results with per-criterion scores (1-5), compliance flags, and coaching notes
- qa_rubric: The configurable QA checklist criteria and scoring descriptions  
- silver_transcriptions: Raw call transcriptions

Metrics:
- overall_qa_score: Weighted average of all criteria (1.0-5.0)
- Individual scores: greeting_score, empathy_score, accuracy_score, escalation_score, compliance_score (each 1-5)
- requires_human_review: Boolean flag for outlier calls

Dimensions:
- agent_id: Contact center agent identifier
- queue_type: Department (Sales, Support, Billing, Technical, Complaints)
- call_category: AI-classified call type
- sentiment: Customer sentiment (Positive/Negative/Neutral/Mixed)
---""")

# COMMAND ----------

# DBTITLE 1,Verify AI Skills
# Verify that the AI Skills are properly registered and accessible
funcs = spark.sql(f"SHOW USER FUNCTIONS IN {FQ} LIKE '*ai_skill*'").collect()

print("\nRegistered AI Skills:")
for f in funcs:
    print(f"  - {f[0]}")

print(f"\n{'='*60}")
print("USAGE EXAMPLES")
print(f"{'='*60}")
print(f"\n-- Sentiment analysis on any text:")
print(f"SELECT {FQ}.ai_skill_sentiment_analysis('The agent was very helpful and resolved my issue quickly!')")
print(f"\n-- QA scoring on any transcript:")
print(f"SELECT {FQ}.ai_skill_qa_scorer('Agent: Thank you for calling. My name is Sarah from billing...')")
print(f"\n-- Batch scoring on a table:")
print(f"SELECT *, {FQ}.ai_skill_sentiment_analysis(comment_text) AS sentiment")
print(f"FROM my_catalog.my_schema.customer_feedback")