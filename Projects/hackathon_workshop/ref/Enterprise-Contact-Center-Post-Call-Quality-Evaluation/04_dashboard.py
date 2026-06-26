# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Contact Center — 04 QA Scoring Dashboard
# MAGIC
# MAGIC Creates a supervisor-facing dashboard that:
# MAGIC - Shows overall QA score distributions and trends
# MAGIC - Ranks agents by performance with drill-down
# MAGIC - Flags outlier calls requiring human review
# MAGIC - Identifies coaching opportunities by criterion
# MAGIC - Breaks down performance by queue type

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - same as other notebooks
dbutils.widgets.text("catalog", "chada_demos", "Unity Catalog")
dbutils.widgets.text("schema", "contact_center_qa", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# DBTITLE 1,KPIs: Overall QA Summary
# MAGIC %sql
# MAGIC -- Overall QA metrics summary
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_calls_evaluated,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_qa_score,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_for_review,
# MAGIC   ROUND(COUNT(CASE WHEN requires_human_review THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_flagged,
# MAGIC   ROUND(AVG(greeting_score), 2) AS avg_greeting,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   ROUND(AVG(accuracy_score), 2) AS avg_accuracy,
# MAGIC   ROUND(AVG(escalation_score), 2) AS avg_escalation,
# MAGIC   ROUND(AVG(compliance_score), 2) AS avg_compliance
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations

# COMMAND ----------

# DBTITLE 1,Agent Performance Rankings
# MAGIC %sql
# MAGIC -- Agent performance leaderboard
# MAGIC SELECT 
# MAGIC   agent_id,
# MAGIC   COUNT(*) AS calls_evaluated,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_score,
# MAGIC   MIN(overall_qa_score) AS min_score,
# MAGIC   MAX(overall_qa_score) AS max_score,
# MAGIC   ROUND(AVG(greeting_score), 2) AS avg_greeting,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   ROUND(AVG(accuracy_score), 2) AS avg_accuracy,
# MAGIC   ROUND(AVG(escalation_score), 2) AS avg_escalation,
# MAGIC   ROUND(AVG(compliance_score), 2) AS avg_compliance,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_calls
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC GROUP BY agent_id
# MAGIC ORDER BY avg_score DESC

# COMMAND ----------

# DBTITLE 1,Calls Requiring Human Review (Outliers)
# MAGIC %sql
# MAGIC -- Outlier calls flagged for human review
# MAGIC SELECT 
# MAGIC   call_id,
# MAGIC   agent_id,
# MAGIC   queue_type,
# MAGIC   overall_qa_score,
# MAGIC   greeting_score,
# MAGIC   empathy_score,
# MAGIC   accuracy_score,
# MAGIC   escalation_score,
# MAGIC   compliance_score,
# MAGIC   compliance_flags,
# MAGIC   coaching_notes,
# MAGIC   evaluated_at
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC WHERE requires_human_review = true
# MAGIC ORDER BY overall_qa_score ASC, evaluated_at DESC

# COMMAND ----------

# DBTITLE 1,Performance by Queue Type
# MAGIC %sql
# MAGIC -- QA scores broken down by queue/department
# MAGIC SELECT 
# MAGIC   queue_type,
# MAGIC   COUNT(*) AS total_calls,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_score,
# MAGIC   ROUND(AVG(greeting_score), 2) AS avg_greeting,
# MAGIC   ROUND(AVG(empathy_score), 2) AS avg_empathy,
# MAGIC   ROUND(AVG(accuracy_score), 2) AS avg_accuracy,
# MAGIC   ROUND(AVG(escalation_score), 2) AS avg_escalation,
# MAGIC   ROUND(AVG(compliance_score), 2) AS avg_compliance,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_calls,
# MAGIC   ROUND(COUNT(CASE WHEN overall_qa_score >= 4 THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_excellent
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC GROUP BY queue_type
# MAGIC ORDER BY avg_score DESC

# COMMAND ----------

# DBTITLE 1,Coaching Opportunities: Lowest Criterion Scores
# MAGIC %sql
# MAGIC -- Identify agents needing coaching by specific criterion
# MAGIC -- Shows agents whose average on any criterion falls below 3.0
# MAGIC WITH agent_criterion_scores AS (
# MAGIC   SELECT 
# MAGIC     agent_id,
# MAGIC     'Greeting & ID' AS criterion, AVG(greeting_score) AS avg_score FROM ${catalog}.${schema}.gold_qa_evaluations GROUP BY agent_id
# MAGIC   UNION ALL
# MAGIC   SELECT agent_id, 'Empathy', AVG(empathy_score) FROM ${catalog}.${schema}.gold_qa_evaluations GROUP BY agent_id
# MAGIC   UNION ALL
# MAGIC   SELECT agent_id, 'Accuracy', AVG(accuracy_score) FROM ${catalog}.${schema}.gold_qa_evaluations GROUP BY agent_id
# MAGIC   UNION ALL
# MAGIC   SELECT agent_id, 'Escalation', AVG(escalation_score) FROM ${catalog}.${schema}.gold_qa_evaluations GROUP BY agent_id
# MAGIC   UNION ALL
# MAGIC   SELECT agent_id, 'Compliance', AVG(compliance_score) FROM ${catalog}.${schema}.gold_qa_evaluations GROUP BY agent_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   agent_id,
# MAGIC   criterion,
# MAGIC   ROUND(avg_score, 2) AS avg_criterion_score,
# MAGIC   CASE 
# MAGIC     WHEN avg_score < 2.0 THEN 'URGENT - Immediate coaching required'
# MAGIC     WHEN avg_score < 3.0 THEN 'WARNING - Coaching recommended'
# MAGIC     WHEN avg_score < 4.0 THEN 'MONITOR - Room for improvement'
# MAGIC     ELSE 'GOOD - Meeting expectations'
# MAGIC   END AS coaching_priority
# MAGIC FROM agent_criterion_scores
# MAGIC WHERE avg_score < 3.5
# MAGIC ORDER BY avg_score ASC

# COMMAND ----------

# DBTITLE 1,Score Distribution & Trends
# MAGIC %sql
# MAGIC -- Score distribution over time (daily trend)
# MAGIC SELECT 
# MAGIC   DATE(evaluated_at) AS eval_date,
# MAGIC   COUNT(*) AS calls_evaluated,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_score,
# MAGIC   COUNT(CASE WHEN overall_qa_score >= 4 THEN 1 END) AS excellent_calls,
# MAGIC   COUNT(CASE WHEN overall_qa_score < 3 THEN 1 END) AS poor_calls,
# MAGIC   COUNT(CASE WHEN requires_human_review THEN 1 END) AS flagged_calls
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC GROUP BY DATE(evaluated_at)
# MAGIC ORDER BY eval_date DESC

# COMMAND ----------

# DBTITLE 1,Sentiment Distribution by Category
# MAGIC %sql
# MAGIC -- Customer sentiment broken down by call category
# MAGIC SELECT 
# MAGIC   call_category,
# MAGIC   sentiment,
# MAGIC   COUNT(*) AS call_count,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_qa_score
# MAGIC FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC GROUP BY call_category, sentiment
# MAGIC ORDER BY call_category, sentiment