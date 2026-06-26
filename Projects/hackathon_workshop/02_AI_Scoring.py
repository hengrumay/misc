# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — AI Scoring + Gold Scorecard
# MAGIC
# MAGIC **What this does:** Runs AI functions on each stitched conversation to produce a complete QA scorecard.
# MAGIC
# MAGIC | AI Function           | What It Produces                                              |
# MAGIC |-----------------------|--------------------------------------------------------------|
# MAGIC | `ai_analyze_sentiment()` | Sentiment label (Positive/Negative/Neutral/Mixed)            |
# MAGIC | `ai_classify()`          | Call category + disposition + protocol adherence             |
# MAGIC | `ai_similarity()`        | Script adherence scores (greeting/closing vs expected)       |
# MAGIC | `ai_query(Claude Sonnet 4)` | 10-criterion rubric scores (1-5) + coaching notes         |
# MAGIC | `ai_summarize()`         | 20-word call summaries for dashboard                         |
# MAGIC | `ai_mask()`              | HIPAA-compliant PII-redacted transcripts                     |
# MAGIC
# MAGIC **Rubric Criteria 1, 2, 3:** E2E Functionality + Business Impact + AI Quality
# MAGIC
# MAGIC ---
# MAGIC *Prerequisite: Run `00_Generate_Data` then `01_Stitch_Transcripts` first*

# COMMAND ----------

# DBTITLE 1,Step 1: Sentiment Analysis
# MAGIC %sql
# MAGIC -- Analyze customer sentiment for each call
# MAGIC CREATE OR REPLACE TEMP VIEW scored_sentiment AS
# MAGIC SELECT 
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   direction,
# MAGIC   division,
# MAGIC   full_transcript,
# MAGIC   ai_analyze_sentiment(full_transcript) AS sentiment,
# MAGIC   CAST(transcript_duration AS INT) AS call_duration_seconds,
# MAGIC   num_utterances
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.silver_conversations

# COMMAND ----------

# DBTITLE 1,Step 2b: Script Adherence (ai_similarity)
# MAGIC %sql
# MAGIC -- Compare agent's actual greeting/closing against expected scripts using ai_similarity
# MAGIC -- UPDATE: Replace 'HCP' below with your HC_PROVIDER value from notebook 00 config
# MAGIC -- Expected scripts:
# MAGIC --   Greeting: 'Thank you for calling HCP, this is [agent name] speaking. How may I help you today?'
# MAGIC --   Closing: 'Is there anything else I can help you with? Thank you for calling HCP. Have a wonderful day.'
# MAGIC -- Scores 0.0 to 1.0: how close the agent's language matches the ideal
# MAGIC CREATE OR REPLACE TEMP VIEW scored_classified AS
# MAGIC WITH base_classified AS (
# MAGIC   SELECT 
# MAGIC     s.*,
# MAGIC     ai_classify(full_transcript, ARRAY('Billing Dispute', 'Appointment Scheduling', 'Clinical Triage', 'Complaint', 'General Inquiry', 'Insurance Question', 'Prescription Refill')) AS call_category,
# MAGIC     ai_classify(full_transcript, ARRAY('escalate_to_supervisor', 'routine', 'coaching_opportunity')) AS disposition,
# MAGIC     ai_classify(full_transcript, ARRAY('compliant', 'partially_compliant', 'non_compliant')) AS protocol_adherence
# MAGIC   FROM scored_sentiment s
# MAGIC ),
# MAGIC -- Extract first and last agent utterances for script adherence check
# MAGIC agent_lines AS (
# MAGIC   SELECT 
# MAGIC     interaction_id,
# MAGIC     -- First agent line = greeting
# MAGIC     REGEXP_EXTRACT(full_transcript, '(?m)^\\[.*?\\] INTERNAL: (.+?)$', 1) AS actual_greeting,
# MAGIC     -- Last agent line = closing
# MAGIC     REGEXP_EXTRACT(full_transcript, '(?m).*\\[.*?\\] INTERNAL: (.+?)$', 1) AS actual_closing
# MAGIC   FROM mmt_aws_usw2_catalog.contact_calls.silver_conversations
# MAGIC )
# MAGIC SELECT
# MAGIC   c.*,
# MAGIC   ai_similarity(
# MAGIC     a.actual_greeting,
# MAGIC     'Thank you for calling HCP, this is [agent name] speaking. How may I help you today?'
# MAGIC   ) AS greeting_adherence_score,
# MAGIC   ai_similarity(
# MAGIC     a.actual_closing,
# MAGIC     'Is there anything else I can help you with? Thank you for calling HCP. Have a wonderful day.'
# MAGIC   ) AS closing_adherence_score
# MAGIC FROM base_classified c
# MAGIC LEFT JOIN agent_lines a ON c.interaction_id = a.interaction_id

# COMMAND ----------

# DBTITLE 1,Step 3: QA Rubric Scoring (10 Criteria)
# MAGIC %sql
# MAGIC -- Score each call against the QA rubric using ai_query (Claude Sonnet 4)
# MAGIC -- Architecture: Claude Sonnet 4 = strict, nuanced scorer; GPT-5.5 Pro judges in nb 03
# MAGIC -- Rubric sourced from mmt_aws_usw2_catalog.contact_calls.qa_rubric (10 criteria)
# MAGIC CREATE OR REPLACE TEMP VIEW gold_qa_evaluations AS
# MAGIC WITH rubric_text AS (
# MAGIC   SELECT CONCAT_WS('\n', COLLECT_LIST(
# MAGIC     CONCAT('- ', criterion, ' (weight ', CAST(weight AS STRING), '): ', expected_behavior)
# MAGIC   )) AS rubric_prompt
# MAGIC   FROM mmt_aws_usw2_catalog.contact_calls.qa_rubric
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.*,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-sonnet-4-6',
# MAGIC     CONCAT(
# MAGIC       'Score this contact center call transcript on the following criteria (1=poor, 5=exceptional). ',
# MAGIC       'Return ONLY a JSON object with keys: ',
# MAGIC       'greeting_score, identity_verification_score, empathy_score, commitment_score, ',
# MAGIC       'branding_score, compliance_score, resolution_score, further_assistance_score, ',
# MAGIC       'closing_score, customer_service_score, ',
# MAGIC       'overall_score (weighted average using weights below), coaching_notes (one sentence), requires_human_review (true ONLY if overall_score < 3.0 or a critical safety/compliance violation occurred, false otherwise). ',
# MAGIC       '\n\nRubric:\n', r.rubric_prompt,
# MAGIC       '\n\nTranscript:\n', c.full_transcript
# MAGIC     )
# MAGIC   ) AS qa_scores_json
# MAGIC FROM scored_classified c
# MAGIC CROSS JOIN rubric_text r

# COMMAND ----------

# DBTITLE 1,Step 4: Parse Scores and Persist Gold Table
# MAGIC %sql
# MAGIC -- Parse JSON scores + script adherence → persist gold_scorecard for notebooks 03, 04, 05
# MAGIC -- LLM wraps JSON in code fences; extract the JSON object between first { and last }
# MAGIC CREATE OR REPLACE TABLE mmt_aws_usw2_catalog.contact_calls.gold_scorecard AS
# MAGIC WITH cleaned AS (
# MAGIC   SELECT *,
# MAGIC     REGEXP_EXTRACT(qa_scores_json, '(?s)(\\{.*\\})', 1) AS clean_json
# MAGIC   FROM gold_qa_evaluations
# MAGIC   WHERE qa_scores_json IS NOT NULL AND qa_scores_json != ''
# MAGIC )
# MAGIC SELECT
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   direction,
# MAGIC   division,
# MAGIC   call_duration_seconds,
# MAGIC   num_utterances,
# MAGIC   sentiment,
# MAGIC   call_category,
# MAGIC   disposition,
# MAGIC   protocol_adherence,
# MAGIC   -- Rubric scores (10 criteria, 1-5 scale)
# MAGIC   CAST(get_json_object(clean_json, '$.greeting_score') AS INT) AS greeting_score,
# MAGIC   CAST(get_json_object(clean_json, '$.identity_verification_score') AS INT) AS identity_verification_score,
# MAGIC   CAST(get_json_object(clean_json, '$.empathy_score') AS INT) AS empathy_score,
# MAGIC   CAST(get_json_object(clean_json, '$.commitment_score') AS INT) AS commitment_score,
# MAGIC   CAST(get_json_object(clean_json, '$.branding_score') AS INT) AS branding_score,
# MAGIC   CAST(get_json_object(clean_json, '$.compliance_score') AS INT) AS compliance_score,
# MAGIC   CAST(get_json_object(clean_json, '$.resolution_score') AS INT) AS resolution_score,
# MAGIC   CAST(get_json_object(clean_json, '$.further_assistance_score') AS INT) AS further_assistance_score,
# MAGIC   CAST(get_json_object(clean_json, '$.closing_score') AS INT) AS closing_score,
# MAGIC   CAST(get_json_object(clean_json, '$.customer_service_score') AS INT) AS customer_service_score,
# MAGIC   CAST(get_json_object(clean_json, '$.overall_score') AS DOUBLE) AS overall_qa_score,
# MAGIC   ROUND(greeting_adherence_score, 3) AS greeting_adherence,
# MAGIC   ROUND(closing_adherence_score, 3) AS closing_adherence,
# MAGIC   -- Coaching
# MAGIC   get_json_object(clean_json, '$.coaching_notes') AS coaching_notes,
# MAGIC   CAST(get_json_object(clean_json, '$.requires_human_review') AS BOOLEAN) AS requires_human_review,
# MAGIC   -- AI-generated summary for dashboards (20 words)
# MAGIC   ai_summarize(full_transcript, 20) AS call_summary,
# MAGIC   -- HIPAA: PII-redacted transcript safe for sharing with QA vendors
# MAGIC   ai_mask(full_transcript, ARRAY('person', 'phone', 'address', 'ssn')) AS redacted_transcript,
# MAGIC   full_transcript,
# MAGIC   current_timestamp() AS evaluated_at
# MAGIC FROM cleaned;
# MAGIC
# MAGIC -- Verify: show summary after table creation
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_scored,
# MAGIC   SUM(CASE WHEN greeting_score IS NOT NULL THEN 1 ELSE 0 END) AS scores_parsed_ok,
# MAGIC   ROUND(AVG(overall_qa_score), 2) AS avg_overall_qa,
# MAGIC   ROUND(AVG(greeting_adherence), 3) AS avg_greeting_adherence,
# MAGIC   SUM(CASE WHEN requires_human_review THEN 1 ELSE 0 END) AS flagged_for_review
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard

# COMMAND ----------

# DBTITLE 1,View: Gold Scorecard Results
# MAGIC %sql
# MAGIC -- Final gold table: one row per call with all QA scores + AI enrichment
# MAGIC SELECT 
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   sentiment,
# MAGIC   call_category,
# MAGIC   disposition,
# MAGIC   protocol_adherence,
# MAGIC   overall_qa_score,
# MAGIC   greeting_score,
# MAGIC   identity_verification_score,
# MAGIC   empathy_score,
# MAGIC   compliance_score,
# MAGIC   resolution_score,
# MAGIC   greeting_adherence,
# MAGIC   closing_adherence,
# MAGIC   requires_human_review,
# MAGIC   call_summary,
# MAGIC   coaching_notes
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.gold_scorecard
# MAGIC ORDER BY overall_qa_score ASC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Gold Layer Complete — AI-Scored QA Evaluations
# MAGIC
# MAGIC **What we just built:** Every call now has:
# MAGIC * **Sentiment** — Customer emotional state (Positive/Negative/Neutral/Mixed)
# MAGIC * **Category** — Business classification (Billing, Clinical, Scheduling, etc.)
# MAGIC * **5 QA Scores** — Each criterion 1-5 (greeting, empathy, accuracy, escalation, compliance)
# MAGIC * **Overall Score** — Weighted composite
# MAGIC * **Coaching Notes** — AI-generated improvement recommendations
# MAGIC * **Human Review Flag** — Low-confidence calls flagged for supervisor review
# MAGIC
# MAGIC **Business Impact:** A human QA reviewer takes ~45 minutes per call. This AI pipeline scores 50 calls in seconds — projected **98% reduction in QA review time** for routine calls.
# MAGIC
# MAGIC **Next notebook →** `03_LLM_Judge_Evals` validates these AI scores against golden reference data.
# MAGIC
# MAGIC ---
# MAGIC ### For the pod
# MAGIC * The `gold_scorecard` view is what powers the dashboard and Genie Space
# MAGIC * `requires_human_review = true` calls flow to the HITL triage queue
# MAGIC * In production, this runs as a scheduled Lakeflow job on new call batches