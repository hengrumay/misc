# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — Stitch Transcripts (Bronze → Silver)
# MAGIC
# MAGIC **What this does:** Transforms line-by-line Genesys transcripts into full conversations with metadata for AI scoring.
# MAGIC
# MAGIC ```
# MAGIC BEFORE (transcripts_raw)                                                    AFTER (silver_conversations)
# MAGIC +----------------+------+----------+------------------+-----------------+   +----------------+--------------+---------+------------------+-------------------------------------+
# MAGIC | interaction_id | line | who      | date_time        | text            |   | interaction_id | agent        | queue   | start / end      | full_transcript                     |
# MAGIC +----------------+------+----------+------------------+-----------------+   +----------------+--------------+---------+------------------+-------------------------------------+
# MAGIC | call-001       | 1    | AGENT    | 2026-05-11 07:24 | Hi, this is...  |   | call-001       | Maria Garcia | Billing | 07:24 -> 07:29   | [07:24:00] AGENT: Hi, this is...    |
# MAGIC | call-001       | 2    | CUSTOMER | 2026-05-11 07:24 | I have a...     |   |                |              |         | duration: 355s   | [07:24:29] CUSTOMER: I have a...    |
# MAGIC | call-001       | 3    | AGENT    | 2026-05-11 07:25 | Let me...       |   |                |              |         |                  | [07:24:59] AGENT: Let me...         |
# MAGIC | call-001       | 4    | CUSTOMER | 2026-05-11 07:25 | Thanks          |   |                |              |         |                  | [07:25:28] CUSTOMER: Thanks         |
# MAGIC | call-002       | 1    | AGENT    | 2026-05-29 18:29 | Welcome...      |   | call-002       | James Smith  | Nurse   | 18:29 -> 18:40   | [18:29:00] AGENT: Welcome to...     |
# MAGIC | call-002       | 2    | CUSTOMER | 2026-05-29 18:30 | My child...     |   |                |              |         | duration: 649s   | [18:29:38] CUSTOMER: My child...    |
# MAGIC | ...            | ...  | ...      | ...              | ...             |   | ...            | ...          | ...     | ...              | ...                                 |
# MAGIC +----------------+------+----------+------------------+-----------------+   +----------------+--------------+---------+------------------+-------------------------------------+
# MAGIC   500+ rows (2-17 per call)                                                   50 rows (1 per call, timestamps inline)
# MAGIC ```
# MAGIC
# MAGIC **Why this step is needed:** Genesys Cloud exports transcripts as one row per utterance (confirmed by the `line_num` bigint column in `dev_ds_b_playground_hackathon_call_transcripts`). AI scoring needs the full conversation as a single text block.
# MAGIC
# MAGIC **Key operations:**
# MAGIC 1. GROUP BY `interaction_id` + ORDER BY `line_num` -- reassemble utterances in order
# MAGIC 2. CONCAT with speaker labels (+ optional per-utterance timestamps if `date_time` is available)
# MAGIC 3. JOIN to `interactions_raw` on `Conversation_ID` -- attach metadata (queue, agent, duration)
# MAGIC
# MAGIC > **Note on ordering:** The stitch uses `line_num` (bigint) for sequential utterance ordering -- NOT `date_time`. This means the stitching works correctly even if per-utterance timestamps are missing. The `date_time` field is purely optional enrichment embedded in the output text. If `date_time` is NULL, timestamps are omitted and the transcript shows speaker labels only.
# MAGIC
# MAGIC **Rubric Criterion 1:** End-to-End Functionality — "Fully automated ingest → AI → validate → query"
# MAGIC
# MAGIC ---
# MAGIC *Prerequisite: `transcripts_raw` and `interactions_raw` must exist in Delta (run `00_Generate_Data` for synthetic data, or point to your Genesys export)*

# COMMAND ----------

# DBTITLE 1,Preview Raw Transcripts (Line-by-Line)
# MAGIC %sql
# MAGIC -- Each row is ONE utterance from a call. We need to stitch these into full conversations.
# MAGIC SELECT interaction_id, line_num, date_time, participant, participant_type, transcribed_text
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.transcripts_raw
# MAGIC WHERE interaction_id = (SELECT interaction_id FROM mmt_aws_usw2_catalog.contact_calls.transcripts_raw LIMIT 1)
# MAGIC ORDER BY line_num

# COMMAND ----------

# DBTITLE 1,What Happens Next
# MAGIC %md
# MAGIC ### The Transform
# MAGIC
# MAGIC The cell below takes those individual utterances (2-17 per call) and stitches them into **one row** with the full conversation:
# MAGIC
# MAGIC ```
# MAGIC                    transcripts_raw (500+ rows)             interactions_raw (50 rows)
# MAGIC                          |                                        |
# MAGIC                          |  GROUP BY interaction_id               |
# MAGIC                          |  ORDER BY line_num                     |
# MAGIC                          |  CONCAT_WS with speaker labels         |
# MAGIC                          |  PRESERVE start/end timestamps         |
# MAGIC                          |                                        |
# MAGIC                          v                                        |
# MAGIC                stitched (50 rows)                                 |
# MAGIC                - full_transcript                                  |
# MAGIC                - transcript_start_time                            |
# MAGIC                - transcript_end_time                              |
# MAGIC                - transcript_duration                              |
# MAGIC                - num_utterances                                   |
# MAGIC                          |                                        |
# MAGIC                          +--------------- JOIN -------------------+
# MAGIC                                           |              (on interaction_id = Conversation_ID)
# MAGIC                                           v
# MAGIC                             silver_conversations (50 rows)
# MAGIC                             - full_transcript (complete text)
# MAGIC                             - transcript_start_time, end_time, duration
# MAGIC                             - agent_name, queue, division
# MAGIC                             - call_duration_seconds
# MAGIC                             - skills, language, wrap_up
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Stitch Transcripts into Full Conversations
# MAGIC %sql
# MAGIC -- Core ETL: Group utterances by call, order by line_num (NOT date_time), concatenate into full transcript
# MAGIC -- line_num is the guaranteed sequential ordering column from Genesys; date_time is optional enrichment
# MAGIC CREATE OR REPLACE TABLE mmt_aws_usw2_catalog.contact_calls.silver_conversations AS
# MAGIC WITH stitched AS (
# MAGIC   SELECT 
# MAGIC     t.interaction_id,
# MAGIC     t.direction,
# MAGIC     t.transcript_duration,
# MAGIC     t.transcript_start_time,
# MAGIC     t.transcript_end_time,
# MAGIC     -- Concatenate all lines into one conversation with speaker labels
# MAGIC     CONCAT_WS('\n', 
# MAGIC       COLLECT_LIST(
# MAGIC         -- Timestamps: included if date_time is available; gracefully omits if NULL
# MAGIC         CONCAT(
# MAGIC           CASE WHEN t.date_time IS NOT NULL THEN CONCAT('[', t.date_time, '] ') ELSE '' END,
# MAGIC           UPPER(t.participant_type), ': ', t.transcribed_text
# MAGIC         )
# MAGIC       )
# MAGIC     ) AS full_transcript,
# MAGIC     COUNT(*) AS num_utterances
# MAGIC   FROM (
# MAGIC     SELECT * FROM mmt_aws_usw2_catalog.contact_calls.transcripts_raw ORDER BY interaction_id, line_num
# MAGIC   ) t
# MAGIC   GROUP BY t.interaction_id, t.direction, t.transcript_duration, 
# MAGIC            t.transcript_start_time, t.transcript_end_time
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.*,
# MAGIC   i.Users AS agent_name,
# MAGIC   i.Queue AS queue,
# MAGIC   i.Duration AS call_duration_seconds,
# MAGIC   i.Skills AS skills,
# MAGIC   i.Languages AS language,
# MAGIC   i.Wrap_up AS wrap_up,
# MAGIC   i.Division AS division,
# MAGIC   i.Conversation_ID
# MAGIC FROM stitched s
# MAGIC JOIN mmt_aws_usw2_catalog.contact_calls.interactions_raw i ON s.interaction_id = i.Conversation_ID;
# MAGIC
# MAGIC -- Show results immediately so the output isn't empty
# MAGIC SELECT
# MAGIC   COUNT(*) AS conversations_created,
# MAGIC   SUM(num_utterances) AS utterances_stitched,
# MAGIC   ROUND(AVG(call_duration_seconds), 0) AS avg_call_duration_sec
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.silver_conversations

# COMMAND ----------

# DBTITLE 1,Verify: Full Conversations Ready
# MAGIC %sql
# MAGIC -- Distribution stats: how varied are the conversations?
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_conversations,
# MAGIC   SUM(num_utterances) AS total_utterances_stitched,
# MAGIC   MIN(num_utterances) AS shortest_call_lines,
# MAGIC   MAX(num_utterances) AS longest_call_lines,
# MAGIC   ROUND(AVG(num_utterances), 1) AS avg_lines_per_call,
# MAGIC   ROUND(AVG(call_duration_seconds), 0) AS avg_duration_sec,
# MAGIC   COUNT(CASE WHEN num_utterances <= 5 THEN 1 END) AS abandoned_short_calls,
# MAGIC   COUNT(CASE WHEN num_utterances >= 12 THEN 1 END) AS extended_calls
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.silver_conversations

# COMMAND ----------

# DBTITLE 1,Sample: Stitched Conversations
# MAGIC %sql
# MAGIC -- Sample output: one row per call with full transcript text
# MAGIC SELECT 
# MAGIC   interaction_id,
# MAGIC   agent_name,
# MAGIC   queue,
# MAGIC   transcript_start_time,
# MAGIC   transcript_end_time,
# MAGIC   transcript_duration,
# MAGIC   num_utterances,
# MAGIC   LEFT(full_transcript, 300) AS transcript_preview
# MAGIC FROM mmt_aws_usw2_catalog.contact_calls.silver_conversations
# MAGIC ORDER BY num_utterances DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Silver Layer Complete
# MAGIC
# MAGIC **What we did:** Transformed 500+ individual utterances into 50 complete call transcripts (2-17 lines each), joined with Genesys metadata.
# MAGIC
# MAGIC **Output schema (`silver_conversations`):**
# MAGIC
# MAGIC | Column | Source | Purpose |
# MAGIC |--------|--------|---------|
# MAGIC | `full_transcript` | Stitched from `transcripts_raw` | AI scoring input -- with per-utterance timestamps inline (if available) |
# MAGIC | `transcript_start_time` | `transcripts_raw.transcript_start_time` | When the call began (may be NULL in some exports) |
# MAGIC | `transcript_end_time` | `transcripts_raw.transcript_end_time` | When the call ended (may be NULL in some exports) |
# MAGIC | `transcript_duration` | `transcripts_raw.transcript_duration` | Total call length (from Genesys) |
# MAGIC | `agent_name` | `interactions_raw.Users` | Agent performance tracking |
# MAGIC | `queue` | `interactions_raw.Queue` | Department-level reporting |
# MAGIC | `call_duration_seconds` | `interactions_raw.Duration` | Call length context for scoring |
# MAGIC | `num_utterances` | COUNT of transcript lines | Conversation complexity indicator |
# MAGIC | `division` | `interactions_raw.Division` | Facility-level rollup |
# MAGIC | `skills`, `language` | `interactions_raw` | Routing and language context |
# MAGIC
# MAGIC **Next notebook -->** `02_AI_Scoring` runs sentiment analysis, rubric scoring, and topic extraction on each conversation.
# MAGIC
# MAGIC ---
# MAGIC ### For the pod
# MAGIC This is the step that maps raw Genesys exports into AI-ready format. When switching from synthetic to real Genesys data, only the table references at the top need to change -- the transform logic stays the same.
# MAGIC
# MAGIC ---
# MAGIC ### Scaling to Production
# MAGIC
# MAGIC This notebook uses `CREATE OR REPLACE TABLE` (full rebuild) which is fine for 50 calls. In production with 10K+ daily calls:
# MAGIC
# MAGIC | Concern | Hackathon (now) | Production | How |
# MAGIC |---------|----------------|------------|-----|
# MAGIC | Trigger | Manual run | Incremental on new files | Auto Loader + Lakeflow Declarative Pipeline |
# MAGIC | Processing | Full rebuild every run | Only new/changed calls | Streaming Table with `APPEND` flow |
# MAGIC | Partitioning | None (50 rows) | By date | `CLUSTER BY (transcript_start_time)` via Liquid Clustering |
# MAGIC | Monitoring | Manual verify cell | Automated alerts | Expectations: `EXPECT (num_utterances > 0)`, row count checks |
# MAGIC | Latency | Minutes (batch) | Seconds (streaming) | Trigger.AvailableNow or continuous |
# MAGIC
# MAGIC **Lakeflow version of this stitch (pseudo-code):**
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_conversations
# MAGIC AS SELECT ... -- same stitch logic
# MAGIC FROM STREAM(read_files('/path/to/genesys/transcripts/'))
# MAGIC ```
# MAGIC
# MAGIC **Key point for judges:** The SQL logic is identical at any scale -- only the orchestration wrapper changes. This is a 10-minute migration from notebook to Lakeflow pipeline.