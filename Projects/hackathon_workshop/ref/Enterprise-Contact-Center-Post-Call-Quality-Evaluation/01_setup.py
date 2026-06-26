# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Contact Center — 01 Setup
# MAGIC
# MAGIC Creates the schema, Delta tables, QA rubric data, and registers **Unity Catalog
# MAGIC SQL functions** that power the post-call quality evaluation agent.
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `bronze_call_metadata` | Raw call file metadata + agent/queue info from Auto Loader |
# MAGIC | Silver | `silver_transcriptions` | Whisper transcriptions with call metadata |
# MAGIC | Gold | `gold_qa_evaluations` | QA scores, compliance flags, sentiment, coaching notes |
# MAGIC | Ref | `qa_rubric` | 5-criterion weighted QA checklist for agent scoring |

# COMMAND ----------

# DBTITLE 1,Configuration

# -- Parameterized configuration: override via widgets or job parameters --
dbutils.widgets.text("catalog", "chada_demos", "Unity Catalog")
dbutils.widgets.text("schema", "contact_center_qa", "Schema")
dbutils.widgets.text("volume_path", "/Volumes/chada_demos/contact_center_qa/call_recordings", "Call Recordings Volume Path")
dbutils.widgets.text("warehouse_id", "4b9b953939869799", "SQL Warehouse ID")
dbutils.widgets.text("whisper_endpoint", "va_whisper_large_v3", "Whisper Model Endpoint")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en", "Embedding Endpoint")
dbutils.widgets.text("vector_search_endpoint", "one-env-shared-endpoint-1", "Vector Search Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_PATH = dbutils.widgets.get("volume_path")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")
VS_ENDPOINT = dbutils.widgets.get("vector_search_endpoint")

FQ = f"{CATALOG}.{SCHEMA}"
print(f"Config: {FQ} | Volume: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Initialize Schema & Tables

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print(f"Catalog creation skipped (may already exist or lack permissions): {e}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

# Ensure Volume exists for audio files
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.audio_files COMMENT 'Raw audio files for advisory call recordings'")
    print(f"Volume ready: {VOLUME_PATH}")
except Exception as e:
    print(f"Volume creation note: {e}")

# -- Bronze: call metadata from Auto Loader --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_call_metadata (
  call_id              STRING     COMMENT 'Unique call identifier',
  filename             STRING     COMMENT 'Original filename of the recording',
  file_path            STRING     COMMENT 'Full Volume path to the audio file',
  agent_id             STRING     COMMENT 'Contact center agent identifier',
  queue_type           STRING     COMMENT 'Queue/department: Sales, Support, Billing, Technical, Complaints',
  call_duration_seconds INT       COMMENT 'Call duration in seconds',
  call_timestamp       TIMESTAMP  COMMENT 'Timestamp when the call occurred',
  file_size_bytes      LONG       COMMENT 'Size of the audio file in bytes',
  ingested_at          TIMESTAMP  COMMENT 'Timestamp when Auto Loader ingested the file'
)
USING DELTA
COMMENT 'Bronze layer: raw call file metadata with agent and queue info'
TBLPROPERTIES ('quality' = 'bronze')
""")

# -- Silver: Whisper transcriptions with call metadata --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.silver_transcriptions (
  call_id              STRING     COMMENT 'Unique call identifier',
  filename             STRING     COMMENT 'Original audio filename',
  file_path            STRING     COMMENT 'Full Volume path',
  agent_id             STRING     COMMENT 'Contact center agent identifier',
  transcription        STRING     COMMENT 'Full text transcription from Whisper',
  word_count           INT        COMMENT 'Number of words in the transcription',
  call_duration_seconds INT       COMMENT 'Call duration in seconds',
  transcribed_at       TIMESTAMP  COMMENT 'Timestamp when transcription completed'
)
USING DELTA
COMMENT 'Silver layer: call transcriptions produced by Whisper large-v3 with agent metadata'
TBLPROPERTIES ('quality' = 'silver')
""")

# -- Gold: QA evaluations with per-criterion scores and compliance flags --
# CREATE OR REPLACE TABLE {FQ}.gold_enriched_calls (
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.gold_enriched_calls (
  filename               STRING     COMMENT 'Original audio filename',
  file_path              STRING     COMMENT 'Full Volume path',
  call_id                STRING     COMMENT 'Unique call identifier',
  agent_id               STRING     COMMENT 'Contact center agent identifier',
  queue_type             STRING     COMMENT 'Queue/department',
  transcription          STRING     COMMENT 'Full transcription text',
  overall_qa_score       DOUBLE     COMMENT 'Weighted overall QA score 1.0-5.0',
  greeting_score         INT        COMMENT 'Proper greeting and ID verification (1-5)',
  empathy_score          INT        COMMENT 'Empathy markers and active listening (1-5)',
  accuracy_score         INT        COMMENT 'Correct information provided (1-5)',
  escalation_score       INT        COMMENT 'Escalation protocol adherence (1-5)',
  compliance_score       INT        COMMENT 'Regulatory compliance and disclosures (1-5)',
  sentiment              STRING     COMMENT 'Overall sentiment: Positive, Negative, Neutral, Mixed',
  sentiment_confidence   DOUBLE     COMMENT 'Confidence score for sentiment 0.0-1.0',
  topics                 STRING     COMMENT 'Comma-separated extracted topics',
  call_category          STRING     COMMENT 'Call type: Sales, Support, Billing, Technical, Complaints',
  compliance_flags       STRING     COMMENT 'JSON array of compliance violations found',
  coaching_notes         STRING     COMMENT 'AI-generated coaching recommendations',
  requires_human_review  BOOLEAN    COMMENT 'True if flagged as outlier needing supervisor review',
  evaluated_at           TIMESTAMP  COMMENT 'Timestamp when evaluation completed'
)
USING DELTA
COMMENT 'Gold layer: QA evaluation results with per-criterion scores, compliance flags, and coaching notes'
TBLPROPERTIES ('quality' = 'gold')
""")

# -- QA Rubric reference table (configurable checklist) --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.qa_rubric (
  rubric_id     INT        COMMENT 'Unique rubric criterion ID',
  category      STRING     COMMENT 'QA checklist category',
  criterion     STRING     COMMENT 'Specific assessment criterion',
  score_1_desc  STRING     COMMENT 'Description of score 1 (Fail)',
  score_3_desc  STRING     COMMENT 'Description of score 3 (Acceptable)',
  score_5_desc  STRING     COMMENT 'Description of score 5 (Excellent)',
  weight        DOUBLE     COMMENT 'Weight of this criterion in overall QA score'
)
USING DELTA
COMMENT 'Configurable QA checklist rubric for evaluating contact center agent calls'
""")

print("All tables initialized.")

# COMMAND ----------

# DBTITLE 1,Seed Advisor Rubric

rubric_count = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.qa_rubric").collect()[0]["cnt"]
if rubric_count == 0:
    spark.sql(f"""
    INSERT INTO {FQ}.qa_rubric VALUES
    (1, 'Proper Greeting & ID Verification',
        'Agent properly greets the customer, states their name and department, and verifies customer identity',
        'No greeting; fails to identify themselves or verify customer',
        'Basic greeting; states name but incomplete verification',
        'Warm, professional greeting; states name and dept; full identity verification completed',
        0.15),
    (2, 'Empathy & Active Listening',
        'Agent demonstrates empathy, validates customer feelings, paraphrases concerns, and asks clarifying questions',
        'Dismissive or cold; interrupts customer; ignores stated concerns',
        'Neutral tone; acknowledges concern without demonstrating empathy',
        'Validates feelings; paraphrases concerns; asks clarifying questions; reassures customer',
        0.20),
    (3, 'Accurate Information Provided',
        'Agent provides correct, complete information including policies, procedures, and relevant details',
        'Provides incorrect or misleading information; guesses without verification',
        'Provides mostly correct info with minor gaps; does not cite source',
        'Fully accurate info; cites policy or documentation; confirms customer understanding',
        0.25),
    (4, 'Escalation Protocol Adherence',
        'Agent correctly identifies when escalation is needed and follows proper escalation procedures',
        'Fails to escalate when clearly needed; attempts to handle beyond authority',
        'Recognizes need but incomplete handoff; missing warm transfer',
        'Correctly identifies escalation triggers; follows protocol; warm transfer with context',
        0.20),
    (5, 'Compliance & Required Disclosures',
        'Agent delivers all required regulatory disclosures and follows compliance protocols',
        'Misses mandatory disclosures; non-compliant language used',
        'Most disclosures given but incomplete; minor compliance gaps',
        'All required disclosures given; regulatory language correct; proper consent obtained',
        0.20)
    """)
    print("QA Rubric seeded with 5 criteria.")
else:
    print(f"QA Rubric already has {rubric_count} rows -- skipping seed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL UC Functions (12 total)
# MAGIC
# MAGIC All functions are pure SQL -- no Python UDFs, no `WorkspaceClient()` dependencies.
# MAGIC This ensures they work in all contexts including model serving endpoints.

# COMMAND ----------

# DBTITLE 1,UC Function 1: find_audio_file

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.find_audio_file")
spark.sql(f"""
CREATE FUNCTION {FQ}.find_audio_file(speaker_query STRING)
RETURNS STRING
COMMENT 'Finds an audio file in the Volume by speaker name, number, or filename fragment. Returns JSON with file_path, filename, and match metadata.'
RETURN (
  WITH files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  ),
  speaker_num AS (
    SELECT CASE
      WHEN regexp_extract(lower(speaker_query), 'speaker[_\\\\s]*0*(\\\\d+)', 1) != ''
        THEN regexp_extract(lower(speaker_query), 'speaker[_\\\\s]*0*(\\\\d+)', 1)
      WHEN regexp_extract(lower(speaker_query), '\\\\b0*(\\\\d+)\\\\b', 1) != ''
        THEN regexp_extract(lower(speaker_query), '\\\\b0*(\\\\d+)\\\\b', 1)
      ELSE NULL
    END AS num
  ),
  matches AS (
    SELECT filename, path, file_size FROM files, speaker_num
    WHERE speaker_num.num IS NOT NULL
      AND lower(filename) RLIKE concat('speaker[_\\\\s]*0*', speaker_num.num, '[_.]')
  )
  SELECT CASE
    WHEN (SELECT num FROM speaker_num) IS NULL THEN
      to_json(named_struct('status', 'error', 'message', concat('Could not parse speaker from: ', speaker_query)))
    WHEN (SELECT count(*) FROM matches) = 0 THEN
      to_json(named_struct('status', 'not_found', 'message', concat('No files for speaker ', (SELECT num FROM speaker_num))))
    ELSE
      to_json(named_struct(
        'status', 'found',
        'file_path', (SELECT path FROM matches LIMIT 1),
        'filename', (SELECT filename FROM matches LIMIT 1),
        'speaker_id', (SELECT num FROM speaker_num),
        'file_size_bytes', (SELECT file_size FROM matches LIMIT 1)
      ))
  END
)
""")
print("Registered: find_audio_file")

# COMMAND ----------

# DBTITLE 1,UC Function 2: find_all_audio_files

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.find_all_audio_files")
spark.sql(f"""
CREATE FUNCTION {FQ}.find_all_audio_files()
RETURNS STRING
COMMENT 'Lists all .wav audio files in the advisory services Volume. Returns JSON array with file metadata.'
RETURN (
  WITH files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size,
      _metadata.file_modification_time AS modified_time
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  )
  SELECT to_json(named_struct(
    'total_files', (SELECT count(*) FROM files),
    'files', (SELECT collect_list(
      named_struct('filename', filename, 'file_path', path, 'file_size_bytes', file_size, 'modified_time', modified_time)
    ) FROM files)
  ))
)
""")
print("Registered: find_all_audio_files")

# COMMAND ----------

# DBTITLE 1,UC Function 3: read_audio_base64

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.read_audio_base64")
spark.sql(f"""
CREATE FUNCTION {FQ}.read_audio_base64(file_path STRING)
RETURNS STRING
COMMENT 'Reads an audio file from the Volume and returns its base64-encoded binary content for Whisper inference.'
RETURN (
  SELECT base64(content)
  FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  WHERE _metadata.file_path = file_path
  LIMIT 1
)
""")
print("Registered: read_audio_base64")

# COMMAND ----------

# MAGIC %md
# MAGIC Before running `UC Function 4`, make sure you follow the instructions to host the Whisper large-v3 ASR model using model serving on Databricks.
# MAGIC
# MAGIC For Whisper, the easiest path is Databricks Marketplace: install a Whisper model (e.g., whisper-large-v3) from the marketplace into your catalog and deploy it as a serving endpoint with a GPU tier matched to your throughput needs. The agent's LLM endpoints (Claude, Llama) are Foundation Model API endpoints available by default; all endpoint names are configurable via widget parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ![][image1]
# MAGIC
# MAGIC [image1]: <data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/4gHYSUNDX1BST0ZJTEUAAQEAAAHIAAAAAAQwAABtbnRyUkdCIFhZWiAH4AABAAEAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAACRyWFlaAAABFAAAABRnWFlaAAABKAAAABRiWFlaAAABPAAAABR3dHB0AAABUAAAABRyVFJDAAABZAAAAChnVFJDAAABZAAAAChiVFJDAAABZAAAAChjcHJ0AAABjAAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAAgAAAAcAHMAUgBHAEJYWVogAAAAAAAAb6IAADj1AAADkFhZWiAAAAAAAABimQAAt4UAABjaWFlaIAAAAAAAACSgAAAPhAAAts9YWVogAAAAAAAA9tYAAQAAAADTLXBhcmEAAAAAAAQAAAACZmYAAPKnAAANWQAAE9AAAApbAAAAAAAAAABtbHVjAAAAAAAAAAEAAAAMZW5VUwAAACAAAAAcAEcAbwBvAGcAbABlACAASQBuAGMALgAgADIAMAAxADb/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAGHAnADASIAAhEBAxEB/8QAHQABAAICAwEBAAAAAAAAAAAAAAMFBgcBAgQICf/EAEoQAAEEAQMBAwcKBAQEBAYDAAIAAQMEBQYREhMHFCEiMVNUkdHSCBUyNEFRVZKTsSMlcnNCYXGhFiQz0xdSdLIYgYKzweE1RfH/xAAZAQEBAQEBAQAAAAAAAAAAAAAAAQIDBAX/xAAvEQEAAgEBBwIEBgMBAAAAAAAAAQIRAwQSITFBUfATgSJxkeEjMqGx0fFSYZLB/9oADAMBAAIRAxEAPwD9U0REBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEVOqHKa90Pg7x4vM6wwtG5GzOdezfijkFnZnbcSJnbwdn/wBHZBmyKgo3qWTpxZDG3ILVWcWOKaGRjCQX8ziTeDt/oobuZxuPkaG3ZYT49QhESNwD7TPiz8Ab7SLYW+9SZiOMtVpa84rGZZKipIpY5owmhkGSOQWIDF2cSF23Z2dvOzqOtfo3TsR07sE51Ju72BikYnhl4iXA2Z/JLiYFs/jsTP8AayrMxjhK/RUwkJb8SZ9n2fZ/M/3KGnepZCIp6FyCzGEssBHDIxiMkZuEgO7eYhMSEm87ELs/izoL9FQ1rlO6c8dO3DOdaXoTjFIxPFJxYuBM30S4kL7P47Ez/autC/RytGvlMXdguU7kQT17FeQZIpoiZiEwIXdiF2dnZ2fZ2dBkCKnRBcIqdEFwip0QXCKnRBcIqdEFwip0QXCKnRBcIqdEFwip0QXCKnRBcIqWrXPIHYbvUsDQSNGzRsL8vJEt35M//m/2Xit5HTWPzEGn72tq9fKWmZ4KUtmuE8jO+zcQduT7uz7bN47OgydFWfMxfilr2R/CnzKX4pa9kfwoLNFWfMxfilr2R/CnzMX4pa9kfwoLNFWfMxfilr2R/CnzKX4pa9kfwoLNFWfMxfilr2R/CnzMX4pa9kfwoLNFWfMxfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX4pa9kfwoLNFWfMxfilr2R/CnzKX4pa9kfwoLNFWfMxfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX4pa9kfwoLNFWfMpfilr2R/CnzKX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>

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1781060194138.png](./image_1781060194138.png "image_1781060194138.png")

# COMMAND ----------

# MAGIC %md
# MAGIC You can setup the model serving endpoint using all UI operations as shown above in the screenshots. However, you can also uncommend below code for SDK way of hosting.
# MAGIC
# MAGIC > __Note:__ You need to make sure below parameters setup are correct for your workspace to host the Whisper model via model serving.

# COMMAND ----------

# DBTITLE 1,Manual setup here
# endpoint_name = WHISPER_ENDPOINT
# model_uc_path = "system.ai.whisper_large_v3" # make sure your model after marketplace install is here
# version = 3
# workload_type = "GPU_MEDIUM"

# COMMAND ----------

# DBTITLE 1,then run this for SDK model serving
# import datetime

# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.serving import EndpointCoreConfigInput
# from databricks.sdk.errors import ResourceDoesNotExist

# w = WorkspaceClient()

# config = EndpointCoreConfigInput.from_dict({
#     "served_models": [
#         {
#             "name": endpoint_name,
#             "model_name": model_uc_path,
#             "model_version": version,
#             "workload_type": workload_type,
#             "workload_size": "Small",
#             "scale_to_zero_enabled": "False",
#         }
#     ]
# })

# try:
#     w.serving_endpoints.get(name=endpoint_name)
#     print(f"Endpoint '{endpoint_name}' already exists. Updating config...")
#     model_details = w.serving_endpoints.update_config(name=endpoint_name, served_models=config.served_models)
#     model_details.result(timeout=datetime.timedelta(minutes=30))
# except ResourceDoesNotExist:
#     print(f"Creating endpoint '{endpoint_name}'...")
#     model_details = w.serving_endpoints.create(name=endpoint_name, config=config)
#     model_details.result(timeout=datetime.timedelta(minutes=30))

# print("Endpoint ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC > Make sure you have Whisper model hosted before running next command

# COMMAND ----------

# DBTITLE 1,UC Function 4: transcribe_audio

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.transcribe_audio")
spark.sql(f"""
CREATE FUNCTION {FQ}.transcribe_audio(file_path STRING)
RETURNS STRING
COMMENT 'Transcribes an audio file using the Whisper large-v3 speech recognition model via ai_query. Returns the full transcript text.'
RETURN (
  SELECT ai_query(
    endpoint    => '{WHISPER_ENDPOINT}',
    request     => unbase64({FQ}.read_audio_base64(file_path)),
    returnType  => 'STRING',
    failOnError => false
  )
)
""")
print("Registered: transcribe_audio")

# COMMAND ----------

# DBTITLE 1,UC Function 5: classify_call_category

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.classify_call_category")
spark.sql(f"""
CREATE FUNCTION {FQ}.classify_call_category(transcription STRING)
RETURNS STRING
COMMENT 'Classifies a contact center call transcript into one category: Sales, Support, Billing, Technical, Complaints, Retention, Registration, Housing, Billing, Career Services, or Other.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are an enterprise contact center quality analyst. Classify this call transcript ',
      'into exactly ONE category from the following list:\\n',
      '- Sales\\n- Support\\n- Billing\\n- Technical\\n',
      '- Complaints\\n- Retention\\n- Account Management\\n- Other\\n\\n',
      'Respond with ONLY the category name. No explanation.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: classify_call_category")

# COMMAND ----------

# DBTITLE 1,UC Function 6: analyze_call_sentiment

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.analyze_call_sentiment")
spark.sql(f"""
CREATE FUNCTION {FQ}.analyze_call_sentiment(transcription STRING)
RETURNS STRING
COMMENT 'Analyzes student sentiment from a call transcript. Returns JSON with sentiment label and confidence.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'Analyze the overall student sentiment in this higher education advisory call transcript. ',
      'Return a JSON object with exactly two fields:\\n',
      '  "sentiment": one of "Positive", "Negative", "Neutral", "Mixed"\\n',
      '  "confidence": a decimal between 0.0 and 1.0\\n\\n',
      'Return ONLY the JSON. No markdown, no explanation.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: analyze_call_sentiment")

# COMMAND ----------

# DBTITLE 1,UC Function 7: extract_topics_and_intent

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.extract_topics_and_intent")
spark.sql(f"""
CREATE FUNCTION {FQ}.extract_topics_and_intent(transcription STRING)
RETURNS STRING
COMMENT 'Extracts key topics and primary intent from a call transcript. Returns JSON with topics array and intent string.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are analyzing a higher education advisory call. Extract the following from this transcript:\\n',
      '1. "topics": A JSON array of 2-5 key topics discussed (e.g., "FAFSA deadline", "GPA requirements", "transfer credits")\\n',
      '2. "intent": The single primary reason the student called (e.g., "Inquire about financial aid eligibility")\\n',
      '3. "improvement_areas": A JSON array of 0-3 areas where the advisor could improve\\n\\n',
      'Return ONLY a JSON object with these three fields. No markdown.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: extract_topics_and_intent")

# COMMAND ----------

# DBTITLE 1,UC Function 8: assess_rubric_rag

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.assess_rubric_rag")
spark.sql(f"""
CREATE FUNCTION {FQ}.assess_rubric_rag(transcription STRING)
RETURNS STRING
COMMENT 'Assesses advisor performance against the QA checklist rubric using RAG. Retrieves rubric criteria from the qa_rubric reference table and produces a weighted score (1-5) with per-criterion scores and coaching notes.'
RETURN (
  WITH rubric AS (
    SELECT collect_list(
      concat(
        'Criterion: ', criterion, ' (Weight: ', CAST(weight AS STRING), ')\\n',
        '  Score 1 (Poor): ', score_1_desc, '\\n',
        '  Score 3 (Acceptable): ', score_3_desc, '\\n',
        '  Score 5 (Excellent): ', score_5_desc
      )
    ) AS criteria
    FROM {FQ}.qa_rubric
  )
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are a contact center QA analyst evaluating an agent call against the quality checklist.\\n\\n',
      '## RUBRIC CRITERIA:\\n',
      array_join((SELECT criteria FROM rubric), '\\n\\n'),
      '\\n\\n## CALL TRANSCRIPT:\\n', transcription,
      '\\n\\n## INSTRUCTIONS:\\n',
      'Score each criterion 1-5. Then compute a single weighted overall score (round to nearest integer).\\n',
      'Also identify any compliance violations and generate specific coaching recommendations.\\n',
      'Return ONLY a JSON object with:\\n',
      '  "overall_score": integer 1-5\\n',
      '  "assessment": a 2-3 sentence narrative summary of agent performance\\n',
      '  "criterion_scores": object mapping criterion name to its individual score\\n',
      '  "compliance_flags": array of specific compliance violations found (empty array if none)\\n',
      '  "coaching_notes": specific coaching recommendations for improvement\\n',
      '  "requires_human_review": boolean true if score <= 2 or compliance violations found\\n',
      'No markdown formatting. Just the JSON.'
    )
  )
)
""")
print("Registered: assess_rubric_rag")

# COMMAND ----------

# DBTITLE 1,UC Function 9: transcribe_and_save_to_silver (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.transcribe_and_save_to_silver")
spark.sql(f"""
CREATE FUNCTION {FQ}.transcribe_and_save_to_silver(file_path STRING)
RETURNS STRING
COMMENT 'Transcribes a single audio file using Whisper large-v3 and returns the transcription with metadata. Returns JSON with status, filename, speaker_id, transcription text, word_count, and duration_hint.'
RETURN (
  WITH file_info AS (
    SELECT
      split(transcribe_and_save_to_silver.file_path, '/')[size(split(transcribe_and_save_to_silver.file_path, '/'))-1] AS fn,
      COALESCE(
        NULLIF(regexp_extract(transcribe_and_save_to_silver.file_path, 'Speaker[_\\\\s]*0*(\\\\d+)', 1), ''),
        'unknown'
      ) AS sid
  ),
  transcript AS (
    SELECT {FQ}.transcribe_audio(transcribe_and_save_to_silver.file_path) AS txt
  ),
  wc AS (
    SELECT size(split(trim((SELECT txt FROM transcript)), '\\\\s+')) AS word_count
  )
  SELECT to_json(named_struct(
    'status', 'success',
    'filename', (SELECT fn FROM file_info),
    'speaker_id', (SELECT sid FROM file_info),
    'transcription', (SELECT txt FROM transcript),
    'word_count', (SELECT word_count FROM wc),
    'duration_hint', CASE
      WHEN (SELECT word_count FROM wc) < 100 THEN 'short'
      WHEN (SELECT word_count FROM wc) < 500 THEN 'medium'
      ELSE 'long'
    END
  ))
)
""")
print("Registered: transcribe_and_save_to_silver")

# COMMAND ----------

# DBTITLE 1,UC Function 10: process_all_audio_to_silver (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.process_all_audio_to_silver")
spark.sql(f"""
CREATE FUNCTION {FQ}.process_all_audio_to_silver()
RETURNS STRING
COMMENT 'Checks audio file transcription status. Shows total files in Volume, how many are already transcribed in silver, and how many are pending. Returns JSON summary with counts and sample pending files.'
RETURN (
  WITH all_files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  ),
  already_done AS (
    SELECT file_path FROM {FQ}.silver_transcriptions
  ),
  pending AS (
    SELECT a.filename, a.path, a.file_size
    FROM all_files a
    LEFT ANTI JOIN already_done d ON a.path = d.file_path
  ),
  stats AS (
    SELECT
      (SELECT count(*) FROM all_files) AS total_files,
      (SELECT count(*) FROM already_done) AS already_transcribed,
      (SELECT count(*) FROM pending) AS pending_transcription
  )
  SELECT to_json(named_struct(
    'status', 'complete',
    'total_files', (SELECT total_files FROM stats),
    'already_transcribed', (SELECT already_transcribed FROM stats),
    'pending_transcription', (SELECT pending_transcription FROM stats),
    'sample_pending', (SELECT collect_list(named_struct('filename', filename, 'file_path', path))
                       FROM (SELECT * FROM pending LIMIT 5)),
    'message', CASE
      WHEN (SELECT pending_transcription FROM stats) = 0
        THEN 'All files already transcribed to silver.'
      ELSE concat('Found ', (SELECT pending_transcription FROM stats),
                  ' files pending transcription. Use transcribe_and_save_to_silver(file_path) for each file.')
    END
  ))
)
""")
print("Registered: process_all_audio_to_silver")

# COMMAND ----------

# DBTITLE 1,UC Function 11: enrich_silver_to_gold (SQL)
spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.enrich_silver_to_gold")
spark.sql(f"""
CREATE FUNCTION {FQ}.enrich_silver_to_gold()
RETURNS STRING
COMMENT 'Reports enrichment pipeline status. Shows silver record count, gold record count, and how many silver records are pending enrichment. Returns JSON with pipeline status and counts.'
RETURN (
  WITH silver_count AS (
    SELECT count(*) AS cnt FROM {FQ}.silver_transcriptions
  ),
  gold_count AS (
    SELECT count(*) AS cnt FROM {FQ}.gold_enriched_calls
  ),
  pending AS (
    SELECT count(*) AS cnt
    FROM {FQ}.silver_transcriptions s
    LEFT ANTI JOIN {FQ}.gold_enriched_calls g
      ON s.file_path = g.file_path
    WHERE s.transcription IS NOT NULL AND length(trim(s.transcription)) > 10
  )
  SELECT to_json(named_struct(
    'status', CASE WHEN (SELECT cnt FROM pending) = 0 THEN 'up_to_date' ELSE 'pending_enrichment' END,
    'silver_total', (SELECT cnt FROM silver_count),
    'gold_total', (SELECT cnt FROM gold_count),
    'pending_enrichment', (SELECT cnt FROM pending),
    'message', CASE
      WHEN (SELECT cnt FROM pending) = 0 AND (SELECT cnt FROM gold_count) > 0
        THEN 'All silver records have been enriched to gold.'
      WHEN (SELECT cnt FROM pending) = 0 AND (SELECT cnt FROM gold_count) = 0
        THEN 'No silver records available yet. Run transcription first.'
      ELSE concat((SELECT cnt FROM pending), ' silver records ready for enrichment. Use enrich_single_call(transcription) to enrich individual calls.')
    END
  ))
)
""")
print("Registered: enrich_silver_to_gold")

# COMMAND ----------

# DBTITLE 1,UC Function 12: enrich_single_call (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.enrich_single_call")
spark.sql(f"""
CREATE FUNCTION {FQ}.enrich_single_call(transcription STRING)
RETURNS STRING
COMMENT 'Runs the full AI enrichment pipeline on a single call transcript: sentiment analysis, topic extraction, intent classification, call categorization, and rubric-based RAG assessment. Returns comprehensive JSON with all enrichment results in one call.'
RETURN (
  WITH sentiment AS (
    SELECT {FQ}.analyze_call_sentiment(transcription) AS raw
  ),
  topics AS (
    SELECT {FQ}.extract_topics_and_intent(transcription) AS raw
  ),
  category AS (
    SELECT {FQ}.classify_call_category(transcription) AS raw
  ),
  rubric AS (
    SELECT {FQ}.assess_rubric_rag(transcription) AS raw
  )
  SELECT to_json(named_struct(
    'sentiment', COALESCE(try_parse_json((SELECT raw FROM sentiment)):sentiment::STRING, 'Unknown'),
    'sentiment_confidence', COALESCE(try_parse_json((SELECT raw FROM sentiment)):confidence::DOUBLE, 0.0),
    'topics', COALESCE(try_parse_json((SELECT raw FROM topics)):topics::STRING, '[]'),
    'intent', COALESCE(try_parse_json((SELECT raw FROM topics)):intent::STRING, 'Unknown'),
    'call_category', COALESCE((SELECT raw FROM category), 'Other'),
    'rubric_score', COALESCE(try_parse_json((SELECT raw FROM rubric)):overall_score::INT, 0),
    'rubric_assessment', COALESCE(try_parse_json((SELECT raw FROM rubric)):assessment::STRING, 'N/A'),
    'criterion_scores', COALESCE(try_parse_json((SELECT raw FROM rubric)):criterion_scores::STRING, '[]'),
    'improvement_areas', COALESCE(try_parse_json((SELECT raw FROM topics)):improvement_areas::STRING, '[]')
  ))
)
""")
print("Registered: enrich_single_call")

# COMMAND ----------

# DBTITLE 1,Verify All Registered Functions

spark.sql(f"USE CATALOG {CATALOG}")
funcs = spark.sql(f"SHOW USER FUNCTIONS IN {FQ}").collect()
print(f"\nAll UC Functions in {FQ}:")
for f in funcs:
    print(f"  - {f[0]}")

expected = {
    "find_audio_file", "find_all_audio_files", "read_audio_base64", "transcribe_audio",
    "classify_call_category", "analyze_call_sentiment", "extract_topics_and_intent",
    "assess_rubric_rag", "transcribe_and_save_to_silver", "process_all_audio_to_silver",
    "enrich_silver_to_gold", "enrich_single_call",
}
registered = {f[0].split(".")[-1] for f in funcs}
missing = expected - registered
if missing:
    print(f"\nWARNING: Missing functions: {missing}")
else:
    print(f"\nAll {len(expected)} functions registered successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All 12 UC functions registered (all pure SQL):
# MAGIC
# MAGIC | # | Function | Purpose |
# MAGIC |---|----------|---------|
# MAGIC | 1 | `find_audio_file` | Find specific audio file by speaker |
# MAGIC | 2 | `find_all_audio_files` | List all audio files in Volume |
# MAGIC | 3 | `read_audio_base64` | Read audio file as base64 for Whisper |
# MAGIC | 4 | `transcribe_audio` | Transcribe via `ai_query` + Whisper |
# MAGIC | 5 | `classify_call_category` | Classify call into Higher Ed categories |
# MAGIC | 6 | `analyze_call_sentiment` | Sentiment + confidence via LLM |
# MAGIC | 7 | `extract_topics_and_intent` | Extract topics, intent, improvement areas |
# MAGIC | 8 | `assess_rubric_rag` | RAG rubric assessment against advisor criteria |
# MAGIC | 9 | `transcribe_and_save_to_silver` | Transcribe single file (returns JSON) |
# MAGIC | 10 | `process_all_audio_to_silver` | Check transcription pipeline status |
# MAGIC | 11 | `enrich_silver_to_gold` | Check enrichment pipeline status |
# MAGIC | 12 | `enrich_single_call` | Full AI enrichment in one call |