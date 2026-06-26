# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # 03 — Batch Pipeline: Bronze → Silver → Gold → VS Index Sync
# MAGIC
# MAGIC This notebook populates the Delta tables that power dashboards, the VS index, and `03_test` data-quality checks.
# MAGIC The deployed agent handles **on-demand** single-call analysis; this pipeline handles **bulk** processing.
# MAGIC
# MAGIC | Phase | Source → Target | Key function |
# MAGIC |-------|----------------|----------------------------|
# MAGIC | 1 | Volume → `bronze_call_metadata` | `dbutils.fs.ls` + metadata |
# MAGIC | 2 | Bronze → `silver_transcriptions` | `transcribe_audio` (Whisper) |
# MAGIC | 3 | Silver → `gold_enriched_calls` | 4 AI UC functions in one SQL |
# MAGIC | 4 | Gold → VS index | `VectorSearchClient.sync()` |

# COMMAND ----------

# DBTITLE 1,Configuration
# -- Config: override via widgets or accept defaults from 02_deploy --
dbutils.widgets.text("catalog",       "yyang",            "Catalog")
dbutils.widgets.text("schema",        "contact_center_qa", "Schema")
dbutils.widgets.text("volume_path",   "/Volumes/chada_demos/pubsec_demos/audio/", "Audio Volume Path")
dbutils.widgets.text("vs_endpoint",   "one-env-shared-endpoint-10", "VS Endpoint")
dbutils.widgets.text("warehouse_id",  "8baced1ff014912d", "SQL Warehouse ID")

CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
FQ           = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH  = dbutils.widgets.get("volume_path").rstrip("/")
VS_ENDPOINT  = dbutils.widgets.get("vs_endpoint")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
VS_INDEX     = f"{FQ}.gold_enriched_calls_vs_index"

print(f"Catalog/Schema : {FQ}")
print(f"Volume         : {VOLUME_PATH}")
print(f"VS endpoint    : {VS_ENDPOINT}")
print(f"VS index       : {VS_INDEX}")

# COMMAND ----------

# DBTITLE 1,Phase 1 header
# MAGIC %md
# MAGIC ## Phase 1 — Bronze Ingestion
# MAGIC List `.wav` files in the Volume using `dbutils.fs.ls` (metadata-only, no binary reads)  
# MAGIC and INSERT new files into `bronze_call_metadata`.

# COMMAND ----------

# DBTITLE 1,Phase 1: Ingest audio metadata → bronze_call_metadata
import re, uuid, io, wave
from datetime import datetime

def normalize_volume_path(p: str) -> str:
    """dbutils returns dbfs:/Volumes/...; UC tables use /Volumes/..."""
    return re.sub(r'^dbfs:', '', p)

def get_wav_duration(file_path: str) -> int | None:
    """Return WAV duration in whole seconds by reading the file header.
    UC Volume paths (/Volumes/...) are directly accessible via Python open().
    Reads only the first 4 KB — sufficient for any standard WAV header.
    Returns None on any error (non-WAV, corrupt header, permission issue).
    """
    try:
        clean_path = re.sub(r'^dbfs:', '', file_path)
        with open(clean_path, 'rb') as f:
            header = f.read(4096)
        with wave.open(io.BytesIO(header), 'rb') as wf:
            frames = wf.getnframes()
            rate   = wf.getframerate()
            return int(frames / rate) if rate > 0 else None
    except Exception:
        return None

# -- List .wav files in the Volume (metadata only, no audio bytes loaded) --
try:
    raw_files = dbutils.fs.ls(VOLUME_PATH + "/")
except Exception:
    raw_files = dbutils.fs.ls(VOLUME_PATH)

wav_files = [
    (normalize_volume_path(f.path), f.name, f.size)
    for f in raw_files
    if f.name.lower().endswith(".wav")
]
print(f"Found {len(wav_files)} .wav files in {VOLUME_PATH}")

# -- Find already-ingested paths --
existing_paths = {
    r.file_path
    for r in spark.sql(f"SELECT file_path FROM {FQ}.bronze_call_metadata").collect()
}
print(f"Already in bronze: {len(existing_paths)}")

# -- Build rows for new files --
new_rows = []
for file_path, filename, size in wav_files:
    if file_path not in existing_paths:
        match = re.search(r'Speaker[_\s]*0*(\d+)', filename, re.IGNORECASE)
        agent_id = f"agent_{match.group(1).zfill(3)}" if match else "unknown"
        new_rows.append({
            "call_id":              str(uuid.uuid4()),
            "filename":             filename,
            "file_path":            file_path,
            "agent_id":             agent_id,
            "queue_type":           "Unknown",
            "call_duration_seconds": get_wav_duration(file_path),
            "call_timestamp":       datetime.now(),
            "file_size_bytes":      size,
            "ingested_at":          datetime.now(),
        })

print(f"New files to ingest: {len(new_rows)}")

if new_rows:
    bronze_schema = spark.table(f"{FQ}.bronze_call_metadata").schema
    bronze_df = spark.createDataFrame(new_rows, schema=bronze_schema)
    bronze_df.write.mode("append").saveAsTable(f"{FQ}.bronze_call_metadata")
    print(f"\u2705 Ingested {len(new_rows)} files into bronze_call_metadata")
else:
    print("\u2705 All files already in bronze — nothing to ingest")

bronze_total = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.bronze_call_metadata").collect()[0]["cnt"]
print(f"bronze_call_metadata total rows: {bronze_total}")

# COMMAND ----------

# DBTITLE 1,Backfill: call_duration_seconds for existing bronze rows
# One-time backfill: populate call_duration_seconds for bronze rows where it is NULL.
# get_wav_duration() reads the WAV header on the driver (UC Volume paths accessible via open()).
# Uses MERGE so only NULL rows are touched — safe to re-run.

null_rows = spark.sql(f"""
    SELECT call_id, file_path
    FROM {FQ}.bronze_call_metadata
    WHERE call_duration_seconds IS NULL
""").collect()

print(f"Rows with NULL call_duration_seconds: {len(null_rows)}")

if null_rows:
    updates = []
    for row in null_rows:
        dur = get_wav_duration(row.file_path)
        updates.append((row.call_id, row.file_path, dur))

    ok  = sum(1 for *_, d in updates if d is not None)
    bad = sum(1 for *_, d in updates if d is None)
    print(f"  Resolved: {ok}  |  Still NULL (non-WAV / error): {bad}")

    updates_df = spark.createDataFrame(updates, ["call_id", "file_path", "call_duration_seconds"])
    updates_df.createOrReplaceTempView("_dur_backfill")

    spark.sql(f"""
        MERGE INTO {FQ}.bronze_call_metadata AS t
        USING _dur_backfill AS s ON t.call_id = s.call_id
        WHEN MATCHED AND t.call_duration_seconds IS NULL
        THEN UPDATE SET t.call_duration_seconds = s.call_duration_seconds
    """)

    still_null = spark.sql(
        f"SELECT count(*) AS cnt FROM {FQ}.bronze_call_metadata WHERE call_duration_seconds IS NULL"
    ).collect()[0]["cnt"]
    print(f"\u2705 Backfill complete. Rows still NULL: {still_null}")

    display(spark.sql(f"""
        SELECT filename, agent_id, call_duration_seconds
        FROM {FQ}.bronze_call_metadata
        ORDER BY call_duration_seconds DESC
        LIMIT 15
    """))
else:
    print("\u2705 No NULL durations — nothing to backfill.")

# COMMAND ----------

# DBTITLE 1,Phase 2 header
# MAGIC %md
# MAGIC ## Phase 2 — Silver: Whisper Transcription
# MAGIC For each file in bronze not yet in silver, call `transcribe_audio` (Whisper large-v3 via `ai_query`)  
# MAGIC and INSERT the result with computed `word_count` into `silver_transcriptions`.
# MAGIC
# MAGIC > **Note**: This calls Whisper once per audio file. Each call takes ~5–60 s depending on audio length.  
# MAGIC > For large batches, increase `BATCH_LIMIT` or run multiple times.

# COMMAND ----------

# DBTITLE 1,Phase 2: Transcribe → silver_transcriptions
# -- How many files are pending? --
pending_silver = spark.sql(f"""
    SELECT b.call_id, b.filename, b.file_path, b.agent_id, b.call_duration_seconds
    FROM {FQ}.bronze_call_metadata b
    LEFT ANTI JOIN {FQ}.silver_transcriptions s ON b.file_path = s.file_path
    ORDER BY b.ingested_at
""").collect()

print(f"Pending transcription: {len(pending_silver)}")
for row in pending_silver:
    print(f"  - {row.filename}")

if not pending_silver:
    print("\u2705 All bronze files already transcribed to silver.")

# COMMAND ----------

# DBTITLE 1,Phase 2: Run transcription (SQL INSERT via UC function)
# Use a single SQL INSERT...WITH to call transcribe_audio for pending files.
# BATCH_LIMIT: process N files per run — increase or set to None for all at once.
# Rerun this cell to continue processing remaining files.
BATCH_LIMIT = None

if pending_silver:
    print(f"Transcribing {len(pending_silver)} file(s)... (this may take several minutes)")

    spark.sql(f"""
        INSERT INTO {FQ}.silver_transcriptions
        WITH pending AS (
            SELECT b.call_id, b.filename, b.file_path, b.agent_id, b.call_duration_seconds
            FROM   {FQ}.bronze_call_metadata b
            LEFT ANTI JOIN {FQ}.silver_transcriptions s ON b.file_path = s.file_path
            ORDER BY b.filename
            {'LIMIT ' + str(BATCH_LIMIT) if BATCH_LIMIT else ''}
        ),
        transcribed AS (
            SELECT
                call_id, filename, file_path, agent_id,
                {FQ}.transcribe_audio(file_path) AS transcription,
                call_duration_seconds
            FROM pending
        )
        SELECT
            call_id,
            filename,
            file_path,
            agent_id,
            transcription,
            CASE
                WHEN transcription IS NOT NULL
                THEN size(split(trim(transcription), '\\\\s+'))
                ELSE 0
            END AS word_count,
            call_duration_seconds,
            current_timestamp() AS transcribed_at
        FROM transcribed
        WHERE transcription IS NOT NULL
          AND length(trim(coalesce(transcription, ''))) > 5
    """)

    silver_total = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.silver_transcriptions").collect()[0]["cnt"]
    print(f"\u2705 Transcription complete. silver_transcriptions total rows: {silver_total}")

    # Preview
    display(spark.sql(f"""
        SELECT filename, agent_id,
               word_count,
               left(transcription, 120) AS transcription_preview,
               transcribed_at
        FROM {FQ}.silver_transcriptions
        ORDER BY transcribed_at DESC LIMIT 10
    """))

# COMMAND ----------

# DBTITLE 1,Phase 3 header
# MAGIC %md
# MAGIC ## Phase 3 — Gold: AI Enrichment
# MAGIC For each silver record not yet in gold, run 4 AI UC functions in **one SQL pass** per batch:
# MAGIC
# MAGIC | Function | Output columns |
# MAGIC |---|---|
# MAGIC | `analyze_call_sentiment` | `sentiment`, `sentiment_confidence` |
# MAGIC | `extract_topics_and_intent` | `topics` |
# MAGIC | `classify_call_category` | `call_category` |
# MAGIC | `assess_rubric_rag` | `overall_qa_score`, per-criterion scores, `compliance_flags`, `coaching_notes`, `requires_human_review` |
# MAGIC
# MAGIC Criterion names are fetched dynamically from `qa_rubric` so the SQL always matches the seeded rubric.

# COMMAND ----------

# DBTITLE 1,Phase 3: Build dynamic criterion-score expressions from qa_rubric
# -- Fetch criterion names in rubric_id order --
criteria_rows = spark.sql(f"""
    SELECT rubric_id, criterion FROM {FQ}.qa_rubric ORDER BY rubric_id
""").collect()

# Map rubric_id (1-5) -> gold column name
GOLD_CRITERION_COLS = [
    "greeting_score",    # rubric_id 1
    "empathy_score",     # rubric_id 2
    "accuracy_score",    # rubric_id 3
    "escalation_score",  # rubric_id 4
    "compliance_score",  # rubric_id 5
]

criterion_exprs = []
for row in criteria_rows:
    idx = row.rubric_id - 1  # 0-based
    if 0 <= idx < len(GOLD_CRITERION_COLS):
        col_name   = GOLD_CRITERION_COLS[idx]
        # Escape quotes in criterion name for safe embedding in SQL string literal
        crit_safe  = row.criterion.replace("'", "''").replace('"', '\\"')
        criterion_exprs.append(
            f"COALESCE(CAST(get_json_object(rubric_json, '$.criterion_scores[\"{crit_safe}\"]') AS INT), 0) AS {col_name}"
        )

# Pad missing columns with 0 if rubric has <5 rows
for i in range(len(criterion_exprs), len(GOLD_CRITERION_COLS)):
    criterion_exprs.append(f"0 AS {GOLD_CRITERION_COLS[i]}")

print("Criterion → column mapping:")
for row, expr in zip(criteria_rows, criterion_exprs):
    print(f"  {row.criterion!r:50s} -> {expr.split(' AS ')[-1]}")

# COMMAND ----------

# DBTITLE 1,Phase 3: Check pending silver → gold enrichment
pending_gold_count = spark.sql(f"""
    SELECT count(*) AS cnt
    FROM   {FQ}.silver_transcriptions s
    LEFT ANTI JOIN {FQ}.gold_enriched_calls g ON s.file_path = g.file_path
    WHERE  s.transcription IS NOT NULL
      AND  length(trim(s.transcription)) > 10
""").collect()[0]["cnt"]

print(f"Pending enrichment: {pending_gold_count} silver record(s)")

if pending_gold_count == 0:
    print("\u2705 All silver records already enriched to gold.")

# COMMAND ----------

# DBTITLE 1,Phase 3: Enrich silver → gold_enriched_calls (single SQL pass)
# BATCH_LIMIT: enrich N silver records per run — rerun to continue. None for run all.
BATCH_LIMIT = None

if pending_gold_count > 0:
    batch = min(BATCH_LIMIT, pending_gold_count) if BATCH_LIMIT else pending_gold_count
    print(f"Enriching {batch} of {pending_gold_count} pending record(s) (BATCH_LIMIT={BATCH_LIMIT})...")
    print("4 AI calls per record, ~30-120 s each")

    criterion_sql = ",\n        ".join(criterion_exprs)

    gold_insert_sql = f"""
    INSERT INTO {FQ}.gold_enriched_calls
    WITH pending AS (
        SELECT
            s.call_id, s.filename, s.file_path, s.agent_id, s.transcription,
            COALESCE(b.queue_type, 'Unknown') AS queue_type
        FROM   {FQ}.silver_transcriptions s
        LEFT JOIN  {FQ}.bronze_call_metadata b ON s.file_path = b.file_path
        LEFT ANTI JOIN {FQ}.gold_enriched_calls g ON s.file_path = g.file_path
        WHERE  s.transcription IS NOT NULL
          AND  length(trim(s.transcription)) > 10
        ORDER BY s.filename
        {'LIMIT ' + str(BATCH_LIMIT) if BATCH_LIMIT else ''}
    ),
    enriched AS (
        SELECT
            p.*,
            -- 4 AI UC function calls (parallelised by Databricks across records)
            {FQ}.analyze_call_sentiment(p.transcription)    AS sentiment_json,
            {FQ}.extract_topics_and_intent(p.transcription) AS topics_json,
            {FQ}.classify_call_category(p.transcription)    AS category_raw,
            {FQ}.assess_rubric_rag(p.transcription)         AS rubric_json
        FROM pending p
    )
    SELECT
        e.filename,
        e.file_path,
        e.call_id,
        e.agent_id,
        e.queue_type,
        e.transcription,
        -- Rubric overall score
        COALESCE(CAST(get_json_object(e.rubric_json, '$.overall_score') AS DOUBLE), 0.0) AS overall_qa_score,
        -- Per-criterion scores (dynamically mapped from qa_rubric order)
        {criterion_sql},
        -- Sentiment
        COALESCE(get_json_object(e.sentiment_json, '$.sentiment'),           'Unknown') AS sentiment,
        COALESCE(CAST(get_json_object(e.sentiment_json, '$.confidence') AS DOUBLE), 0.0) AS sentiment_confidence,
        -- Topics & category
        COALESCE(get_json_object(e.topics_json, '$.topics'), '[]')             AS topics,
        COALESCE(trim(e.category_raw), 'Other')                                AS call_category,
        -- Compliance & coaching
        COALESCE(get_json_object(e.rubric_json, '$.compliance_flags'), '[]')   AS compliance_flags,
        COALESCE(get_json_object(e.rubric_json, '$.coaching_notes'),   '')     AS coaching_notes,
        COALESCE(CAST(get_json_object(e.rubric_json, '$.requires_human_review') AS BOOLEAN), false) AS requires_human_review,
        current_timestamp() AS evaluated_at
    FROM enriched e
    """

    spark.sql(gold_insert_sql)

    gold_total = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.gold_enriched_calls").collect()[0]["cnt"]
    print(f"\u2705 Enrichment complete. gold_enriched_calls total rows: {gold_total}")

    # Preview
    display(spark.sql(f"""
        SELECT filename, agent_id, queue_type,
               overall_qa_score, sentiment, call_category, requires_human_review,
               left(coaching_notes, 100) AS coaching_preview,
               evaluated_at
        FROM {FQ}.gold_enriched_calls
        ORDER BY evaluated_at DESC LIMIT 10
    """))

# COMMAND ----------

# DBTITLE 1,Phase 4 header
# MAGIC %md
# MAGIC ## Phase 4 — VS Index Sync
# MAGIC Trigger a sync on `gold_enriched_calls_vs_index` so the Knowledge Assistant  
# MAGIC and any VS-powered queries reflect the newly inserted gold records.

# COMMAND ----------

# DBTITLE 1,Phase 4: Trigger VS index sync + poll to ONLINE
# Use the pre-installed Databricks SDK (no extra pip needed)
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def get_index_state(w, index_name):
    """Returns (state_str, indexed_row_count, message).
    SDK VectorIndexStatus exposes `ready` (bool), NOT `detailed_state`.
    We map ready=True -> 'ONLINE', False -> 'SYNCING'.
    """
    idx    = w.vector_search_indexes.get_index(index_name)
    status = idx.status
    if status is None:
        return "UNKNOWN", 0, ""
    ready = bool(getattr(status, 'ready', False))
    rows  = int(getattr(status, 'indexed_row_count', 0) or 0)
    msg   = str(getattr(status, 'message', '') or '')
    return ("ONLINE" if ready else "SYNCING"), rows, msg

state, rows, msg = get_index_state(w, VS_INDEX)
print(f"Before sync: state={state}, indexed_rows={rows}")
if msg:
    print(f"  {msg}")

# Trigger sync
print(f"\nTriggering sync on {VS_INDEX} ...")
w.vector_search_indexes.sync_index(VS_INDEX)
print("Sync triggered. Polling every 30 s (max 10 min)...")

for attempt in range(20):
    time.sleep(30)
    state, rows, msg = get_index_state(w, VS_INDEX)
    print(f"[{attempt+1:02d}] state={state:<12}  indexed_rows={rows}")
    if state == "ONLINE":
        print(f"\n\u2705 VS index ONLINE! indexed_rows={rows}")
        break
else:
    print(f"\u26a0\ufe0f  Still syncing after 10 min; last state={state}, rows={rows}")

# COMMAND ----------

idx    = w.vector_search_indexes.get_index(VS_INDEX)
status = idx.status
status

# COMMAND ----------

get_index_state(w, VS_INDEX)

# COMMAND ----------

# DBTITLE 1,Summary header
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Pipeline Summary
bronze_ct = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.bronze_call_metadata").collect()[0]["cnt"]
silver_ct = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.silver_transcriptions").collect()[0]["cnt"]
gold_ct   = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.gold_enriched_calls").collect()[0]["cnt"]

state, vs_rows, _ = get_index_state(w, VS_INDEX)

print("=" * 55)
print("  BATCH PIPELINE SUMMARY")
print("=" * 55)
print(f"  Bronze  (bronze_call_metadata)   : {bronze_ct:>6} rows")
print(f"  Silver  (silver_transcriptions)  : {silver_ct:>6} rows")
print(f"  Gold    (gold_enriched_calls)     : {gold_ct:>6} rows")
print(f"  VS idx  (gold_enriched_calls_idx) : {vs_rows:>6} indexed  [{state}]")
print("=" * 55)

if gold_ct > 0:
    # Quick QA spot-check
    qa = spark.sql(f"""
        SELECT
            round(avg(overall_qa_score), 2)                    AS avg_qa_score,
            count(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_calls,
            count(CASE WHEN requires_human_review THEN 1 END)  AS flagged_for_review,
            count(DISTINCT call_category)                      AS unique_categories
        FROM {FQ}.gold_enriched_calls
    """).collect()[0]
    print(f"  Avg QA score     : {qa['avg_qa_score']}")
    print(f"  Positive calls   : {qa['positive_calls']}")
    print(f"  Flagged review   : {qa['flagged_for_review']}")
    print(f"  Call categories  : {qa['unique_categories']}")
    print("=" * 55)

dbutils.notebook.exit(f"bronze={bronze_ct} silver={silver_ct} gold={gold_ct} vs_indexed={vs_rows} vs_state={state}")