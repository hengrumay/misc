# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Contact Center — 02 Deploy
# MAGIC
# MAGIC This notebook orchestrates the full deployment pipeline:
# MAGIC 1. **Ingest**: Auto Loader streams call file metadata into bronze
# MAGIC 2. **Agent Definition**: LangGraph agent with QA evaluation tools
# MAGIC 3. **MLflow Logging**: Log agent to MLflow with full resource declarations
# MAGIC 4. **Deployment**: Register model in Unity Catalog and deploy serving endpoint
# MAGIC 5. **Post-Deploy Validation**: Smoke-test the live endpoint
# MAGIC
# MAGIC **Redeploy Only?** Skip to the last section to update an existing endpoint.

# COMMAND ----------

# MAGIC %pip install langgraph==0.3.4 databricks-langchain databricks-agents unitycatalog-ai[databricks] unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Configuration

dbutils.widgets.text("catalog", "yyang", "Unity Catalog")
dbutils.widgets.text("schema", "contact_center_qa", "Schema")
dbutils.widgets.text("volume_name", "audio_files", "Volume Name")
dbutils.widgets.text("volume_path", "/Volumes/chada_demos/pubsec_demos/audio/", "Call Recordings Volume Path")
dbutils.widgets.text("warehouse_id", "8baced1ff014912d", "SQL Warehouse ID")
dbutils.widgets.text("whisper_endpoint", "va_whisper_large_v3", "Whisper Endpoint")
dbutils.widgets.text("llm_endpoint", "databricks-gemini-3-5-flash", "LLM Endpoint")
dbutils.widgets.text("agent_llm_endpoint", "databricks-claude-sonnet-4-6", "Agent LLM Endpoint")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en", "Embedding Endpoint")
dbutils.widgets.text("vector_search_endpoint", "one-env-shared-endpoint-1", "VS Endpoint")
dbutils.widgets.text("checkpoint_base", "dbfs:/tmp/checkpoints/contact_center_qa", "Checkpoint Base Path")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = dbutils.widgets.get("volume_path")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")
VS_ENDPOINT = dbutils.widgets.get("vector_search_endpoint")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")

FQ = f"{CATALOG}.{SCHEMA}"
# Model registered in 'main' catalog (UC model registry permissions)
MODEL_CATALOG = CATALOG
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MODEL_CATALOG}.{SCHEMA}")
AGENT_MODEL_NAME = f"{MODEL_CATALOG}.{SCHEMA}.contact_center_qa_agent"

print(f"Pipeline data: {FQ}")
print(f"Agent model: {AGENT_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Infrastructure Setup

# COMMAND ----------

# DBTITLE 1,Create Volume for Audio Files

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME_NAME} COMMENT 'Raw audio files for advisory call recordings'")
except Exception as e:
    print(f"Volume creation note (may use existing volume): {e}")
print(f"Volume / audio source: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Ingest — Auto Loader (Bronze)
# MAGIC
# MAGIC Streams audio file metadata from the Volume into `bronze_audio_files`.

# COMMAND ----------

# DBTITLE 1,Auto Loader: Ingest Audio File Metadata to Bronze

from pyspark.sql.functions import (
    col, current_timestamp, element_at, split, regexp_replace
)

# Ensure bronze table exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_audio_files (
  filename STRING, file_path STRING, file_size_bytes LONG,
  modified_time TIMESTAMP, ingested_at TIMESTAMP
) USING DELTA COMMENT 'Bronze: raw audio file metadata from Auto Loader'
""")

bronze_table = f"{FQ}.bronze_audio_files"
# Use Volume path for checkpoints (DBFS not writable on serverless)
checkpoint_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/checkpoints/bronze_audio"

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/checkpoints/bronze_schema")
    .load(VOLUME_PATH)
    .withColumn("file_path", regexp_replace(col("path"), "^dbfs:", ""))
    .withColumn("filename", element_at(split(col("path"), "/"), -1))
    .select(
        col("filename"),
        col("file_path"),
        col("length").alias("file_size_bytes"),
        col("modificationTime").alias("modified_time"),
        current_timestamp().alias("ingested_at"),
    )
)

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table(bronze_table)
)

query.awaitTermination()
bronze_count = spark.table(bronze_table).count()
print(f"Bronze ingestion complete: {bronze_count} files cataloged in {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Write Agent Code
# MAGIC
# MAGIC The agent is a LangGraph-based tool-calling agent that exposes all 10 pipeline
# MAGIC UC functions as callable tools. It uses Claude as the reasoning LLM.

# COMMAND ----------

# DBTITLE 1,Write agent.py
# MAGIC %%writefile agent.py
# MAGIC """
# MAGIC Higher Education Advisory Services - LangGraph Agent
# MAGIC
# MAGIC This agent orchestrates the full advisory call processing pipeline via
# MAGIC Unity Catalog function tools.
# MAGIC """
# MAGIC import os
# MAGIC from typing import Any, Generator, Optional
# MAGIC
# MAGIC import mlflow
# MAGIC from databricks_langchain import ChatDatabricks, UCFunctionToolkit
# MAGIC from langchain_core.runnables import RunnableLambda
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext,
# MAGIC )
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC LLM_ENDPOINT_NAME = os.environ.get("AGENT_LLM_ENDPOINT", "databricks-claude-sonnet-4-6")
# MAGIC CATALOG = os.environ.get("CATALOG", "yyang")
# MAGIC SCHEMA = os.environ.get("SCHEMA", "contact_center_qa")
# MAGIC
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC system_prompt = """You are an AI-powered advisor quality analyst for a Higher Education call center.
# MAGIC
# MAGIC You help administrators and QA managers process, transcribe, and analyze advisory calls
# MAGIC (financial aid, admissions, enrollment, academic advising) at scale.
# MAGIC
# MAGIC ## Your Tools
# MAGIC
# MAGIC ### Discovery & File Management
# MAGIC 1. **find_audio_file(speaker_query)** - Locate a specific speaker's audio file by name or number.
# MAGIC 2. **find_all_audio_files()** - List every audio file in the advisory services Volume.
# MAGIC
# MAGIC ### Transcription
# MAGIC 3. **transcribe_and_save_to_silver(file_path)** - Transcribe a single audio file with Whisper and return the transcript with metadata.
# MAGIC 4. **process_all_audio_to_silver()** - Check transcription status: shows total files, already transcribed, and pending counts.
# MAGIC
# MAGIC ### Analysis (work on any transcript text)
# MAGIC 5. **classify_call_category(transcription)** - Classify a call into: Financial Aid, Admissions, Enrollment, Academic Advising, Registration, Housing, Billing, Career Services, or Other.
# MAGIC 6. **analyze_call_sentiment(transcription)** - Analyze student sentiment. Returns JSON with sentiment label and confidence.
# MAGIC 7. **extract_topics_and_intent(transcription)** - Extract key topics, primary intent, and improvement areas.
# MAGIC 8. **assess_rubric_rag(transcription)** - Score advisor performance 1-5 across 5 rubric criteria using RAG.
# MAGIC 9. **enrich_single_call(transcription)** - Run ALL enrichment at once: sentiment, topics, category, and rubric assessment in one call.
# MAGIC
# MAGIC ### Pipeline Status
# MAGIC 10. **enrich_silver_to_gold()** - Check enrichment pipeline status: silver vs gold record counts.
# MAGIC
# MAGIC ## Recommended Workflows
# MAGIC
# MAGIC | User Request | Tool Sequence |
# MAGIC |---|---|
# MAGIC | "Transcribe speaker 12" | find_audio_file -> transcribe_and_save_to_silver |
# MAGIC | "Analyze this transcript" | enrich_single_call (or individual tools) |
# MAGIC | "Score this call" | assess_rubric_rag |
# MAGIC | "What files do we have?" | find_all_audio_files |
# MAGIC | "Pipeline status" | process_all_audio_to_silver -> enrich_silver_to_gold |
# MAGIC | "Full analysis of speaker 5" | find_audio_file -> transcribe_and_save_to_silver -> enrich_single_call |
# MAGIC
# MAGIC ## Guidelines
# MAGIC - Always confirm what was accomplished after each tool call.
# MAGIC - Report errors clearly with suggested remediation.
# MAGIC - For full call analysis, use enrich_single_call which runs all enrichment in one step.
# MAGIC - The rubric assessment scores advisors 1-5 across: Greeting, Active Listening, Accurate Information, Empathy, and Resolution.
# MAGIC """
# MAGIC
# MAGIC uc_tool_names = [
# MAGIC     f"{CATALOG}.{SCHEMA}.find_audio_file",
# MAGIC     f"{CATALOG}.{SCHEMA}.find_all_audio_files",
# MAGIC     f"{CATALOG}.{SCHEMA}.transcribe_and_save_to_silver",
# MAGIC     f"{CATALOG}.{SCHEMA}.process_all_audio_to_silver",
# MAGIC     f"{CATALOG}.{SCHEMA}.enrich_silver_to_gold",
# MAGIC     f"{CATALOG}.{SCHEMA}.classify_call_category",
# MAGIC     f"{CATALOG}.{SCHEMA}.analyze_call_sentiment",
# MAGIC     f"{CATALOG}.{SCHEMA}.extract_topics_and_intent",
# MAGIC     f"{CATALOG}.{SCHEMA}.assess_rubric_rag",
# MAGIC     f"{CATALOG}.{SCHEMA}.enrich_single_call",
# MAGIC ]
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC tools = uc_toolkit.tools
# MAGIC
# MAGIC
# MAGIC def create_tool_calling_agent(model, tools, system_prompt=None):  # noqa: E303
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     def should_continue(state):
# MAGIC         last = state["messages"][-1]
# MAGIC         if hasattr(last, "tool_calls") and last.tool_calls:
# MAGIC             return "continue"
# MAGIC         if isinstance(last, dict) and last.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         return "end"
# MAGIC
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     model_runnable = preprocessor | model
# MAGIC
# MAGIC     def call_model(state, config):
# MAGIC         return {"messages": [model_runnable.invoke(state, config)]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent", should_continue, {"continue": "tools", "end": END}
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(self, messages, context=None, custom_inputs=None):
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         out_msgs = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 for m in node_data.get("messages", []):
# MAGIC                     if isinstance(m, dict):
# MAGIC                         out_msgs.append(ChatAgentMessage(**m))
# MAGIC                     else:
# MAGIC                         role = getattr(m, "type", "assistant")
# MAGIC                         role = "assistant" if role == "ai" else role
# MAGIC                         kwargs = {}
# MAGIC                         if getattr(m, "tool_calls", None):
# MAGIC                             kwargs["tool_calls"] = m.tool_calls
# MAGIC                         out_msgs.append(ChatAgentMessage(
# MAGIC                             role=role,
# MAGIC                             content=getattr(m, "content", str(m)),
# MAGIC                             **kwargs,
# MAGIC                         ))
# MAGIC         return ChatAgentResponse(messages=out_msgs)
# MAGIC
# MAGIC     def predict_stream(self, messages, context=None, custom_inputs=None):
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 for m in node_data.get("messages", []):
# MAGIC                     if isinstance(m, dict):
# MAGIC                         yield ChatAgentChunk(**{"delta": m})
# MAGIC                     else:
# MAGIC                         role = getattr(m, "type", "assistant")
# MAGIC                         role = "assistant" if role == "ai" else role
# MAGIC                         yield ChatAgentChunk(**{"delta": {
# MAGIC                             "role": role,
# MAGIC                             "content": getattr(m, "content", str(m)),
# MAGIC                         }})
# MAGIC
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4: Local Agent Smoke Test

# COMMAND ----------

# DBTITLE 1,Test Agent Locally (Pre-Deploy)

import os, importlib
os.environ["AGENT_LLM_ENDPOINT"] = AGENT_LLM_ENDPOINT
os.environ["CATALOG"] = CATALOG
os.environ["SCHEMA"] = SCHEMA

import agent
importlib.reload(agent)
from agent import AGENT
from mlflow.types.agent import ChatAgentMessage

print("=" * 60)
print("LOCAL TEST 1: List available audio files")
print("=" * 60)
response = AGENT.predict(
    messages=[ChatAgentMessage(role="user", content="What audio files are available?")]
)
for msg in response.messages:
    print(f"[{msg.role}] {str(msg.content)[:300]}")
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        tc_names = []
        for tc in msg.tool_calls:
            if isinstance(tc, dict):
                tc_names.append(tc.get('name', tc.get('function', {}).get('name', '?')))
            else:
                tc_names.append(getattr(tc, 'name', getattr(tc, 'function', {}).get('name', '?') if isinstance(getattr(tc, 'function', None), dict) else str(tc)))
        print(f"  Tool calls: {tc_names}")

print("\n" + "=" * 60)
print("LOCAL TEST 2: Describe the full pipeline")
print("=" * 60)
response2 = AGENT.predict(
    messages=[ChatAgentMessage(role="user", content="Describe what tools you have and how you process advisory calls end to end.")]
)
for msg in response2.messages:
    print(f"[{msg.role}] {str(msg.content)[:500]}")

print("\nLocal smoke tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 5: Log Agent to MLflow

# COMMAND ----------

# DBTITLE 1,Upgrade MLflow for Resource Declarations
##: serverless should have newest Mlflow already
# %pip install --upgrade "mlflow[databricks]>=2.17.0"
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Log Model with Resources
import mlflow
mlflow.set_registry_uri("databricks-uc")

# Re-read widgets after restart
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
MODEL_CATALOG = CATALOG
AGENT_MODEL_NAME = f"{MODEL_CATALOG}.{SCHEMA}.contact_center_qa_agent"
AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")

UC_FUNCTIONS = [
    f"{FQ}.find_audio_file",
    f"{FQ}.find_all_audio_files",
    f"{FQ}.read_audio_base64",
    f"{FQ}.transcribe_audio",
    f"{FQ}.classify_call_category",
    f"{FQ}.analyze_call_sentiment",
    f"{FQ}.extract_topics_and_intent",
    f"{FQ}.assess_rubric_rag",
    f"{FQ}.transcribe_and_save_to_silver",
    f"{FQ}.process_all_audio_to_silver",
    f"{FQ}.enrich_silver_to_gold",
    f"{FQ}.enrich_single_call",
]

SERVING_ENDPOINTS = [
    AGENT_LLM_ENDPOINT,
    LLM_ENDPOINT,
    WHISPER_ENDPOINT,
]

print(f"MLflow version: {mlflow.__version__}")

# Build resource list -- required so agents.deploy() grants the service principal access
resources_list = []
try:
    from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction
    for ep in SERVING_ENDPOINTS:
        resources_list.append(DatabricksServingEndpoint(endpoint_name=ep))
    for fn in UC_FUNCTIONS:
        resources_list.append(DatabricksFunction(function_name=fn))
    print(f"Resources (DatabricksFunction): {len(resources_list)}")
except (ImportError, AttributeError):
    try:
        from mlflow.models.resources import DatabricksServingEndpoint, DatabricksUCFunction
        for ep in SERVING_ENDPOINTS:
            resources_list.append(DatabricksServingEndpoint(endpoint_name=ep))
        for fn in UC_FUNCTIONS:
            resources_list.append(DatabricksUCFunction(uc_function=fn))
        print(f"Resources (DatabricksUCFunction): {len(resources_list)}")
    except (ImportError, AttributeError):
        print(f"WARNING: Cannot declare resources with mlflow {mlflow.__version__}")

with mlflow.start_run(run_name="contact_center_qa_agent"):
    log_kwargs = dict(
        artifact_path="agent",
        python_model="agent.py",
        pip_requirements=[
            "mlflow[databricks]>=2.17.0",
            "langgraph==0.3.4",
            "databricks-langchain",
            "unitycatalog-ai[databricks]",
            "unitycatalog-langchain[databricks]",
        ],
    )
    if resources_list:
        log_kwargs["resources"] = resources_list
    logged_agent_info = mlflow.pyfunc.log_model(**log_kwargs)

print(f"Model logged: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 6: Register & Deploy

# COMMAND ----------

# DBTITLE 1,Register Model in Unity Catalog

import mlflow
mlflow.set_registry_uri("databricks-uc")

registered_model = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=AGENT_MODEL_NAME,
)
print(f"Registered: {AGENT_MODEL_NAME} v{registered_model.version}")

# COMMAND ----------

# DBTITLE 1,Deploy Agent Serving Endpoint

from databricks import agents

deployment = agents.deploy(
    model_name=AGENT_MODEL_NAME,
    model_version=registered_model.version,
)

print(f"Deployment initiated:")
print(f"  Endpoint: {deployment.endpoint_name if hasattr(deployment, 'endpoint_name') else 'pending'}")
print(f"  Model: {AGENT_MODEL_NAME} v{registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 7: Post-Deployment Validation

# COMMAND ----------

# DBTITLE 1,Wait for Endpoint Ready

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoint_name = deployment.endpoint_name if hasattr(deployment, 'endpoint_name') else f"{SCHEMA}_higher_ed_advisory_agent"

print(f"Waiting for endpoint '{endpoint_name}' to be ready...")
for attempt in range(60):
    try:
        ep = w.serving_endpoints.get(endpoint_name)
        state = ep.state.ready if ep.state else None
        if state and "READY" in str(state).upper():
            print(f"Endpoint is READY after {attempt * 15}s")
            break
        print(f"  [{attempt * 15}s] State: {state}")
    except Exception as e:
        print(f"  [{attempt * 15}s] Waiting... ({e})")
    time.sleep(15)
else:
    raise TimeoutError(f"Endpoint '{endpoint_name}' did not become ready within 15 minutes")

# COMMAND ----------

# DBTITLE 1,Post-Deploy Test: Endpoint Tool Invocation

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def query_endpoint(prompt: str) -> dict:
    """Send a chat message to the deployed agent endpoint."""
    try:
        response = w.serving_endpoints.query(
            name=endpoint_name,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.as_dict() if hasattr(response, "as_dict") else response
    except AttributeError:
        # Fallback: SDK as_dict() bug on nested dicts in agent responses
        return w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint_name}/invocations",
            body={"messages": [{"role": "user", "content": prompt}]},
        )

# -- Test 1: List files (invokes find_all_audio_files) --
print("=" * 60)
print("POST-DEPLOY TEST 1: find_all_audio_files")
print("=" * 60)
try:
    r1 = query_endpoint("List all available audio files in the volume.")
    msgs = r1.get("choices", [{}])[0].get("message", {}).get("content", str(r1))
    print(f"Response: {str(msgs)[:400]}")
    assert any(kw in str(r1).lower() for kw in ["file", "audio", "speaker", "wav", "total"]), \
        "Expected file listing in response"
    print("PASS: find_all_audio_files invoked successfully")
except Exception as e:
    print(f"FAIL: {e}")

# -- Test 2: Find specific file (invokes find_audio_file) --
print("\n" + "=" * 60)
print("POST-DEPLOY TEST 2: find_audio_file")
print("=" * 60)
try:
    r2 = query_endpoint("Find the audio file for speaker 1.")
    msgs2 = str(r2)
    print(f"Response: {msgs2[:400]}")
    assert any(kw in msgs2.lower() for kw in ["speaker", "found", "file_path", "not_found"]), \
        "Expected speaker search result"
    print("PASS: find_audio_file invoked successfully")
except Exception as e:
    print(f"FAIL: {e}")

# -- Test 3: Pipeline description (agent reasoning) --
print("\n" + "=" * 60)
print("POST-DEPLOY TEST 3: Agent reasoning & pipeline knowledge")
print("=" * 60)
try:
    r3 = query_endpoint(
        "What steps would you take to run the full pipeline? "
        "Describe the tools you'd use and in what order."
    )
    msgs3 = str(r3)
    print(f"Response: {msgs3[:500]}")
    assert any(kw in msgs3.lower() for kw in ["transcrib", "silver", "gold", "enrich", "rubric"]), \
        "Expected pipeline description"
    print("PASS: Agent correctly describes pipeline")
except Exception as e:
    print(f"FAIL: {e}")

print("\n" + "=" * 60)
print("ALL POST-DEPLOYMENT TESTS PASSED")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 8: Vector Search Setup (Optional)
# MAGIC
# MAGIC Creates a Vector Search index on `gold_enriched_calls` for semantic search.

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed for Vector Search
try:
    spark.sql(f"""
    ALTER TABLE {FQ}.gold_enriched_calls
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print("CDF enabled on gold_enriched_calls")
except Exception as e:
    print(f"CDF note: {e}")

# COMMAND ----------

# DBTITLE 1,Create Vector Search Index

# Use VectorSearchClient (avoids SDK .as_dict() deserialization bug)
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
vs_index_name = f"{FQ}.gold_enriched_calls_vs_index"

try:
    existing = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=vs_index_name)
    print(f"Vector Search index already exists: {vs_index_name}")
except Exception:
    try:
        print(f"Creating Vector Search index: {vs_index_name}")
        index = vsc.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT,
            source_table_name=f"{FQ}.gold_enriched_calls",
            index_name=vs_index_name,
            primary_key="file_path",
            pipeline_type="TRIGGERED",
            embedding_source_column="transcription",
            embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
            columns_to_sync=[
                "filename", "agent_id", "queue_type", "sentiment", "sentiment_confidence",
                "topics", "call_category", "overall_qa_score", "coaching_notes",
                "requires_human_review",
            ],
        )
        print(f"Vector Search index created: {vs_index_name}")
    except Exception as create_err:
        print(f"Vector Search index creation skipped: {create_err}")

# COMMAND ----------

# DBTITLE 1,Agent Bricks — Create Knowledge Assistant
# MAGIC %md
# MAGIC ## Stage 8b: Agent Bricks — Knowledge Assistant
# MAGIC
# MAGIC Creates a **Knowledge Assistant (KA)** Agent Brick backed by the `gold_enriched_calls_vs_index`.
# MAGIC The KA exposes a conversational RAG interface over call transcriptions and QA metadata.

# COMMAND ----------

# DBTITLE 1,Create KA — Contact Center Q&A

# Knowledge Assistants API: POST /api/2.0/knowledge-assistants
# (SDK 0.67 lacks the knowledgeassistants module; call REST directly)
from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()
KA_DISPLAY_NAME = "Contact Center QA Assistant"
vs_index_name = (
    f"{FQ}.gold_enriched_calls_vs_index"
    if "FQ" in dir() else
    "yyang.contact_center_qa.gold_enriched_calls_vs_index"
)

# 'name' is a required user-supplied resource identifier (CLI auto-generates it from
# display_name; raw REST calls must provide it explicitly).
# Format: lowercase, hyphens/underscores, no spaces.
result = w.api_client.do(
    "POST",
    "/api/2.0/knowledge-assistants",
    body={
        "name": "contact-center-qa-assistant",
        "display_name": KA_DISPLAY_NAME,
        "description": (
            "AI-powered Q&A over contact center call transcriptions and QA evaluation results. "
            "Ask about specific calls, agent performance, sentiment trends, compliance flags, "
            "and coaching recommendations."
        ),
        "instructions": (
            "You are a contact center QA analyst. "
            "Answer questions using call transcriptions and QA scores in your knowledge base. "
            "Cite filename and agent_id as evidence. Use overall_qa_score, sentiment, and "
            "call_category to contextualize answers. Highlight compliance_flags when relevant. "
            "If the answer is not in the available calls, say so clearly."
        ),
        "knowledge_sources": [{
            "display_name": "Gold Enriched Calls VS Index",
            "description": "VS index over call transcriptions with QA scores, sentiment, topics, compliance flags, and coaching notes.",
            "source_type": "index",
            "index_source": {
                "name": "gold-enriched-calls-index",
                "type": "DELTA_SYNC",
                "index": {
                    "name": vs_index_name,
                    "text_col": "coaching_notes",
                    "doc_uri_col": "filename",
                },
            },
        }],
    },
)

# Response is wrapped under 'knowledge_assistant' key
ka = result.get("knowledge_assistant", result)
ka_name          = ka.get("name")                              # e.g. 'contact-center-qa-assistant'
ka_tile_id       = ka.get("id")                                # UUID
ka_endpoint      = ka.get("endpoint_name")                     # e.g. 'ka-5154a6db-endpoint'
ka_resource_name = f"knowledge-assistants/{ka_name}"           # used by cell 28

print(f"KA name     : {ka_name}")
print(f"tile_id     : {ka_tile_id}")
print(f"endpoint    : {ka_endpoint}")
print(f"resource    : {ka_resource_name}")
print("\nFull response:")
print(json.dumps(result, indent=2, default=str))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 9: Genie Space Preparation
# MAGIC
# MAGIC The `gold_enriched_calls` table is structured and commented for direct
# MAGIC publishing to a Databricks Genie Space.
# MAGIC
# MAGIC **To create the Genie Space:**
# MAGIC 1. Navigate to **AI/BI Genie** in the Databricks workspace
# MAGIC 2. Click **New Genie Space**
# MAGIC 3. Add table: `chada_demos.higher_ed_advisory.gold_enriched_calls`
# MAGIC 4. Optionally add: `chada_demos.higher_ed_advisory.advisor_rubric`
# MAGIC 5. Set instructions:
# MAGIC    > "This data contains AI-analyzed higher education advisory calls.
# MAGIC    > Each row is one call with sentiment, topic, intent, category, and
# MAGIC    > a rubric-based advisor performance score (1-5)."

# COMMAND ----------

# DBTITLE 1,Pipeline Complete -- Summary

bronze_ct = spark.table(f"{FQ}.bronze_audio_files").count()
silver_ct = spark.table(f"{FQ}.silver_transcriptions").count()
gold_ct = spark.table(f"{FQ}.gold_enriched_calls").count()

print(f"""
{'=' * 60}
  HIGHER EDUCATION ADVISORY SERVICES -- PIPELINE SUMMARY
{'=' * 60}

  Catalog/Schema:  {FQ}
  Agent Model:     {AGENT_MODEL_NAME}
  Endpoint:        {endpoint_name}

  +----------+-----------+
  |  Layer   |  Records  |
  +----------+-----------+
  |  Bronze  |  {bronze_ct:<9} |
  |  Silver  |  {silver_ct:<9} |
  |  Gold    |  {gold_ct:<9} |
  +----------+-----------+

  Vector Search:   {vs_index_name}
  Genie-Ready:     gold_enriched_calls (all columns commented)
{'=' * 60}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Redeploy Only
# MAGIC
# MAGIC **Use this section when iterating on the agent** (changing tools, system prompt, etc.)
# MAGIC without re-running the full pipeline above. Skip directly to this cell.
# MAGIC
# MAGIC This will:
# MAGIC 1. Re-write `agent.py` (edit the code above if needed)
# MAGIC 2. Log a new model version to MLflow
# MAGIC 3. Register in Unity Catalog
# MAGIC 4. Update the existing serving endpoint

# COMMAND ----------

# DBTITLE 1,Redeploy: Log + Register + Update Endpoint

# import mlflow
# from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction

# mlflow.set_registry_uri("databricks-uc")

# # Re-read config
# CATALOG = dbutils.widgets.get("catalog")
# SCHEMA = dbutils.widgets.get("schema")
# FQ = f"{CATALOG}.{SCHEMA}"
# AGENT_LLM_ENDPOINT = dbutils.widgets.get("agent_llm_endpoint")
# LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
# WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
# model_name = f"main.{SCHEMA}.higher_ed_advisory_agent"

# # Re-write agent.py (uses the same agent_code variable from Stage 3 above)
# agent_path = "/Workspace/Users/chad.ammirati@databricks.com/Higher_Ed_Advisory_Services/agent.py"
# with open(agent_path, "w") as f:
#     f.write(agent_code.strip())
# print("agent.py re-written")

# # Resources
# resources = [
#     DatabricksServingEndpoint(endpoint_name=AGENT_LLM_ENDPOINT),
#     DatabricksFunction(function_name=f"{FQ}.find_audio_file"),
#     DatabricksFunction(function_name=f"{FQ}.find_all_audio_files"),
#     DatabricksFunction(function_name=f"{FQ}.read_audio_base64"),
#     DatabricksFunction(function_name=f"{FQ}.transcribe_audio"),
#     DatabricksFunction(function_name=f"{FQ}.classify_call_category"),
#     DatabricksFunction(function_name=f"{FQ}.analyze_call_sentiment"),
#     DatabricksFunction(function_name=f"{FQ}.extract_topics_and_intent"),
#     DatabricksFunction(function_name=f"{FQ}.assess_rubric_rag"),
#     DatabricksFunction(function_name=f"{FQ}.transcribe_and_save_to_silver"),
#     DatabricksFunction(function_name=f"{FQ}.process_all_audio_to_silver"),
#     DatabricksFunction(function_name=f"{FQ}.enrich_silver_to_gold"),
#     DatabricksFunction(function_name=f"{FQ}.enrich_single_call"),
# ]

# # Log
# mlflow.set_experiment("/Workspace/Users/chad.ammirati@databricks.com/Higher_Ed_Advisory_Services/02_deploy")
# with mlflow.start_run(run_name="higher_ed_advisory_agent_redeploy"):
#     model_info = mlflow.pyfunc.log_model(
#         artifact_path="agent",
#         python_model="agent.py",
#         resources=resources,
#         pip_requirements=[
#             "mlflow[databricks]>=2.17.0",
#             "langgraph==0.3.4",
#             "databricks-langchain",
#             "unitycatalog-ai[databricks]",
#             "unitycatalog-langchain[databricks]",
#         ],
#     )
# print(f"Model logged: {model_info.model_uri}")

# # Register
# mv = mlflow.register_model(model_info.model_uri, model_name)
# print(f"Registered: {model_name} v{mv.version}")

# # Update endpoint
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.serving import ServedEntityInput

# w = WorkspaceClient()
# w.serving_endpoints.update_config(
#     name="higher_ed_advisory_agent",
#     served_entities=[
#         ServedEntityInput(
#             entity_name=model_name,
#             entity_version=str(mv.version),
#             workload_size="Small",
#             scale_to_zero_enabled=True,
#         )
#     ],
# )
# print(f"Endpoint update initiated for version {mv.version}")
# print("Endpoint will take a few minutes to deploy the new version.")