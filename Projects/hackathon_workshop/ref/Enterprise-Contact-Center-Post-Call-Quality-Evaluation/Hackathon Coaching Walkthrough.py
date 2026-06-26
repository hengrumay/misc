# Databricks notebook source
# DBTITLE 1,Hackathon Coaching: Enterprise Contact Center Post-Call QA
# MAGIC %md
# MAGIC # 🎓 Hackathon Coaching Walkthrough
# MAGIC ## Enterprise Contact Center — Post-Call Quality Evaluation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Welcome, Hackers! 👋
# MAGIC
# MAGIC This document walks you through building an **AI-powered post-call quality evaluation system** on Databricks — from zero to a deployed agent in ~30 minutes.
# MAGIC
# MAGIC **What you'll build:**
# MAGIC - A medallion-architecture data pipeline (Bronze → Silver → Gold)
# MAGIC - 12 Unity Catalog SQL functions powered by AI (`ai_query()`)
# MAGIC - A LangGraph agent that orchestrates transcription + analysis
# MAGIC - A deployed Model Serving endpoint (REST API)
# MAGIC - A QA dashboard + Genie Space for business users
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏗️ Architecture at a Glance
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────────────────┐
# MAGIC │                      DATA FLOW (Medallion)                             │
# MAGIC │                                                                        │
# MAGIC │  Audio Files (.wav)  ──►  UC Volume (/Volumes/.../audio_files/)        │
# MAGIC │        │                                                               │
# MAGIC │        ▼                                                               │
# MAGIC │  ┌──────────┐   Auto Loader streams file metadata                      │
# MAGIC │  │  BRONZE  │   (filename, file_path, file_size, ingested_at)          │
# MAGIC │  └────┬─────┘                                                          │
# MAGIC │        ▼                                                               │
# MAGIC │  ┌──────────┐   Whisper large-v3 Speech-to-Text via ai_query()         │
# MAGIC │  │  SILVER  │   (transcription, word_count, speaker_id, duration)      │
# MAGIC │  └────┬─────┘                                                          │
# MAGIC │        ▼                                                               │
# MAGIC │  ┌──────────┐   LLM Enrichment: sentiment, topics, rubric scoring      │
# MAGIC │  │   GOLD   │   (qa_score 1-5, compliance flags, coaching notes)       │
# MAGIC │  └────┬─────┘                                                          │
# MAGIC │        │                                                               │
# MAGIC │        ├───► QA Scoring Dashboard (supervisors)                        │
# MAGIC │        ├───► AI Agent Endpoint (natural language queries)               │
# MAGIC │        ├───► Genie Space (business analysts)                           │
# MAGIC │        └───► Reusable AI Skills (cross-team)                           │
# MAGIC └────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Prerequisites Checklist
# MAGIC
# MAGIC | Requirement | Details |
# MAGIC |---|---|
# MAGIC | **Workspace** | Unity Catalog enabled, serverless compute |
# MAGIC | **SQL Warehouse** | For `ai_query()` execution |
# MAGIC | **Whisper Endpoint** | `va_whisper_large_v3` (from Marketplace → GPU serving) |
# MAGIC | **LLM Endpoint** | `databricks-claude-sonnet-4-6` (agent reasoning) |
# MAGIC | **Analysis LLM** | `databricks-gemini-3-5-flash` or `databricks-meta-llama-3-3-70b-instruct` |
# MAGIC | **Audio Files** | `.wav` files in a UC Volume |
# MAGIC | **Vector Search** | Shared endpoint (e.g., `one-env-shared-endpoint-10`) |

# COMMAND ----------

# DBTITLE 1,Architecture Diagram (Visual)
# MAGIC %md
# MAGIC ### Architecture Workflow Diagram
# MAGIC
# MAGIC ![hackathon_architecture.png](./hackathon_architecture.png)

# COMMAND ----------

# DBTITLE 1,Step 1: Setup (01_setup) — Foundation
# MAGIC %md
# MAGIC ---
# MAGIC ## 🚀 Step 1: Setup (`01_setup`) — Build the Foundation
# MAGIC **⏱️ Time: ~3 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Step Does
# MAGIC
# MAGIC Creates all infrastructure: catalog, schema, Delta tables, QA rubric seed data, and 12 UC SQL functions.
# MAGIC
# MAGIC ### 💡 Key Concepts to Explain
# MAGIC
# MAGIC #### 1. Unity Catalog as the Backbone
# MAGIC - **Catalog** = top-level namespace (e.g., `yyang`)
# MAGIC - **Schema** = logical grouping (e.g., `contact_center_qa`)
# MAGIC - **Everything lives in UC**: tables, functions, volumes, models → unified governance
# MAGIC
# MAGIC #### 2. Medallion Architecture
# MAGIC | Layer | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_audio_files` | Raw file metadata from Auto Loader |
# MAGIC | Silver | `silver_transcriptions` | Whisper transcriptions + derived fields |
# MAGIC | Gold | `gold_enriched_calls` | LLM-enriched: sentiment, QA scores, topics |
# MAGIC | Reference | `advisor_rubric` | 5-criterion weighted scoring checklist |
# MAGIC
# MAGIC #### 3. The QA Rubric (Scoring Framework)
# MAGIC The rubric drives the entire quality evaluation. Each criterion has:
# MAGIC - Score 1 description (poor)
# MAGIC - Score 3 description (adequate)
# MAGIC - Score 5 description (excellent)
# MAGIC - A **weight** (all weights sum to 1.0)
# MAGIC
# MAGIC | Criterion | Weight | What It Measures |
# MAGIC |---|---|---|
# MAGIC | Proper Greeting & ID Verification | 15% | Professional opening + identity check |
# MAGIC | Empathy & Active Listening | 25% | Emotional intelligence + attentiveness |
# MAGIC | Accurate Information Delivery | 25% | Correctness of guidance provided |
# MAGIC | Resolution & Next Steps | 20% | Clear outcome + action items |
# MAGIC | Compliance & Documentation | 15% | Regulatory adherence + notes |
# MAGIC
# MAGIC #### 4. UC SQL Functions (The Agent's Tools)
# MAGIC All 12 functions are **pure SQL** — no Python UDFs. This means they work everywhere: notebooks, SQL warehouses, model serving, Genie spaces.
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │  UC Functions (12 total) — All Pure SQL              │
# MAGIC ├───────────────────────────────────────────────────────────┤
# MAGIC │  DISCOVERY                                            │
# MAGIC │   • find_audio_file(speaker_query)                    │
# MAGIC │   • find_all_audio_files()                            │
# MAGIC ├───────────────────────────────────────────────────────────┤
# MAGIC │  TRANSCRIPTION                                        │
# MAGIC │   • read_audio_base64(file_path)                      │
# MAGIC │   • transcribe_audio(file_path)                       │
# MAGIC │   • transcribe_and_save_to_silver(file_path)           │
# MAGIC │   • process_all_audio_to_silver()                     │
# MAGIC ├───────────────────────────────────────────────────────────┤
# MAGIC │  ANALYSIS (LLM-powered via ai_query)                  │
# MAGIC │   • classify_call_category(transcription)             │
# MAGIC │   • analyze_call_sentiment(transcription)             │
# MAGIC │   • extract_topics_and_intent(transcription)          │
# MAGIC │   • assess_rubric_rag(transcription)                  │
# MAGIC │   • enrich_single_call(transcription)                 │
# MAGIC ├───────────────────────────────────────────────────────────┤
# MAGIC │  PIPELINE ORCHESTRATION                               │
# MAGIC │   • enrich_silver_to_gold()                           │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 🔑 Pattern to Highlight: `ai_query()` in SQL Functions
# MAGIC
# MAGIC This is the **"aha!" moment** for the audience. Show how a UC function calls an LLM:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION catalog.schema.analyze_call_sentiment(transcription STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN (
# MAGIC   SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT('Analyze the sentiment...\n\nTranscript:\n', transcription)
# MAGIC   )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Why this matters:**
# MAGIC - No Python server needed — LLM inference embedded in SQL
# MAGIC - Governed by UC permissions (who can call it)
# MAGIC - Reusable anywhere — notebooks, dashboards, agents, other functions
# MAGIC - Audit trail built-in via Unity Catalog
# MAGIC
# MAGIC ### ⚠️ Common Pitfall
# MAGIC > **Whisper endpoint must be running** before `transcribe_audio` works.
# MAGIC > Deploy from Marketplace → Models → `whisper-large-v3` → Create Serving Endpoint (GPU).

# COMMAND ----------

# DBTITLE 1,Step 2: Deploy (02_deploy) — Ingest + Agent + Serve
# MAGIC %md
# MAGIC ---
# MAGIC ## 🚀 Step 2: Deploy (`02_deploy`) — Ingest, Build Agent, Deploy
# MAGIC **⏱️ Time: ~15 minutes** (mostly waiting for endpoint to warm up)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Step Does
# MAGIC
# MAGIC 1. **Auto Loader** → streams audio file metadata into bronze
# MAGIC 2. **Write agent code** (`agent.py`) → LangGraph tool-calling agent
# MAGIC 3. **Local smoke test** → verify agent works before deploying
# MAGIC 4. **MLflow log** → package agent with resource dependencies
# MAGIC 5. **Register + Deploy** → UC model + serving endpoint
# MAGIC 6. **Post-deploy validation** → test the live endpoint
# MAGIC 7. **(Optional) Vector Search** → semantic search over gold data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Concepts to Explain
# MAGIC
# MAGIC #### Stage 2A: Auto Loader (Bronze Ingestion)
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC   .format("cloudFiles")
# MAGIC   .option("cloudFiles.format", "binaryFile")
# MAGIC   .load(volume_path)
# MAGIC   .select(filename, file_path, file_size, modified_time, ingested_at)
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .toTable("bronze_audio_files")
# MAGIC ```
# MAGIC
# MAGIC **Coaching points:**
# MAGIC - Auto Loader incrementally picks up new files → no re-processing
# MAGIC - `binaryFile` format reads metadata without loading audio content
# MAGIC - Checkpoint stored in a Volume (serverless-compatible)
# MAGIC - `.trigger(availableNow=True)` for batch-style one-shot processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Stage 2B: The LangGraph Agent (`agent.py`)
# MAGIC
# MAGIC This is the **core innovation**. Walk through the architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │         LangGraph Agent Loop                       │
# MAGIC │                                                    │
# MAGIC │  User Message                                      │
# MAGIC │       │                                            │
# MAGIC │       ▼                                            │
# MAGIC │  ┌────────────────┐                                │
# MAGIC │  │  Agent Node    │  Claude Sonnet 4 (reasoning)   │
# MAGIC │  │  (call_model)  │  + system prompt + tools        │
# MAGIC │  └─────┬──────────┘                                │
# MAGIC │        │                                           │
# MAGIC │   has tool_calls?                                   │
# MAGIC │     │        │                                      │
# MAGIC │     yes      no ───► END (return response)         │
# MAGIC │     │                                               │
# MAGIC │     ▼                                               │
# MAGIC │  ┌────────────────┐                                │
# MAGIC │  │  Tools Node    │  Executes UC functions          │
# MAGIC │  │  (10 UC tools) │  via UCFunctionToolkit          │
# MAGIC │  └─────┬──────────┘                                │
# MAGIC │        │                                           │
# MAGIC │        └──────► back to Agent Node (loop)          │
# MAGIC └──────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key components of `agent.py`:**
# MAGIC
# MAGIC | Component | Library | Purpose |
# MAGIC |---|---|---|
# MAGIC | `ChatDatabricks` | `databricks-langchain` | Connect to Foundation Model endpoint |
# MAGIC | `UCFunctionToolkit` | `databricks-langchain` | Wrap UC functions as LangChain tools |
# MAGIC | `StateGraph` | `langgraph` | Define agent ↔ tools control flow |
# MAGIC | `ChatAgentToolNode` | `mlflow` | MLflow-compatible tool execution node |
# MAGIC | `LangGraphChatAgent` | Custom class | Wraps graph as `mlflow.pyfunc.ChatAgent` |
# MAGIC
# MAGIC **The system prompt** is crucial — it tells the LLM:
# MAGIC - What tools are available and what they do
# MAGIC - Recommended tool sequences for common requests
# MAGIC - How to interpret results and report back
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Stage 2C: MLflow Logging & Resource Declarations
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run():
# MAGIC     logged_agent_info = mlflow.langchain.log_model(
# MAGIC         lc_model="agent.py",
# MAGIC         artifact_path="agent",
# MAGIC         resources=[
# MAGIC             DatabricksFunction(function_name=f"{FQ}.find_audio_file"),
# MAGIC             DatabricksServingEndpoint(endpoint_name="databricks-claude-sonnet-4-6"),
# MAGIC             DatabricksServingEndpoint(endpoint_name="va_whisper_large_v3"),
# MAGIC             DatabricksTable(table_name=f"{FQ}.gold_enriched_calls"),
# MAGIC             # ... all resources declared
# MAGIC         ],
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Why resource declarations matter:**
# MAGIC - Tells Databricks what permissions the agent needs at serving time
# MAGIC - Auto-provisions service principal access
# MAGIC - Creates lineage tracking (which model uses which tables/endpoints)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Stage 2D: Deploy to Model Serving
# MAGIC
# MAGIC ```python
# MAGIC from databricks import agents
# MAGIC deployment = agents.deploy(
# MAGIC     model_name="catalog.schema.contact_center_qa_agent",
# MAGIC     model_version=1,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **One line to deploy!** This:
# MAGIC - Creates a serving endpoint with scale-to-zero
# MAGIC - Configures auth + networking
# MAGIC - Enables the AI Playground integration
# MAGIC - Sets up MLflow tracing automatically
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎬 Live Demo Moment
# MAGIC
# MAGIC After deployment, show the agent responding in **AI Playground**:
# MAGIC
# MAGIC > **User:** "Find and transcribe speaker 5, then run a full quality analysis."
# MAGIC >
# MAGIC > **Agent:** Calls `find_audio_file` → `transcribe_and_save_to_silver` → `enrich_single_call`
# MAGIC > Reports back with sentiment, topics, rubric score, and coaching recommendations.
# MAGIC
# MAGIC ### ⚠️ Common Pitfall
# MAGIC > Endpoint takes **5-10 minutes** to become READY. Use the wait loop!
# MAGIC > Also: if you hit `ResourceDoesNotExist`, the model version may not be registered yet.

# COMMAND ----------

# DBTITLE 1,Step 3: Test (03_test) — Validation
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Step 3: Test (`03_test`) — End-to-End Validation
# MAGIC **⏱️ Time: ~5 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Step Does
# MAGIC
# MAGIC Two-phase test suite ensuring everything works:
# MAGIC
# MAGIC #### Phase 1: Pre-Deployment (Tests 1–9) — No endpoint needed
# MAGIC | Test | What It Validates |
# MAGIC |---|---|
# MAGIC | 1. Schema Validation | All Delta tables have correct columns & types |
# MAGIC | 2. Rubric Integrity | Weights sum to 1.0, no null criteria |
# MAGIC | 3. UC Function Registration | All 12 functions exist in catalog |
# MAGIC | 4. Mock Bronze Ingestion | Schema compatibility with simulated data |
# MAGIC | 5. Mock Silver Transformation | Word count, duration hints, speaker extraction |
# MAGIC | 6. Mock Gold Enrichment | AI response parsing, schema fit |
# MAGIC | 7. Agent Tool Wiring | `UCFunctionToolkit` loads all 10 agent tools |
# MAGIC | 8. Direct SQL Function Tests | Smoke-test `classify_call_category`, `analyze_call_sentiment`, `extract_topics_and_intent` |
# MAGIC | 9. Data Lineage | Bronze ≥ Silver ≥ Gold row counts, no nulls in required columns |
# MAGIC
# MAGIC #### Phase 2: Post-Deployment (Tests 10–12) — Requires live endpoint
# MAGIC | Test | What It Validates |
# MAGIC |---|---|
# MAGIC | 10. Endpoint Health | HTTP 200 from serving endpoint |
# MAGIC | 11. Tool Invocation | Agent correctly calls UC functions |
# MAGIC | 12. Gold Data Quality | No nulls in critical enrichment columns |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Coaching Points
# MAGIC
# MAGIC **Why test at every layer?**
# MAGIC - Schema drift breaks downstream consumers silently
# MAGIC - AI functions can return unexpected formats → mock parsing catches this
# MAGIC - Agent tool wiring is fragile → a renamed function breaks the agent
# MAGIC - Post-deploy tests catch serving configuration issues
# MAGIC
# MAGIC **Pattern: `record_test()` helper**
# MAGIC ```python
# MAGIC def record_test(name, passed, detail=""):
# MAGIC     status = "PASS" if passed else "FAIL"
# MAGIC     test_results.append({"test": name, "status": status, "detail": detail})
# MAGIC ```
# MAGIC Collects all results for a final summary report. Great pattern for hackathon demos — one cell shows green/red for everything.
# MAGIC
# MAGIC ### ⚠️ Common Pitfall
# MAGIC > Set the `endpoint_name` widget to enable post-deploy tests.
# MAGIC > Without it, tests 10-12 are skipped silently.

# COMMAND ----------

# DBTITLE 1,Step 4: Dashboard (04_dashboard) — Supervisor View
# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Step 4: Dashboard (`04_dashboard`) — Supervisor View
# MAGIC **⏱️ Time: ~5 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Step Does
# MAGIC
# MAGIC Creates a supervisor-facing QA dashboard with parameterized SQL queries that power:
# MAGIC
# MAGIC | Widget | Business Question |
# MAGIC |---|---|
# MAGIC | **KPIs Summary** | Overall average QA score, % flagged for review |
# MAGIC | **Agent Leaderboard** | Ranked agents by avg score, with per-criterion breakdown |
# MAGIC | **Outlier Calls** | Calls requiring human review (sorted by severity) |
# MAGIC | **Queue Performance** | Which departments perform best/worst |
# MAGIC | **Coaching Opportunities** | Agents + specific criteria below threshold |
# MAGIC | **Trends** | Daily score distribution and volume |
# MAGIC | **Sentiment × Category** | Heatmap of sentiment vs. call type |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Coaching Points
# MAGIC
# MAGIC **Parameterized SQL with widgets:**
# MAGIC ```sql
# MAGIC SELECT * FROM ${catalog}.${schema}.gold_qa_evaluations
# MAGIC ```
# MAGIC This makes the notebook portable — same queries work across different catalogs/schemas.
# MAGIC
# MAGIC **Coaching opportunity detection pattern:**
# MAGIC ```sql
# MAGIC WITH agent_criterion_scores AS (
# MAGIC   SELECT agent_id, 'Empathy' AS criterion, AVG(empathy_score) AS avg_score
# MAGIC   FROM gold_qa_evaluations
# MAGIC   GROUP BY agent_id
# MAGIC   -- UNION ALL for each criterion...
# MAGIC )
# MAGIC SELECT * WHERE avg_score < 3.5
# MAGIC ORDER BY avg_score ASC
# MAGIC ```
# MAGIC
# MAGIC **Business value pitch:**
# MAGIC - Supervisors see at-a-glance which agents need help
# MAGIC - No manual call listening needed → AI evaluates 100% of calls
# MAGIC - Compliance flags ensure regulatory issues surface immediately

# COMMAND ----------

# DBTITLE 1,Step 5: Genie & AI Skills (05_genie_ai_skill)
# MAGIC %md
# MAGIC ---
# MAGIC ## 🧞 Step 5: Genie Space & AI Skills (`05_genie_ai_skill`)
# MAGIC **⏱️ Time: ~3 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Step Does
# MAGIC
# MAGIC 1. **Registers reusable AI Skills** — UC functions anyone can call
# MAGIC 2. **Creates a Genie Space** — natural language interface for business analysts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Concepts
# MAGIC
# MAGIC #### AI Skills = Reusable UC Functions
# MAGIC
# MAGIC | Skill | What It Does | Who Uses It |
# MAGIC |---|---|---|
# MAGIC | `ai_skill_sentiment_analysis(text)` | Sentiment on any text (not just calls) | Any team |
# MAGIC | `ai_skill_qa_scorer(transcript)` | Full rubric scoring on any interaction | QA teams |
# MAGIC
# MAGIC **Why this is powerful:**
# MAGIC - Written once, used everywhere (SQL, Python, dashboards, agents)
# MAGIC - Governed by UC → track who calls it, how often
# MAGIC - Can be composed into pipelines or other functions
# MAGIC - Cross-team: marketing can use sentiment on social media, support on tickets
# MAGIC
# MAGIC #### Genie Space = No-Code Analytics
# MAGIC
# MAGIC A Genie Space lets business analysts ask questions in plain English:
# MAGIC
# MAGIC > 🗣️ "What's the average QA score for the billing queue this month?"
# MAGIC >
# MAGIC > 🗣️ "Which agents have empathy scores below 3?"
# MAGIC >
# MAGIC > 🗣️ "Show me the trend of compliance flags over the last 30 days"
# MAGIC
# MAGIC **Tables exposed to Genie:**
# MAGIC - `gold_qa_evaluations` — full evaluation results
# MAGIC - `qa_rubric` — scoring criteria reference
# MAGIC - `silver_transcriptions` — raw transcripts for drill-down
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎬 Demo Script for Genie
# MAGIC
# MAGIC 1. Open Genie in sidebar
# MAGIC 2. Select the Contact Center QA space
# MAGIC 3. Ask: "Who are our top 3 agents by QA score?"
# MAGIC 4. Ask: "Which agents need coaching on empathy?"
# MAGIC 5. Ask: "What percentage of calls are flagged for human review?"
# MAGIC
# MAGIC This shows the audience how the **same gold data** serves multiple personas without code.

# COMMAND ----------

# DBTITLE 1,Technology Stack Deep Dive
# MAGIC %md
# MAGIC ---
# MAGIC ## 🛠️ Technology Stack Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Core Databricks Features Used
# MAGIC
# MAGIC | Feature | Role in Project | Why It Matters |
# MAGIC |---|---|---|
# MAGIC | **Unity Catalog** | Stores tables, functions, volumes, models | Single governance plane for everything |
# MAGIC | **Delta Tables** | Medallion architecture storage | ACID transactions, time travel, schema enforcement |
# MAGIC | **UC Volumes** | Store `.wav` audio files | Governed file storage, accessible from SQL |
# MAGIC | **UC Functions** | 12 SQL functions as agent tools | No servers, no UDFs, works in all contexts |
# MAGIC | **`ai_query()`** | Call LLMs/Whisper from SQL | Democratizes AI — any SQL user can use it |
# MAGIC | **Auto Loader** | Incremental file ingestion | Process new files automatically, exactly once |
# MAGIC | **Model Serving** | Deploy agent as REST API | Scale-to-zero, auto-auth, built-in monitoring |
# MAGIC | **Vector Search** | Semantic search over transcripts | RAG-ready, sync from Delta automatically |
# MAGIC | **MLflow** | Log, version, deploy the agent | Reproducibility + lineage |
# MAGIC | **Genie** | NL querying for business users | Zero-code access to insights |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### External Libraries
# MAGIC
# MAGIC | Library | Version | Purpose |
# MAGIC |---|---|---|
# MAGIC | `langgraph` | 0.3.4 | Agent state machine (tool-calling loop) |
# MAGIC | `databricks-langchain` | latest | `ChatDatabricks` + `UCFunctionToolkit` |
# MAGIC | `databricks-agents` | latest | One-line deployment |
# MAGIC | `mlflow` | ≥ 2.17 | Resource declarations + `ChatAgent` interface |
# MAGIC | `unitycatalog-ai` | latest | UC function tooling |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Model Endpoints
# MAGIC
# MAGIC | Endpoint | Model | Task | Cost Tier |
# MAGIC |---|---|---|---|
# MAGIC | `databricks-claude-sonnet-4-6` | Claude Sonnet 4 | Agent reasoning + orchestration | Pay-per-token |
# MAGIC | `databricks-gemini-3-5-flash` | Gemini 3.5 Flash | Analysis (sentiment, topics, rubric) | Pay-per-token |
# MAGIC | `va_whisper_large_v3` | Whisper large-v3 | Audio speech-to-text | GPU (provisioned) |
# MAGIC | `databricks-gte-large-en` | GTE-Large | Embedding for Vector Search | Pay-per-token |

# COMMAND ----------

# DBTITLE 1,Hackathon Tips & Customization Ideas
# MAGIC %md
# MAGIC ---
# MAGIC ## 🏆 Hackathon Tips & Customization Ideas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⏰ Time Management (30-Minute Sprint)
# MAGIC
# MAGIC | Minutes | Activity | Must-Have? |
# MAGIC |---|---|---|
# MAGIC | 0–3 | Run `01_setup` | ✅ |
# MAGIC | 3–10 | Run `02_deploy` (up to local test) | ✅ |
# MAGIC | 10–15 | Deploy endpoint (runs in background) | ✅ |
# MAGIC | 15–20 | Run `03_test` (pre-deploy phase) | ✅ |
# MAGIC | 20–25 | Run `04_dashboard` queries | Nice-to-have |
# MAGIC | 25–30 | Demo in AI Playground + Genie | 🎬 Show-stopper |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Ways to Make It Your Own
# MAGIC
# MAGIC #### Change the Domain (Keep the Pattern!)
# MAGIC The architecture is domain-agnostic. Swap in your own use case:
# MAGIC
# MAGIC | Original (Higher Ed Advisory) | Your Adaptation |
# MAGIC |---|---|
# MAGIC | Student call recordings | Customer support calls |
# MAGIC | FAFSA, enrollment topics | Product issues, billing disputes |
# MAGIC | Advisor quality rubric | Agent compliance checklist |
# MAGIC | Financial Aid queue | Sales, Retention, Technical Support |
# MAGIC
# MAGIC #### Add New UC Functions
# MAGIC ```sql
# MAGIC CREATE FUNCTION catalog.schema.detect_compliance_violation(transcription STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN (
# MAGIC   SELECT ai_query('model-endpoint',
# MAGIC     CONCAT('Check for regulatory violations...\n', transcription))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Extend the Rubric
# MAGIC Add new criteria to `advisor_rubric` table:
# MAGIC - "Upsell Opportunity Detection" (for sales)
# MAGIC - "HIPAA Compliance" (for healthcare)
# MAGIC - "Escalation Appropriateness" (for support)
# MAGIC
# MAGIC #### Add More Consumers
# MAGIC - **Slack bot** → query the agent endpoint from Slack
# MAGIC - **Email digest** → scheduled notebook sends daily QA summary
# MAGIC - **Real-time alerts** → trigger on compliance flags via Workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎤 Demo Script (For Judges)
# MAGIC
# MAGIC **Opening (30 sec):** "We built a system that automatically evaluates 100% of contact center calls — no manual listening required."
# MAGIC
# MAGIC **Architecture (1 min):** Show the medallion flow. Highlight: audio → transcript → AI evaluation.
# MAGIC
# MAGIC **Live Demo (2 min):**
# MAGIC 1. Open AI Playground, select endpoint
# MAGIC 2. Ask: "What audio files do we have?"
# MAGIC 3. Ask: "Transcribe speaker 5 and give me a full quality analysis"
# MAGIC 4. Watch the agent chain multiple tools together
# MAGIC 5. Show the rubric score + coaching recommendations
# MAGIC
# MAGIC **Dashboard (30 sec):** Show supervisor view with agent rankings + coaching alerts.
# MAGIC
# MAGIC **Genie (30 sec):** Ask a plain-English question, get an instant chart.
# MAGIC
# MAGIC **Close (30 sec):** "This scales to thousands of calls per day, costs pennies per evaluation, and surfaces coaching opportunities that would take supervisors weeks to find manually."

# COMMAND ----------

# DBTITLE 1,Troubleshooting & FAQ
# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 Troubleshooting & FAQ
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Issues
# MAGIC
# MAGIC | Problem | Cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `ai_query()` returns error | Warehouse not running or endpoint not available | Start warehouse; verify endpoint name |
# MAGIC | Whisper transcription fails | Endpoint not deployed or cold start | Deploy from Marketplace; wait 5 min for warm-up |
# MAGIC | Agent returns empty response | Tools not loading correctly | Check UC function names match exactly |
# MAGIC | `ResourceDoesNotExist` on deploy | Model not registered in UC yet | Run the registration cell first |
# MAGIC | Auto Loader finds 0 files | Volume path wrong or empty | Verify path: `/Volumes/{catalog}/{schema}/{volume}/` |
# MAGIC | Vector Search sync fails | CDF not enabled on table | Run `ALTER TABLE SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` |
# MAGIC | Endpoint stays PENDING | Resource constraints or config error | Check endpoint logs in Model Serving UI |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### FAQ
# MAGIC
# MAGIC **Q: Can I use a different LLM for analysis?**
# MAGIC A: Yes! Just change the `llm_endpoint` widget. Any Foundation Model endpoint works with `ai_query()`. Gemini 3.5 Flash is fastest/cheapest; Claude gives best quality.
# MAGIC
# MAGIC **Q: How much does this cost to run?**
# MAGIC A: Pay-per-token for LLMs (pennies per call analysis). Whisper needs a GPU endpoint (~$2-5/hr when active, scale-to-zero otherwise). Serverless SQL for `ai_query()` is pay-per-query.
# MAGIC
# MAGIC **Q: Can I process calls in real-time?**
# MAGIC A: Yes — Auto Loader in `continuous` trigger mode picks up files as they land. The agent endpoint is always-on. Add a trigger (e.g., EventBridge) to call the endpoint when new files arrive.
# MAGIC
# MAGIC **Q: How do I add my own audio files?**
# MAGIC A: Upload `.wav` files to the UC Volume path. Auto Loader detects them on next trigger.
# MAGIC
# MAGIC **Q: Can I deploy this as a Databricks App?**
# MAGIC A: Yes! There's an `app.py` in the repo that wraps the agent endpoint in a Gradio/Streamlit UI. Deploy via `databricks apps deploy`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Key Documentation Links
# MAGIC
# MAGIC | Topic | Link |
# MAGIC |---|---|
# MAGIC | UC Functions | https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf.html |
# MAGIC | `ai_query()` | https://docs.databricks.com/large-language-models/ai-functions.html |
# MAGIC | Auto Loader | https://docs.databricks.com/ingestion/auto-loader/ |
# MAGIC | Model Serving | https://docs.databricks.com/machine-learning/model-serving/ |
# MAGIC | LangGraph | https://langchain-ai.github.io/langgraph/ |
# MAGIC | MLflow ChatAgent | https://mlflow.org/docs/latest/llms/chat-agent/ |
# MAGIC | Genie Spaces | https://docs.databricks.com/genie/ |
# MAGIC | Vector Search | https://docs.databricks.com/generative-ai/vector-search/ |

# COMMAND ----------

# DBTITLE 1,Quick Reference: Run Order
# MAGIC %md
# MAGIC ---
# MAGIC ## 🗺️ Quick Reference: Run Order
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────────────────┐
# MAGIC │  NOTEBOOK RUN ORDER                                               │
# MAGIC ├───────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                   │
# MAGIC │  01_setup       ──►  Schema + Tables + Rubric + 12 UC Functions   │
# MAGIC │       │                                                          │
# MAGIC │       ▼                                                          │
# MAGIC │  02_deploy      ──►  AutoLoader + Agent + MLflow + Endpoint       │
# MAGIC │       │                                                          │
# MAGIC │       ▼                                                          │
# MAGIC │  03_test        ──►  Pre-deploy + Post-deploy validation          │
# MAGIC │       │                                                          │
# MAGIC │       ▼                                                          │
# MAGIC │  04_dashboard   ──►  Supervisor QA scoring dashboard              │
# MAGIC │       │                                                          │
# MAGIC │       ▼                                                          │
# MAGIC │  05_genie_skill ──►  Genie Space + Reusable AI Skills            │
# MAGIC │                                                                   │
# MAGIC └───────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Widget Parameters (Set Once, Used Everywhere)
# MAGIC
# MAGIC | Widget | Example Value | Used By |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `yyang` | All notebooks |
# MAGIC | `schema` | `contact_center_qa` | All notebooks |
# MAGIC | `volume_path` | `/Volumes/chada_demos/pubsec_demos/audio/` | 02_deploy |
# MAGIC | `warehouse_id` | `8baced1ff014912d` | 01_setup, 03_test |
# MAGIC | `whisper_endpoint` | `va_whisper_large_v3` | 01_setup, 02_deploy |
# MAGIC | `llm_endpoint` | `databricks-gemini-3-5-flash` | 01_setup, 05_genie |
# MAGIC | `agent_llm_endpoint` | `databricks-claude-sonnet-4-6` | 02_deploy |
# MAGIC | `endpoint_name` | `agents_yyang-contact_center_qa-...` | 03_test (post-deploy) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🌟 The "Wow" Moments to Hit
# MAGIC
# MAGIC 1. **🎯 `ai_query()` in SQL** — "Look, I'm calling an LLM from a SQL function. No Python needed."
# MAGIC 2. **🤖 Agent chains tools** — "It found the file, transcribed it, AND scored it — all from one prompt."
# MAGIC 3. **📊 One data, four consumers** — "Same gold table powers: dashboard, agent, Genie, and AI skills."
# MAGIC 4. **🔒 Governance built-in** — "UC tracks who called which function, which model version is deployed."
# MAGIC 5. **⚡ Scale-to-zero** — "Costs nothing when idle, scales to thousands of evaluations per hour."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Good luck, and happy hacking! 🚀**

# COMMAND ----------

# DBTITLE 1,Generate Architecture Diagram (PNG + SVG)
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

def create_architecture_diagram(save_path_prefix):
    """Create a clean, hackathon-friendly architecture workflow diagram (no emoji - font safe)."""
    
    fig, ax = plt.subplots(1, 1, figsize=(20, 14))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 14)
    ax.axis('off')
    fig.patch.set_facecolor('#FAFBFC')
    ax.set_facecolor('#FAFBFC')
    
    colors = {
        'bronze': '#CD7F32', 'silver': '#708090', 'gold': '#DAA520',
        'agent': '#4A90D9', 'serving': '#7B68EE', 'source': '#E74C3C',
        'header': '#2C3E50', 'arrow': '#34495E',
    }
    
    # Title
    ax.text(10, 13.5, 'Enterprise Contact Center - Post-Call Quality Evaluation', 
            fontsize=20, fontweight='bold', ha='center', va='center', color=colors['header'])
    ax.text(10, 13.0, 'Hackathon Architecture Workflow', 
            fontsize=13, ha='center', va='center', color='#666666', style='italic')
    
    def draw_box(x, y, w, h, color, label, sublabel='', fontsize=11):
        shadow = FancyBboxPatch((x+0.06, y-0.06), w, h, boxstyle="round,pad=0.1", 
                               facecolor='#00000020', edgecolor='none', linewidth=0)
        ax.add_patch(shadow)
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1", 
                            facecolor=color, edgecolor='white', linewidth=2.5, alpha=0.92)
        ax.add_patch(box)
        ax.text(x + w/2, y + h/2 + (0.15 if sublabel else 0), label, 
                fontsize=fontsize, fontweight='bold', ha='center', va='center', color='white')
        if sublabel:
            ax.text(x + w/2, y + h/2 - 0.22, sublabel, 
                    fontsize=8.5, ha='center', va='center', color='#FFFFFFCC')
    
    def draw_arrow(x1, y1, x2, y2, color='#34495E', lw=2.2):
        ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                   arrowprops=dict(arrowstyle='->', color=color, lw=lw, connectionstyle='arc3,rad=0'))
    
    def draw_curved_arrow(x1, y1, x2, y2, color='#34495E', rad=0.2):
        ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                   arrowprops=dict(arrowstyle='->', color=color, lw=1.8, connectionstyle=f'arc3,rad={rad}'))
    
    # === DATA SOURCE ===
    draw_box(0.5, 9.5, 3.2, 1.8, colors['source'], 'AUDIO FILES', '.wav in UC Volume', fontsize=13)
    
    # === MEDALLION PIPELINE ===
    panel = FancyBboxPatch((4.2, 5.0), 7.5, 7.2, boxstyle="round,pad=0.2",
                          facecolor='#F0F4F8', edgecolor='#CBD5E0', linewidth=1.5, alpha=0.7)
    ax.add_patch(panel)
    ax.text(7.95, 11.9, 'MEDALLION DATA PIPELINE', fontsize=13, fontweight='bold', ha='center', color=colors['header'])
    
    draw_box(5.0, 10.2, 5.0, 1.2, colors['bronze'], 'BRONZE', 'Auto Loader > file metadata', fontsize=13)
    draw_box(5.0, 8.2, 5.0, 1.2, colors['silver'], 'SILVER', 'Whisper STT > transcriptions', fontsize=13)
    draw_box(5.0, 6.2, 5.0, 1.2, colors['gold'], 'GOLD', 'LLM > QA scores + sentiment + topics', fontsize=13)
    draw_box(5.0, 5.2, 5.0, 0.7, '#6C757D', 'QA RUBRIC', '5 criteria x weighted scores', fontsize=10)
    
    draw_arrow(7.5, 10.2, 7.5, 9.5, colors['arrow'])
    draw_arrow(7.5, 8.2, 7.5, 7.5, colors['arrow'])
    draw_arrow(3.7, 10.4, 5.0, 10.8, colors['arrow'])
    
    # Labels on arrows
    ax.text(9.0, 9.85, 'ai_query(Whisper)', fontsize=8.5, color='#555', style='italic', ha='center',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor='#DDD', alpha=0.8))
    ax.text(9.0, 7.85, 'ai_query(LLM)', fontsize=8.5, color='#555', style='italic', ha='center',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor='#DDD', alpha=0.8))
    ax.text(3.8, 11.1, 'Auto Loader', fontsize=8.5, color='#555', style='italic', ha='center',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor='#DDD', alpha=0.8))
    
    # === AI AGENT ===
    agent_panel = FancyBboxPatch((12.2, 7.5), 4.2, 4.8, boxstyle="round,pad=0.2",
                                facecolor='#EBF5FF', edgecolor='#90CDF4', linewidth=1.5, alpha=0.7)
    ax.add_patch(agent_panel)
    ax.text(14.3, 12.0, 'AI AGENT', fontsize=13, fontweight='bold', ha='center', color=colors['header'])
    
    draw_box(12.5, 10.5, 3.6, 1.0, colors['agent'], 'LangGraph Agent', 'Claude Sonnet 4', fontsize=11)
    draw_box(12.5, 9.0, 3.6, 1.0, '#3498DB', '10 UC Function Tools', 'Pure SQL | Governed', fontsize=10)
    draw_box(12.5, 7.7, 3.6, 0.9, colors['serving'], 'Model Serving', 'Scale-to-zero endpoint', fontsize=10)
    
    draw_arrow(14.3, 10.5, 14.3, 10.05, colors['arrow'])
    draw_arrow(14.3, 9.0, 14.3, 8.65, colors['arrow'])
    draw_curved_arrow(10.0, 7.0, 12.5, 9.5, '#3498DB', rad=-0.2)
    ax.text(11.0, 8.7, 'reads/writes', fontsize=8, color='#3498DB', style='italic', rotation=28)
    
    # === CONSUMERS ===
    consumer_panel = FancyBboxPatch((0.5, 1.5), 19.0, 3.2, boxstyle="round,pad=0.2",
                                   facecolor='#F0FFF4', edgecolor='#9AE6B4', linewidth=1.5, alpha=0.5)
    ax.add_patch(consumer_panel)
    ax.text(10, 4.4, 'CONSUMERS - Who Benefits?', fontsize=13, fontweight='bold', ha='center', color=colors['header'])
    
    cy, ch = 2.2, 1.8
    draw_box(0.8, cy, 3.5, ch, '#27AE60', 'DASHBOARD', 'Supervisors | Agent rankings', fontsize=10)
    draw_box(4.8, cy, 3.5, ch, '#2ECC71', 'AI PLAYGROUND', 'Natural language queries', fontsize=10)
    draw_box(8.8, cy, 3.5, ch, '#1ABC9C', 'GENIE SPACE', 'Business analysts | No code', fontsize=10)
    draw_box(12.8, cy, 3.5, ch, '#16A085', 'AI SKILLS', 'Reusable UC functions', fontsize=10)
    draw_box(16.8, cy, 2.7, ch, '#0E8A6F', 'VECTOR SEARCH', 'Semantic RAG', fontsize=9)
    
    for cx in [2.55, 6.55, 10.55, 14.55, 18.15]:
        draw_arrow(cx, 5.0, cx, 4.05, '#2ECC7199', lw=1.5)
    
    # === RUN ORDER ===
    run_panel = FancyBboxPatch((16.8, 8.5), 2.8, 4.5, boxstyle="round,pad=0.15",
                              facecolor='#FFF5F5', edgecolor='#FEB2B2', linewidth=1.5, alpha=0.8)
    ax.add_patch(run_panel)
    ax.text(18.2, 12.7, 'RUN ORDER', fontsize=11, fontweight='bold', ha='center', color=colors['header'])
    
    steps = [('01_setup', '~3 min'), ('02_deploy', '~15 min'), ('03_test', '~5 min'),
             ('04_dashboard', '~5 min'), ('05_genie', '~3 min')]
    step_colors = ['#CD7F32', '#4A90D9', '#2ECC71', '#F39C12', '#9B59B6']
    for i, (name, time) in enumerate(steps):
        y_pos = 12.1 - i * 0.75
        circle = plt.Circle((17.2, y_pos), 0.22, color=step_colors[i], zorder=5)
        ax.add_patch(circle)
        ax.text(17.2, y_pos, str(i+1), fontsize=10, fontweight='bold', ha='center', va='center', color='white', zorder=6)
        ax.text(17.55, y_pos, name, fontsize=9, ha='left', va='center', color='#333', fontweight='medium')
        ax.text(19.4, y_pos, time, fontsize=8, ha='right', va='center', color='#888')
        if i < 4:
            ax.annotate('', xy=(17.2, y_pos - 0.38), xytext=(17.2, y_pos - 0.25),
                       arrowprops=dict(arrowstyle='->', color='#AAA', lw=1.2))
    
    # Footer
    ax.text(10, 0.8, 'Built with: Unity Catalog | Delta Lake | ai_query() | LangGraph | MLflow | Model Serving | Vector Search | Genie',
            fontsize=9, ha='center', va='center', color='#888888', style='italic')
    ax.text(10, 0.3, 'Total time: ~31 minutes from zero to deployed agent',
            fontsize=10, ha='center', va='center', color='#555555', fontweight='bold')
    
    plt.tight_layout(pad=0.5)
    fig.savefig(f'{save_path_prefix}.png', dpi=200, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none')
    fig.savefig(f'{save_path_prefix}.svg', format='svg', bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)
    print(f"Saved: {save_path_prefix}.png (high-res)")
    print(f"Saved: {save_path_prefix}.svg (scalable vector)")

# Generate
output_path = '/tmp/hackathon_architecture'
create_architecture_diagram(output_path)

from IPython.display import Image
print("\nTo copy to your Volume for sharing:")
print("  dbutils.fs.cp('file:/tmp/hackathon_architecture.png', '/Volumes/yyang/contact_center_qa/audio_files/hackathon_architecture.png')")
print("  dbutils.fs.cp('file:/tmp/hackathon_architecture.svg', '/Volumes/yyang/contact_center_qa/audio_files/hackathon_architecture.svg')")
Image(filename=f'{output_path}.png')