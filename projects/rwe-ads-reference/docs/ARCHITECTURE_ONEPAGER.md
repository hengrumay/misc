<!-- Styled source (re-render with headless Chrome --print-to-pdf): docs/architecture_onepager.html -->



# RWE ADS Automation — architecture & where agents fit

*As-built annotated · v1.5 · draft for internal review before any external use*

> **As-built status.** The recommended architecture below is **implemented and validated end-to-end across the three synthetic PoCs** (poc_low, poc_med, poc_high). The previously-open piece — **PDF/DOCX extraction** — is **done**: `ai_parse_document` → `ai_extract` turns protocol files into the structured spec that drives the build. (Markdown parsing remains only as a narrative-only fallback.) The **As-built** column in §2 maps each recommended step to the live component.

**In plain English.** The goal is to turn a study protocol (a document describing an epidemiology study) into an analysis-ready dataset (ADS). The recommended pattern does this on governed data by assembling pre-approved SQL building blocks (rather than free-form model-generated SQL), validating each step, and requiring a recorded human approval before results are used. Most of the workflow is a predictable, governed pipeline; an AI "agent" is only genuinely valuable in one place — letting an analyst ask questions of the finished dataset — and that is optional.

**Plain-word glossary.** Study protocol = document describing an epidemiology study · ADS = the finished analysis-ready dataset · cohort = the patient group that fits the study's rules · knowledge base / snippet = library of pre-approved SQL building blocks (a "snippet" = one block) · medallion (bronze→silver→gold) = raw→cleaned→analysis-ready data layers · serverless = Databricks runs the compute for you · in-process governance = model calls run in-platform; `gateway_call` applies PII masking, audit logging, and MLflow tracing in-process (external-egress denial is a separate workspace policy, `egress_policy: deny_external`, not enforced inside the call) · validate-don't-execute = check SQL is valid without running it on data · Genie = ask-questions-in-plain-English over governed tables.

## 1. The big picture

Study protocol (PDF/DOCX) → Extract fields → Structured spec → Build ADS SQL (from approved blocks) → Validate + review → Approved ADS + audit trail

A governed, predictable pipeline. SQL comes from the approved knowledge base; every step is validated; a human approves before use.

## 2. Recommended build (Databricks-native) — with as-built status

| Step | Component | Wave | As-built status |
|---|---|---|---|
| Ingest protocol (PDF/DOCX → fields) | AI Functions | Wave 1 | **✅ Live** — `ai_parse_document` → `ai_extract` → `protocol_spec` (PDF + DOCX; all 3 PoCs) |
| → Structured spec (extracted protocol fields) | | | **✅ Live** — coded `protocol_spec` (dx/ndc/exclusion/outcome codes, ages, windows, covariates) + per-field confidence |
| → Governed data medallion (bronze → silver → gold) | | Wave 1 (SDP task) | **✅ In place** — serverless SDP → `ads_serving.patient_timeline` |
| → Build ADS SQL from approved knowledge base | | Wave 3 | **✅ Live** — composes only `status='approved'` snippets; per-complexity recipes (low/med/high) |
| → Validate (don't execute) | | | **✅ Live** — EXPLAIN-gates every step; executes only vs synthetic gold |
| → Extraction eval → analyst review + approval → serve + audit | | Wave 3 → 5 | **✅ Live** — Stage-1 deterministic `eval_ok` is the hard gate (Stage-3 model judges advisory); protocol + ADS e-sign gates; hash-chained GxP audit; Lakebase serving. **Demo default auto-signs eval-passing specs under a SYSTEM actor — see §4.** |

Orchestrated with **native Databricks Workflows/Jobs and pipelines, on serverless compute** (verified: 20 job tasks + 1 declarative pipeline; **no LangGraph or custom agent framework**). Model calls run **in-platform**; `gateway_call` applies **in-process** PII masking, audit logging, and MLflow tracing. External-egress denial is a **workspace policy** (`egress_policy: deny_external`), not enforced inside `gateway_call`. (The native `ads-ai-gateway` endpoint is a placeholder that 403s on pay-per-token FMs; strict gateway routing is a documented follow-on.)

**Where document (PDF/DOCX) extraction fits.** It's the entry step — Wave 1 (protocol ingestion): protocol files land in a governed Unity Catalog volume, then Databricks AI Functions (`ai_parse_document` → `ai_extract`) turn them into the structured spec that drives everything downstream — build → validate → review → serve. It is a deterministic, governed step (not an agent). **As-built:** implemented and validated on synthetic PDFs + a DOCX; real redacted protocols drop into the same volume with no code change.

## 3. Where an agent genuinely adds value

**Keep it a governed pipeline**

- CORE: Document extraction → structured spec: Databricks AI Functions in a job — **✅ built**
- CORE: ADS SQL from the approved knowledge base, validated before use — **✅ built**
- CORE: Orchestration: native Workflows/Jobs + pipelines (serverless) — **✅ built**
- CORE: Human review + approval; reproducibility + audit trail — **✅ built**

Predictable, governed, easy to audit — the right default for a regulated workflow.

**Add an agent only where it earns its place**

- OPTIONAL: **Genie for analyst Q&A over the approved ADS tables — the clearest fit.** The bundle ships a Genie config placeholder (no space is provisioned); point a Genie space at the approved `ads_output` post-deploy for read-only cohort-size / outcome-rate questions.
- OPTIONAL: Multi-agent orchestration — consider only if the workflow becomes genuinely dynamic (branching decisions). A native Workflow already covers a fixed, known pipeline. **Not built (by design).**

Reserve "agent" for genuinely dynamic reasoning — not a fixed pipeline.

## 4. Data posture & one honest divergence

- **Data posture:** synthetic / de-identified only — deterministically generated RWD (Faker, fixed seed) and synthetic protocol fixtures; **no PHI**. Real redacted protocols are fine when cleared to share.
- **AI Gateway routing (divergence to note):** the document-extraction AI Functions (`ai_parse_document` / `ai_extract`) are **native, in-platform** Databricks calls — external-egress denial is inherently satisfied, but they do **not** route through the named `ads-ai-gateway` endpoint, and PHI masking is not applied to the extraction input unless `ai_mask` is run first (today it relies on inputs being synthetic/redacted). Strict gateway routing for extraction (an `ai_mask` pre-pass, or `ai_query` against a gateway-fronted served model) is a documented follow-on, not a blocker for synthetic data.
- **Eval gate (what blocks e-sign):** the hard gate is **Stage-1 deterministic validators** (`eval_ok`) — a spec that fails them cannot be e-signed. **Stage-3 model judges** (via `gateway_call`) are **advisory**: they set review priority + flags only, never `eval_ok`, and silently degrade to Stage-1/2 when no judge endpoint or source text is available. It is not a model-gated eval.
- **Auto-e-sign (demo default):** the shipped demo config sets `allow_auto_esign: true`, which auto-signs eval-passing specs under a **SYSTEM actor** (a non-human signature, explicitly not a 21 CFR Part 11 human e-signature), **bypassing the human analyst e-sign**. The human e-sign path is real code but off by default — set `allow_auto_esign: false` for any regulated use.
- **poc_high scope:** lab-value (LOINC) covariates are out of scope until a `der_baseline_lab_value` snippet goes through the KB approval path; poc_high is built from approved comorbidity/exposure/outcome snippets.
- **Enrollment window:** defaults to 90 days because the synthetic enrollment spans are short; the outcome follow-up window stays faithful to each protocol (365/730 days).

---

*Working architecture view · draft for internal review. Recommends a governed, Databricks-native pipeline with agents used only where genuinely warranted. As-built annotations reflect the synthetic PoC build.*
