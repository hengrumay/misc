# Wave 2 — Medallion (Bronze / Silver / Gold) Data Architecture

## Overview

Wave 2 implements a classic **medallion architecture** over synthetic RWE using **Spark Declarative Pipelines (DLT)** running serverless. The three layers (Bronze → Silver → Gold) transform raw synthetic data into analysis-ready canonical tables.

## Layer Architecture

### Bronze Layer (`pipelines/bronze.py`)

**Purpose**: Raw data ingestion without transformation.

**Tables** (7 canonical entities):
- `patient`: Demographics (patient_id, birth_date, sex, race, region)
- `provider`: Provider master (provider_id, specialty, region, organization_id)
- `enrollment_span`: Insurance eligibility spans (patient_id, span_start, span_end)
- `encounter`: Patient visits (encounter_id, patient_id, provider_id, encounter_date, encounter_type)
- `medical_claim`: Diagnoses & procedures (claim_id, patient_id, code, code_system, claim_date, description)
- `pharmacy_claim`: Medications (claim_id, patient_id, code, days_supply, fill_date)
- `lab_result`: Lab tests (lab_id, patient_id, code, value, unit, lab_date)

**Data Flow**: Reads pre-generated Delta tables from Wave 1 synthetic generation (`lib/synth/`). Fallback: empty schema on first run, hydrated by Wave 1 job.

**Expectations**: Minimal; schema validation only. Serves as the "single source of truth" for raw state.

---

### Silver Layer (`pipelines/silver.py`)

**Purpose**: Conformation, type standardization, quality validation, referential integrity.

**Key Transformations**:
- **Deduplication**: Removes duplicate records by primary key
- **Type Casting**: Ensures consistent types (dates, numerics, strings)
- **Code Standardization**: Validates code systems (ICD-10-CM, CPT, HCPCS, NDC, LOINC)
- **Referential Integrity**: Joins with `patient` to validate all events reference existing patients
- **Data Quality Expectations**:
  - `patient`: Valid sex (M/F/U), valid race, non-null patient_id
  - `enrollment_span`: `span_start <= span_end`, valid dates
  - `pharmacy_claim`: `days_supply > 0`, valid NDC codes
  - `medical_claim`: Valid ICD-10-CM/CPT/HCPCS codes
  - `lab_result`: Valid LOINC codes, numeric values
  - `encounter`: Valid encounter types, referential integrity with patient

**Expectations Used**:
- `@dlt.expect_or_drop(...)`: Silently drops rows that fail (strictest)
- `@dlt.expect(...)`: Logs quality violations but passes rows through

**Output**: Cleaned, typed, validated conformed layer ready for analytics.

---

### Gold Layer (`pipelines/gold.py`)

**Purpose**: Canonical analytic base tables consumed by the ADS builder (Wave 3+).

**Tables** (3 canonical outputs):

#### 1. `patient_timeline` (Longitudinal Fact Table)
One row per coded event, ordered by patient and date.

**Columns**:
- `patient_id`: Foreign key to patient
- `event_type`: One of {`dx`, `rx`, `proc`, `lab`, `enc`}
  - `dx` = diagnosis (ICD-10-CM)
  - `rx` = pharmacy fill (NDC)
  - `proc` = procedure (CPT/HCPCS)
  - `lab` = lab result (LOINC)
  - `enc` = encounter
- `code`: The coded value (ICD-10-CM, NDC, CPT, LOINC, or encounter_type)
- `code_system`: The coding system (ICD-10-CM, NDC, CPT, HCPCS, LOINC, encounter)
- `event_date`: Date of the event
- `days_supply`: (rx only) Days of medication supply
- `value`: (lab only) Numeric result
- `unit`: (lab only) Unit of measurement (e.g., mg/dL, mmol/L)
- `provider_id`: Treating provider (if available)

**Index**: Naturally indexed by (patient_id, event_date) for timeline queries.

**Use Case**: The ADS builder queries this table to:
- Identify diagnoses (`WHERE event_type = 'dx' AND code IN (...)`)
- Calculate time-to-event outcomes
- Build medication exposure eras
- Fetch baseline covariates

#### 2. `code_rollups` (Classification Reference)
Maps atomic codes to rollup groups (phenotypes, condition clusters).

**Columns**:
- `code`: Atomic code (e.g., "E11.9", "00093-5117-16")
- `code_system`: System (ICD-10-CM, NDC, CPT, HCPCS, LOINC)
- `rollup_group`: Human-readable grouping (e.g., "diabetes", "antihypertensive", "statin")
- `description`: Plain-text definition

**Current Implementation**: Hardcoded seed of common codes (extensible for production).

**Use Case**: The ADS builder joins this to:
- Map individual diagnosis codes to condition flags
- Classify medications by therapeutic class
- Build phenotypes from multiple codes

#### 3. `eligibility_periods` (Enrollment Periods)
Patient insurance eligibility spans used for inclusion criteria and exposure time.

**Columns**:
- `patient_id`: Foreign key
- `span_start`: Eligibility start date
- `span_end`: Eligibility end date

**Use Case**: The ADS builder uses this to:
- Filter for "continuous enrollment" cohorts
- Calculate follow-up time (constrained by enrollment)
- Define baseline and outcome windows

---

## Data Quality Expectations Enforced

| Layer | Table | Expectation | Rule | Action |
|-------|-------|-------------|------|--------|
| Silver | patient | `valid_patient_id` | NOT NULL | drop |
| Silver | patient | `valid_sex` | sex IN ('M','F','U') | pass/log |
| Silver | enrollment_span | `valid_dates` | span_start ≤ span_end | pass/log |
| Silver | pharmacy_claim | `valid_days_supply` | days_supply > 0 | pass/log |
| Silver | medical_claim | `valid_code_system` | IN (ICD-10-CM, CPT, HCPCS) | pass/log |
| Silver | lab_result | `valid_code_system` | = LOINC | pass/log |
| Gold | patient_timeline | `valid_event_type` | IN (dx, rx, proc, lab, enc) | pass/log |
| Gold | eligibility_periods | `valid_dates` | span_start ≤ span_end | pass/log |

---

## RWD Common Data Model Mapping

The silver/gold schemas map to standard CDMs:

| Bronze Entity | Silver Entity | Gold Analytic Use | CDM Equivalent |
|---------------|---------------|-------------------|----------------|
| medical_claim (ICD-10-CM) | medical_claim (dx) | patient_timeline (event_type='dx') | CONDITION_OCCURRENCE |
| medical_claim (CPT) | medical_claim (proc) | patient_timeline (event_type='proc') | PROCEDURE_OCCURRENCE |
| pharmacy_claim | pharmacy_claim | patient_timeline (event_type='rx') + days_supply | DRUG_EXPOSURE |
| lab_result | lab_result | patient_timeline (event_type='lab') + value/unit | MEASUREMENT |
| encounter | encounter | patient_timeline (event_type='enc') | VISIT_OCCURRENCE |
| patient | patient | (demographics) | PERSON |
| enrollment_span | enrollment_span | eligibility_periods | OBSERVATION_PERIOD |
| — | code_rollups | (classification) | CONCEPT + CONCEPT_ANCESTOR |

---

## Pipeline Execution & Idempotency

**DLT Configuration** (`databricks.yml`):
```yaml
pipelines:
  ads_medallion:
    name: "RWE ADS Medallion (Bronze/Silver/Gold)"
    target: "{{catalog}}.{{serving}}"  # Writes to ads_serving schema
    cluster_type: "serverless"
    photon: true
    libraries:
      - path: ./pipelines/bronze.py
      - path: ./pipelines/silver.py
      - path: ./pipelines/gold.py
```

**Execution**:
```bash
databricks bundle run ads_medallion -t dev
```

**Idempotency**:
- Each DLT table is `CREATE OR REPLACE` (via DLT magic)
- Upstream changes (Wave 1 synthetic generation) automatically re-hydrate downstream
- Expectations are **validation gates**, not blocking failures (logged to DLT event log)

---

## Next Steps (Wave 3: ADS Builder)

Wave 3 consumes the gold canonical tables to:
1. Parse study protocol (Wave 1 output)
2. Retrieve approved SQL snippets (Wave 0 KB)
3. Compose ADS-specific SQL from patient_timeline + code_rollups + eligibility_periods
4. Execute validation against synthetic gold (no real patient DB access)
5. Generate reproducibility manifest (audit trail)
6. Output ads_output + cohort_summary (to be synced to Lakebase for low-latency serving)

---

## Deployment Checklist

- [ ] Wave 0 complete: schemas, KB, protocols created
- [ ] Wave 1 complete: synthetic bronze tables generated, protocols ingested
- [ ] Wave 2 (this): DLT pipelines deployed and passing all expectations
- [ ] Check row counts: bronze (e.g., 50k patients) → silver (after dedup) → gold
- [ ] Verify gold tables indexed by patient_id + event_date for timeline queries
- [ ] Confirm eligibility_periods covers full study date range for each patient
- [ ] Ready for Wave 3: ADS builder can query {{gold}}.patient_timeline

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| Bronze tables empty | Wave 1 synthetic generation not run | Run `databricks bundle run wave1_synth_bronze` |
| Silver dedup loses rows | Duplicate patient_ids in bronze | Check bronze source; re-run Wave 1 with different seed if needed |
| Gold patient_timeline missing events | Event filtered in silver (e.g., invalid code_system) | Check silver expectations; inspect DLT event log for dropped rows |
| Code_rollups incomplete | Hardcoded seed didn't cover all codes | Extend `gold_code_rollups()` or load from reference table |
| Eligibility_periods sparse | Enrollment_span generation logic | Check Wave 1 enrollment_span generation parameters in cfg().synth |
