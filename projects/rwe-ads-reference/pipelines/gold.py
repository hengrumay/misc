"""Gold Layer — Spark Declarative Pipeline (DLT).

Canonical analytic base tables for ADS builder consumption:
- patient_timeline: longitudinal, one row per coded event
- code_rollups: code classification hierarchies
- eligibility_periods: enrollment periods

Serverless-compatible. Uses dlt decorators.
"""
from __future__ import annotations

import sys
from pathlib import Path

# DLT notebook context has no __file__. Derive the synced-files root and config
# path from the pipeline `configuration` (ads.config_path) set in resources/pipelines.yml.
import os, sys
try:
    _cfg_path = spark.conf.get("ads.config_path")
    os.environ["ADS_CONFIG_PATH"] = _cfg_path
    _root = os.path.dirname(_cfg_path)
    if _root not in sys.path:
        sys.path.insert(0, _root)
except Exception:
    pass

try:
    import dlt
    from pyspark.sql import functions as F, Window
except ImportError:  # pragma: no cover
    pass

from lib.config import cfg

# Gold publishes to the serving schema (ads_serving), while silver.py publishes
# to the pipeline's default target (ads_curated). Fully-qualified names let one
# serverless pipeline write both schemas. Resolved once at import.
try:
    _SERVING = cfg().serving   # fully-qualified serving schema (from demo.config.yaml)
    _CURATED = cfg().curated   # fully-qualified curated schema (from demo.config.yaml)
except Exception:  # pragma: no cover — cfg() always resolves in the pipeline context
    _SERVING = _CURATED = ""   # names resolve only via cfg(); no literals here


# ============================================================================
# Patient Timeline (canonical analytic base)
# ============================================================================
@dlt.table(
    name=f"{_SERVING}.patient_timeline",
    comment=(
        "Longitudinal patient events: one row per coded event (dx/rx/proc/lab/enc). "
        "Canonical table for ADS builder; indexed by patient_id + event_date."
    ),
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IN ('dx', 'rx', 'proc', 'lab', 'enc')")
@dlt.expect("valid_event_date", "event_date IS NOT NULL")
def gold_patient_timeline():
    """Build longitudinal patient timeline from all event sources.

    Rows are ordered by patient_id, event_date.
    Columns: patient_id, event_type, code, code_system, event_date,
    days_supply, value, unit, provider_id
    """
    silver_medical = dlt.read(f"{_CURATED}.medical_claim")
    silver_pharmacy = dlt.read(f"{_CURATED}.pharmacy_claim")
    silver_lab = dlt.read(f"{_CURATED}.lab_result")
    silver_encounter = dlt.read(f"{_CURATED}.encounter")

    # Diagnoses from medical claims
    diagnoses = (
        silver_medical
        .filter(F.col("code_system") == "ICD-10-CM")
        .select(
            F.col("patient_id"),
            F.lit("dx").alias("event_type"),
            F.col("code"),
            F.col("code_system"),
            F.col("claim_date").alias("event_date"),
            F.lit(None).cast("int").alias("days_supply"),
            F.lit(None).cast("double").alias("value"),
            F.lit(None).cast("string").alias("unit"),
            F.col("provider_id"),
        )
    )

    # Procedures from medical claims
    procedures = (
        silver_medical
        .filter(F.col("code_system").isin(["CPT", "HCPCS"]))
        .select(
            F.col("patient_id"),
            F.lit("proc").alias("event_type"),
            F.col("code"),
            F.col("code_system"),
            F.col("claim_date").alias("event_date"),
            F.lit(None).cast("int").alias("days_supply"),
            F.lit(None).cast("double").alias("value"),
            F.lit(None).cast("string").alias("unit"),
            F.col("provider_id"),
        )
    )

    # Pharmacy (rx) events
    medications = (
        silver_pharmacy
        .select(
            F.col("patient_id"),
            F.lit("rx").alias("event_type"),
            F.col("code"),
            F.col("code_system"),
            F.col("fill_date").alias("event_date"),
            F.col("days_supply"),
            F.lit(None).cast("double").alias("value"),
            F.lit(None).cast("string").alias("unit"),
            F.col("provider_id"),
        )
    )

    # Lab events
    labs = (
        silver_lab
        .select(
            F.col("patient_id"),
            F.lit("lab").alias("event_type"),
            F.col("code"),
            F.col("code_system"),
            F.col("lab_date").alias("event_date"),
            F.lit(None).cast("int").alias("days_supply"),
            F.col("value"),
            F.col("unit"),
            F.lit(None).cast("string").alias("provider_id"),
        )
    )

    # Encounters
    encounters = (
        silver_encounter
        .select(
            F.col("patient_id"),
            F.lit("enc").alias("event_type"),
            F.col("encounter_type").alias("code"),
            F.lit("encounter").alias("code_system"),
            F.col("encounter_date").alias("event_date"),
            F.lit(None).cast("int").alias("days_supply"),
            F.lit(None).cast("double").alias("value"),
            F.lit(None).cast("string").alias("unit"),
            F.col("provider_id"),
        )
    )

    # Union all events and sort
    timeline = (
        diagnoses.unionByName(procedures)
        .unionByName(medications)
        .unionByName(labs)
        .unionByName(encounters)
        .orderBy(F.col("patient_id"), F.col("event_date"))
    )

    return timeline


# ============================================================================
# Code Rollups (classification hierarchies)
# ============================================================================
@dlt.table(
    name=f"{_SERVING}.code_rollups",
    comment=(
        "Code classification hierarchies for grouping and phenotyping. "
        "Each row maps a code (ICD-10-CM, CPT, NDC, LOINC) to a rollup group."
    ),
)
def gold_code_rollups():
    """Build code classification hierarchy (hardcoded for common codes; extensible).

    Rows: code, code_system, rollup_group, description
    """
    from pyspark.sql.types import StructType, StructField, StringType

    # Manually define common rollup groups (in production, this would be a reference table)
    rollups = [
        # Diabetes codes
        ("E11.9", "ICD-10-CM", "diabetes", "Type 2 Diabetes Mellitus"),
        ("E10.9", "ICD-10-CM", "diabetes", "Type 1 Diabetes Mellitus"),
        # Hypertension codes
        ("I10", "ICD-10-CM", "hypertension", "Essential Hypertension"),
        ("I15.0", "ICD-10-CM", "hypertension", "Secondary Hypertension"),
        # Heart failure codes
        ("I50.9", "ICD-10-CM", "heart_failure", "Unspecified Heart Failure"),
        # COPD codes
        ("J44.9", "ICD-10-CM", "copd", "COPD Unspecified"),
        # Lab codes
        ("2345-7", "LOINC", "glucose_labs", "Glucose measurement"),
        ("2093-3", "LOINC", "lipid_labs", "Cholesterol measurement"),
        # Common NDC (grouped by indication)
        ("00093-5117-16", "NDC", "antihypertensive", "Lisinopril"),
        ("00456-2009-60", "NDC", "diabetic_agent", "Metformin"),
        ("00185-0734-11", "NDC", "statin", "Atorvastatin"),
    ]

    schema = StructType([
        StructField("code", StringType(), False),
        StructField("code_system", StringType(), False),
        StructField("rollup_group", StringType(), True),
        StructField("description", StringType(), True),
    ])

    from pyspark.sql import SparkSession
    _spark = SparkSession.getActiveSession()
    df = _spark.createDataFrame(rollups, schema=schema)
    return df


# ============================================================================
# Eligibility Periods (enrollment spans)
# ============================================================================
@dlt.table(
    name=f"{_SERVING}.eligibility_periods",
    comment=(
        "Patient insurance eligibility periods. Used for inclusion criteria "
        "and exposure time calculation. One row per continuous enrollment span."
    ),
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_dates", "span_start <= span_end")
def gold_eligibility_periods():
    """Build eligibility periods table from enrollment spans."""
    silver_enrollment = dlt.read(f"{_CURATED}.enrollment_span")

    return (
        silver_enrollment
        .select(
            F.col("patient_id"),
            F.col("span_start"),
            F.col("span_end"),
        )
        .orderBy(F.col("patient_id"), F.col("span_start"))
    )
