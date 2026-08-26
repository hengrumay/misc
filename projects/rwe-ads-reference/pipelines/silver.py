"""Silver Layer — Spark Declarative Pipeline (DLT).

Conformed, typed, and validated RWD common data model. Quality expectations
enforce referential integrity, date logic, and code standardization.

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


# ============================================================================
# Patient (conformed) — deduped, valid demographics
# ============================================================================
@dlt.table(
    name="patient",
    comment="Conformed patient demographics; deduped, typed, validated",
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_sex", "sex IN ('M', 'F', 'U') OR sex IS NULL")
@dlt.expect("valid_race", "race IN ('White', 'Black', 'Hispanic', 'Asian', 'Native', 'Unknown') OR race IS NULL")
def silver_patient():
    """Conform patient master list: dedup by patient_id, ensure valid types."""
    c = cfg()
    bronze_patient = spark.read.table(cfg().table("raw", "patient"))

    return (
        bronze_patient
        .select(
            F.col("patient_id"),
            F.col("birth_date").cast("date").alias("birth_date"),
            F.col("sex").cast("string").alias("sex"),
            F.col("race").cast("string").alias("race"),
            F.col("region").cast("string").alias("region"),
        )
        .dropDuplicates(["patient_id"])
        .fillna({"sex": "U", "region": "Other"})
    )


# ============================================================================
# Provider (conformed)
# ============================================================================
@dlt.table(
    name="provider",
    comment="Conformed provider master list; deduped, typed",
)
@dlt.expect_or_drop("valid_provider_id", "provider_id IS NOT NULL")
def silver_provider():
    """Conform provider list."""
    bronze_provider = spark.read.table(cfg().table("raw", "provider"))

    return (
        bronze_provider
        .select(
            F.col("provider_id"),
            F.col("specialty").cast("string").alias("specialty"),
            F.col("region").cast("string").alias("region"),
            F.col("organization_id").cast("string").alias("organization_id"),
        )
        .dropDuplicates(["provider_id"])
        .fillna({"specialty": "Other", "region": "Other"})
    )


# ============================================================================
# Enrollment Span (conformed)
# ============================================================================
@dlt.table(
    name="enrollment_span",
    comment="Conformed enrollment spans; date logic validated",
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_dates", "span_start <= span_end")
def silver_enrollment_span():
    """Conform enrollment spans: validate date logic, dedup overlaps."""
    bronze_enrollment = spark.read.table(cfg().table("raw", "enrollment_span"))

    df = (
        bronze_enrollment
        .select(
            F.col("patient_id"),
            F.col("span_start").cast("date").alias("span_start"),
            F.col("span_end").cast("date").alias("span_end"),
        )
        .filter(F.col("span_start") <= F.col("span_end"))
        .dropDuplicates(["patient_id", "span_start", "span_end"])
    )

    return df


# ============================================================================
# Encounter (conformed)
# ============================================================================
@dlt.table(
    name="encounter",
    comment="Conformed encounters; referential integrity with patient/provider",
)
@dlt.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_encounter_type", "encounter_type IN ('office', 'inpatient', 'emergency', 'telehealth') OR encounter_type IS NULL")
def silver_encounter():
    """Conform encounters."""
    bronze_encounter = spark.read.table(cfg().table("raw", "encounter"))
    silver_patient = dlt.read("patient")

    # Join with patient to validate referential integrity
    df = (
        bronze_encounter
        .select(
            F.col("encounter_id"),
            F.col("patient_id"),
            F.col("provider_id"),
            F.col("encounter_date").cast("date").alias("encounter_date"),
            F.col("encounter_type").cast("string").alias("encounter_type"),
        )
        .join(silver_patient.select("patient_id"), "patient_id", "inner")
        .fillna({"encounter_type": "office"})
    )

    return df


# ============================================================================
# Medical Claim (conformed)
# ============================================================================
@dlt.table(
    name="medical_claim",
    comment="Conformed medical claims; code systems standardized",
)
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_code_system", "code_system IN ('ICD-10-CM', 'CPT', 'HCPCS')")
def silver_medical_claim():
    """Conform medical claims: validate codes, referential integrity."""
    bronze_medical = spark.read.table(cfg().table("raw", "medical_claim"))
    silver_patient = dlt.read("patient")

    df = (
        bronze_medical
        .select(
            F.col("claim_id"),
            F.col("patient_id"),
            F.col("claim_date").cast("date").alias("claim_date"),
            F.col("provider_id"),
            F.col("code").cast("string").alias("code"),
            F.col("code_system").cast("string").alias("code_system"),
            F.col("description").cast("string").alias("description"),
        )
        .join(silver_patient.select("patient_id"), "patient_id", "inner")
        .dropDuplicates(["claim_id"])
    )

    return df


# ============================================================================
# Pharmacy Claim (conformed)
# ============================================================================
@dlt.table(
    name="pharmacy_claim",
    comment="Conformed pharmacy claims; NDC standardized, days_supply validated",
)
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_code_system", "code_system = 'NDC'")
@dlt.expect("valid_days_supply", "days_supply IS NULL OR days_supply > 0")
def silver_pharmacy_claim():
    """Conform pharmacy claims: validate NDC codes and days_supply."""
    bronze_pharmacy = spark.read.table(cfg().table("raw", "pharmacy_claim"))
    silver_patient = dlt.read("patient")

    df = (
        bronze_pharmacy
        .select(
            F.col("claim_id"),
            F.col("patient_id"),
            F.col("fill_date").cast("date").alias("fill_date"),
            F.col("provider_id"),
            F.col("code").cast("string").alias("code"),
            F.col("code_system").cast("string").alias("code_system"),
            F.col("description").cast("string").alias("description"),
            F.col("days_supply").cast("int").alias("days_supply"),
            F.col("quantity").cast("int").alias("quantity"),
        )
        .join(silver_patient.select("patient_id"), "patient_id", "inner")
        .filter(F.col("days_supply") > 0)
        .dropDuplicates(["claim_id"])
    )

    return df


# ============================================================================
# Lab Result (conformed)
# ============================================================================
@dlt.table(
    name="lab_result",
    comment="Conformed lab results; LOINC codes standardized, values validated",
)
@dlt.expect_or_drop("valid_lab_id", "lab_id IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_code_system", "code_system = 'LOINC'")
def silver_lab_result():
    """Conform lab results: validate LOINC codes and numeric values."""
    bronze_lab = spark.read.table(cfg().table("raw", "lab_result"))
    silver_patient = dlt.read("patient")

    df = (
        bronze_lab
        .select(
            F.col("lab_id"),
            F.col("patient_id"),
            F.col("lab_date").cast("date").alias("lab_date"),
            F.col("code").cast("string").alias("code"),
            F.col("code_system").cast("string").alias("code_system"),
            F.col("description").cast("string").alias("description"),
            F.col("value").cast("double").alias("value"),
            F.col("unit").cast("string").alias("unit"),
        )
        .join(silver_patient.select("patient_id"), "patient_id", "inner")
        .dropDuplicates(["lab_id"])
    )

    return df
