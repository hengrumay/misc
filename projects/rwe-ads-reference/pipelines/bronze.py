"""Bronze Layer — Spark Declarative Pipeline (DLT).

Raw synthetic RWE data. Re-declares or ingests pre-generated Delta tables
from lib/synth without transformation.

Serverless-compatible. Uses dlt decorators for Databricks Declarative Pipelines.
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

# dlt import — only available in DLT pipeline context
try:
    import dlt
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover
    # Allow file to import-check locally without Spark/dlt installed
    pass

from lib.config import cfg


@dlt.table(
    name="patient",
    comment="Patient master list with demographics (synthetic RWE)",
)
def bronze_patient():
    """Read patient master list from raw synthetic data."""
    c = cfg()
    # Try to read pre-generated table; if it doesn't exist, fall back to empty schema
    try:
        return spark.read.table(c.table("raw", "patient"))
    except Exception:
        # Return empty DataFrame with correct schema for first run
        from pyspark.sql.types import StructType, StructField, StringType, DateType
        schema = StructType([
            StructField("patient_id", StringType(), False),
            StructField("birth_date", DateType(), True),
            StructField("sex", StringType(), True),
            StructField("race", StringType(), True),
            StructField("region", StringType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="provider",
    comment="Provider master list (synthetic RWE)",
)
def bronze_provider():
    """Read provider master list from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "provider"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("provider_id", StringType(), False),
            StructField("specialty", StringType(), True),
            StructField("region", StringType(), True),
            StructField("organization_id", StringType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="enrollment_span",
    comment="Insurance eligibility spans (synthetic RWE)",
)
def bronze_enrollment_span():
    """Read enrollment spans from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "enrollment_span"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, DateType
        schema = StructType([
            StructField("patient_id", StringType(), False),
            StructField("span_start", DateType(), True),
            StructField("span_end", DateType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="encounter",
    comment="Patient encounters/visits (synthetic RWE)",
)
def bronze_encounter():
    """Read encounters from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "encounter"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, DateType
        schema = StructType([
            StructField("encounter_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("provider_id", StringType(), True),
            StructField("encounter_date", DateType(), True),
            StructField("encounter_type", StringType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="medical_claim",
    comment="Medical claims with diagnoses and procedures (synthetic RWE)",
)
def bronze_medical_claim():
    """Read medical claims from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "medical_claim"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, DateType
        schema = StructType([
            StructField("claim_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("claim_date", DateType(), True),
            StructField("provider_id", StringType(), True),
            StructField("code", StringType(), True),
            StructField("code_system", StringType(), True),
            StructField("description", StringType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="pharmacy_claim",
    comment="Pharmacy claims with medications and days_supply (synthetic RWE)",
)
def bronze_pharmacy_claim():
    """Read pharmacy claims from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "pharmacy_claim"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
        schema = StructType([
            StructField("claim_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("fill_date", DateType(), True),
            StructField("provider_id", StringType(), True),
            StructField("code", StringType(), True),
            StructField("code_system", StringType(), True),
            StructField("description", StringType(), True),
            StructField("days_supply", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="lab_result",
    comment="Lab results with LOINC codes (synthetic RWE)",
)
def bronze_lab_result():
    """Read lab results from raw synthetic data."""
    c = cfg()
    try:
        return spark.read.table(c.table("raw", "lab_result"))
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
        schema = StructType([
            StructField("lab_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("lab_date", DateType(), True),
            StructField("code", StringType(), True),
            StructField("code_system", StringType(), True),
            StructField("description", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
        ])
        return spark.createDataFrame([], schema)
