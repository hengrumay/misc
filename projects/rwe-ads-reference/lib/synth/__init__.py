"""Deterministic, seeded synthetic RWE data generators.

Pure Python + Faker + numpy, reproducible by construction. Exports generate_all()
which orchestrates generation of all canonical bronze entities.

When spark is available, writes Delta tables to the bronze schema. Otherwise,
returns pandas DataFrames for local testing.

Entities: patient, enrollment_span, medical_claim, pharmacy_claim, provider,
lab_result, encounter.

Code systems: ICD-10-CM (dx), CPT/HCPCS (proc), NDC (rx), LOINC (lab).
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure lib is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.synth.patient import generate_patients
from lib.synth.provider import generate_providers
from lib.synth.enrollment_span import generate_enrollment_spans
from lib.synth.medical_claim import generate_medical_claims
from lib.synth.pharmacy_claim import generate_pharmacy_claims
from lib.synth.lab_result import generate_lab_results
from lib.synth.encounter import generate_encounters


def generate_all(spark=None) -> dict:
    """Generate all synthetic RWE entities.

    Args:
        spark: SparkSession if available (for writing Delta tables), else None for local testing.

    Returns:
        dict mapping entity name to pandas DataFrame.
    """
    from lib.config import cfg
    cfg_obj = cfg()
    n_patients = cfg_obj.synth.get("n_patients", 50000)
    date_range = cfg_obj.synth.get("date_range", {})
    seed = cfg_obj.synth.get("seed", 20260812)

    print(f"[synth] generating {n_patients} patients, seed={seed}, dates={date_range}")

    # Generate in order (dependencies: providers first, then patients, then events)
    dfs = {}
    dfs["provider"] = generate_providers(n_providers=500, seed=seed)
    dfs["patient"] = generate_patients(
        n_patients=n_patients,
        seed=seed,
        date_range=date_range,
    )
    dfs["enrollment_span"] = generate_enrollment_spans(
        patients_df=dfs["patient"],
        seed=seed,
        date_range=date_range,
    )
    dfs["encounter"] = generate_encounters(
        patients_df=dfs["patient"],
        providers_df=dfs["provider"],
        seed=seed,
        date_range=date_range,
    )
    dfs["medical_claim"] = generate_medical_claims(
        patients_df=dfs["patient"],
        providers_df=dfs["provider"],
        encounters_df=dfs["encounter"],
        seed=seed,
        date_range=date_range,
    )
    dfs["pharmacy_claim"] = generate_pharmacy_claims(
        patients_df=dfs["patient"],
        providers_df=dfs["provider"],
        seed=seed,
        date_range=date_range,
    )
    dfs["lab_result"] = generate_lab_results(
        patients_df=dfs["patient"],
        seed=seed,
        date_range=date_range,
    )

    # Write to bronze if spark is available
    if spark is not None:
        _write_to_bronze(spark, dfs, cfg_obj)

    return dfs


def _write_to_bronze(spark, dfs: dict, cfg_obj) -> None:
    """Write all dataframes to bronze tables (idempotent, overwrite mode)."""
    for table_name, df in dfs.items():
        fqn = cfg_obj.table("raw", table_name)
        print(f"  writing {table_name} ({len(df)} rows) -> {fqn}")
        df_spark = spark.createDataFrame(df)
        df_spark.write.mode("overwrite").saveAsTable(fqn)


if __name__ == "__main__":
    dfs = generate_all(spark=None)
    for name, df in dfs.items():
        print(f"{name}: {len(df)} rows")
