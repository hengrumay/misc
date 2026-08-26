"""Generate synthetic medical claims with diagnoses and procedures (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# Common ICD-10-CM diagnosis codes for realistic RWE
DX_CODES = [
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("I10", "Essential (primary) hypertension"),
    ("I50.9", "Heart failure, unspecified"),
    ("J44.9", "Chronic obstructive pulmonary disease, unspecified"),
    ("M79.3", "Panniculitis, unspecified"),
    ("E78.5", "Lipidemia, unspecified"),
    ("F41.1", "Generalized anxiety disorder"),
    ("M54.5", "Low back pain"),
    ("Z79.4", "Long-term (current) use of insulin"),
    ("R06.02", "Shortness of breath"),
    ("K21.9", "Unspecified gastro-esophageal reflux disease"),
    ("M25.5", "Pain in joint"),
    ("E03.9", "Hypothyroidism, unspecified"),
    ("N18.3", "Chronic kidney disease, stage 3b"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery without angina pectoris"),
]

# Common CPT/HCPCS procedure codes
PROC_CODES = [
    ("99213", "Office visit, established patient"),
    ("99214", "Office visit, established patient, high complexity"),
    ("80053", "Comprehensive metabolic panel"),
    ("85025", "Complete blood count with differential"),
    ("71046", "Chest X-ray, 2 views"),
    ("93000", "12-lead electrocardiogram"),
    ("36415", "Venipuncture"),
    ("90834", "Psychotherapy, 45 minutes"),
    ("99203", "Office visit, new patient"),
    ("96160", "Administration of patient-focused health risk assessment"),
]


def generate_medical_claims(
    patients_df: pd.DataFrame,
    providers_df: pd.DataFrame,
    encounters_df: pd.DataFrame,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate medical claims (diagnosis and procedure records).

    Each row = one diagnosis or procedure on a date for a patient.
    Linked to encounters. Deterministic.

    Args:
        patients_df: patient master list
        providers_df: provider master list
        encounters_df: encounter list (used for realistic event distribution)
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings

    Returns:
        pandas DataFrame with columns:
        - claim_id: string
        - patient_id: string
        - claim_date: date
        - provider_id: string
        - code: string (ICD-10-CM or CPT/HCPCS)
        - code_system: string (ICD-10-CM or CPT)
        - description: string
    """
    np.random.seed(seed)

    if date_range is None:
        date_range = {"start": "2018-01-01", "end": "2024-12-31"}

    study_start = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    study_end = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    provider_ids = providers_df["provider_id"].tolist()
    rows = []
    claim_counter = 0

    for i, patient_row in patients_df.iterrows():
        patient_id = patient_row["patient_id"]
        rng = np.random.RandomState(seed + i)

        # Each patient gets a baseline chronic condition + some acute events
        # ~0-3 chronic conditions per patient
        n_chronic = rng.choice([0, 1, 1, 2, 3], p=[0.20, 0.40, 0.20, 0.15, 0.05])
        # numpy choice needs a 1-D array; DX_CODES is a list of tuples -> pick by index
        _idx = rng.choice(len(DX_CODES), size=min(n_chronic, len(DX_CODES)), replace=False)
        chronic_conditions = [DX_CODES[j] for j in _idx]

        # Add chronic conditions once per patient
        for dx_code, dx_desc in chronic_conditions:
            claim_date = study_start + timedelta(days=rng.randint(0, (study_end - study_start).days))
            provider_id = rng.choice(provider_ids)
            rows.append({
                "claim_id": f"CLM-{claim_counter:012d}",
                "patient_id": patient_id,
                "claim_date": claim_date,
                "provider_id": provider_id,
                "code": dx_code,
                "code_system": "ICD-10-CM",
                "description": dx_desc,
            })
            claim_counter += 1

        # Acute events: 2-6 claims per patient over study period
        n_acute = rng.randint(2, 7)
        for _ in range(n_acute):
            claim_date = study_start + timedelta(days=rng.randint(0, (study_end - study_start).days))
            provider_id = rng.choice(provider_ids)

            # 70% diagnosis, 30% procedure
            if rng.random() < 0.70:
                dx_code, dx_desc = DX_CODES[rng.choice(len(DX_CODES))]
                rows.append({
                    "claim_id": f"CLM-{claim_counter:012d}",
                    "patient_id": patient_id,
                    "claim_date": claim_date,
                    "provider_id": provider_id,
                    "code": dx_code,
                    "code_system": "ICD-10-CM",
                    "description": dx_desc,
                })
            else:
                proc_code, proc_desc = PROC_CODES[rng.choice(len(PROC_CODES))]
                rows.append({
                    "claim_id": f"CLM-{claim_counter:012d}",
                    "patient_id": patient_id,
                    "claim_date": claim_date,
                    "provider_id": provider_id,
                    "code": proc_code,
                    "code_system": "CPT",
                    "description": proc_desc,
                })
            claim_counter += 1

    df = pd.DataFrame(rows)
    return df
