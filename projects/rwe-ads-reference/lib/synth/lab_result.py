"""Generate synthetic lab results with LOINC codes (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# Common LOINC codes for lab tests
LAB_CODES = [
    ("2345-7", "Glucose [Mass/volume] in Serum or Plasma", "mg/dL", 70, 100, 30, 200),
    ("2951-2", "Sodium [Moles/volume] in Serum or Plasma", "mmol/L", 136, 145, 120, 160),
    ("3000-0", "Potassium [Moles/volume] in Serum or Plasma", "mmol/L", 3.5, 5.0, 2.5, 6.5),
    ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma", "mg/dL", 0.7, 1.3, 0.4, 3.0),
    ("2885-2", "Protein [Mass/volume] in Serum or Plasma", "g/dL", 6.0, 8.3, 5.0, 10.0),
    ("1975-2", "Bilirubin.total [Mass/volume] in Serum or Plasma", "mg/dL", 0.2, 1.2, 0.1, 3.0),
    ("2093-3", "Cholesterol [Mass/volume] in Serum or Plasma", "mg/dL", 150, 200, 100, 300),
    ("3043-7", "Triglyceride [Mass/volume] in Serum or Plasma", "mg/dL", 75, 150, 30, 400),
    ("3092-6", "Phosphate [Mass/volume] in Serum or Plasma", "mg/dL", 2.5, 4.5, 1.0, 7.0),
    ("26450-7", "Glucose [Mass/volume] in Capillary blood by Glucometer", "mg/dL", 70, 100, 50, 250),
]


def generate_lab_results(
    patients_df: pd.DataFrame,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate synthetic lab results with LOINC codes.

    Each row = one lab test result for a patient. Realistic normal/abnormal
    value distributions. Deterministic.

    Args:
        patients_df: patient master list
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings

    Returns:
        pandas DataFrame with columns:
        - lab_id: string
        - patient_id: string
        - lab_date: date
        - code: string (LOINC code)
        - code_system: string ('LOINC')
        - description: string
        - value: float (numeric result)
        - unit: string (e.g., mg/dL, mmol/L)
    """
    np.random.seed(seed)

    if date_range is None:
        date_range = {"start": "2018-01-01", "end": "2024-12-31"}

    study_start = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    study_end = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    rows = []
    lab_counter = 0

    for i, patient_row in patients_df.iterrows():
        patient_id = patient_row["patient_id"]
        rng = np.random.RandomState(seed + i)

        # Each patient gets 1-4 lab tests total
        n_labs = rng.choice([0, 1, 2, 3, 4], p=[0.10, 0.40, 0.30, 0.15, 0.05])

        for _ in range(n_labs):
            # Random lab type
            loinc_code, desc, unit, norm_low, norm_high, abs_low, abs_high = LAB_CODES[rng.choice(len(LAB_CODES))]

            # Random date
            lab_date = study_start + timedelta(days=rng.randint(0, (study_end - study_start).days))

            # Value distribution: 70% within normal range, 30% abnormal
            if rng.random() < 0.70:
                value = rng.uniform(norm_low, norm_high)
            else:
                # Abnormal: biased toward higher or lower
                if rng.random() < 0.5:
                    value = rng.uniform(abs_low, norm_low)
                else:
                    value = rng.uniform(norm_high, abs_high)

            # Round to 1-2 decimal places
            value = round(float(value), int(rng.choice([0, 1, 2])))

            rows.append({
                "lab_id": f"LAB-{lab_counter:012d}",
                "patient_id": patient_id,
                "lab_date": lab_date,
                "code": loinc_code,
                "code_system": "LOINC",
                "description": desc,
                "value": value,
                "unit": unit,
            })
            lab_counter += 1

    df = pd.DataFrame(rows)
    return df
