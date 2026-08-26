"""Generate synthetic encounters (visits) (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_encounters(
    patients_df: pd.DataFrame,
    providers_df: pd.DataFrame,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate patient encounters (office visits, hospitalizations, etc).

    Each encounter is a visit to a provider on a date. Deterministic.

    Args:
        patients_df: patient master list
        providers_df: provider master list
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings

    Returns:
        pandas DataFrame with columns:
        - encounter_id: string (UUID-like)
        - patient_id: string
        - provider_id: string
        - encounter_date: date
        - encounter_type: string (office, inpatient, emergency, telehealth)
    """
    np.random.seed(seed)

    if date_range is None:
        date_range = {"start": "2018-01-01", "end": "2024-12-31"}

    study_start = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    study_end = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    provider_ids = providers_df["provider_id"].tolist()
    encounter_types = ["office", "inpatient", "emergency", "telehealth"]

    rows = []
    encounter_counter = 0

    for i, patient_row in patients_df.iterrows():
        patient_id = patient_row["patient_id"]
        rng = np.random.RandomState(seed + i)

        # Average: 3-8 encounters per patient per year
        n_days = (study_end - study_start).days
        n_years = n_days / 365.25
        expected_encounters = rng.uniform(3, 8) * n_years
        actual_encounters = rng.poisson(expected_encounters)

        for _ in range(int(actual_encounters)):
            # Random date within study range
            days_offset = rng.randint(0, n_days)
            encounter_date = study_start + timedelta(days=days_offset)

            # Random provider
            provider_id = rng.choice(provider_ids)

            # Type distribution: mostly office visits
            encounter_type = rng.choice(encounter_types, p=[0.70, 0.10, 0.15, 0.05])

            rows.append({
                "encounter_id": f"ENC-{encounter_counter:012d}",
                "patient_id": patient_id,
                "provider_id": provider_id,
                "encounter_date": encounter_date,
                "encounter_type": encounter_type,
            })
            encounter_counter += 1

    df = pd.DataFrame(rows)
    return df
