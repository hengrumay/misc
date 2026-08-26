"""Generate synthetic enrollment/eligibility spans (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


def generate_enrollment_spans(
    patients_df: pd.DataFrame,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate insurance eligibility spans for each patient.

    Realistically, most patients have 1-3 enrollment spans covering the study
    period, with occasional gaps (when they switched insurance).

    Args:
        patients_df: patient master list (used for patient_ids)
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings

    Returns:
        pandas DataFrame with columns:
        - patient_id: string
        - span_start: date
        - span_end: date
    """
    np.random.seed(seed)

    if date_range is None:
        date_range = {"start": "2018-01-01", "end": "2024-12-31"}

    study_start = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    study_end = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    rows = []
    for i, row in patients_df.iterrows():
        patient_id = row["patient_id"]
        # Seed per-patient randomness based on patient index + global seed
        rng = np.random.RandomState(seed + i)

        # Most patients have 1-3 spans
        n_spans = rng.choice([1, 1, 1, 2, 2, 3], p=[0.50, 0.30, 0.10, 0.07, 0.02, 0.01])

        current_date = study_start
        for _ in range(n_spans):
            if current_date >= study_end:
                break

            # Random span length: typically 1-3 years
            span_length_days = rng.randint(365, 1095)
            span_end = min(current_date + timedelta(days=span_length_days), study_end)

            rows.append({
                "patient_id": patient_id,
                "span_start": current_date,
                "span_end": span_end,
            })

            # Optional gap between spans (30-90 days, 10% chance)
            if rng.random() < 0.10:
                gap = rng.randint(30, 90)
                current_date = span_end + timedelta(days=gap)
            else:
                current_date = span_end

    df = pd.DataFrame(rows)
    return df
