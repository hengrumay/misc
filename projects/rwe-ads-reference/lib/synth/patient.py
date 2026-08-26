"""Generate synthetic patient demographics (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker


def generate_patients(
    n_patients: int = 50000,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate a patient master list with demographics.

    Each row includes: patient_id (UUID), birth_date, sex, race, region.
    Deterministic: same seed => identical data byte-for-byte.

    Args:
        n_patients: number of unique patients to generate
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings (YYYY-MM-DD);
                   used to compute realistic birth_date distribution

    Returns:
        pandas DataFrame with columns:
        - patient_id: string (UUID-like, seeded)
        - birth_date: date
        - sex: string (M, F, U)
        - race: string (White, Black, Hispanic, Asian, Native, Unknown)
        - region: string (US region: NE, MW, S, W, Other)
    """
    np.random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    if date_range is None:
        date_range = {"start": "2018-01-01", "end": "2024-12-31"}

    study_start = datetime.strptime(date_range["start"], "%Y-%m-%d")
    study_end = datetime.strptime(date_range["end"], "%Y-%m-%d")

    # Birth dates: assume patients aged 18-85 at study start
    min_birth = study_start - timedelta(days=85 * 365.25)
    max_birth = study_start - timedelta(days=18 * 365.25)
    birth_dates = [
        min_birth + timedelta(days=float(np.random.uniform(0, (max_birth - min_birth).days)))
        for _ in range(n_patients)
    ]

    sex_dist = np.random.choice(["M", "F", "U"], size=n_patients, p=[0.48, 0.50, 0.02])
    race_dist = np.random.choice(
        ["White", "Black", "Hispanic", "Asian", "Native", "Unknown"],
        size=n_patients,
        p=[0.68, 0.13, 0.11, 0.05, 0.01, 0.02],
    )
    region_dist = np.random.choice(
        ["NE", "MW", "S", "W", "Other"],
        size=n_patients,
        p=[0.20, 0.22, 0.35, 0.20, 0.03],
    )

    # Generate deterministic IDs (fake UUIDs seeded)
    patient_ids = []
    for i in range(n_patients):
        fake.seed_instance(seed + i)
        patient_ids.append(fake.uuid4())

    df = pd.DataFrame({
        "patient_id": patient_ids,
        "birth_date": birth_dates,
        "sex": sex_dist,
        "race": race_dist,
        "region": region_dist,
    })

    return df
