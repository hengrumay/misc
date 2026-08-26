"""Generate synthetic pharmacy claims with medications (deterministic, seeded)."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# Common NDC codes (drug identifiers) for chronic medications + acute treatments
NDC_CODES = [
    ("00093-5117-16", "Lisinopril 20mg tablet (hypertension)"),
    ("00456-2009-60", "Metformin 500mg tablet (diabetes)"),
    ("00185-0734-11", "Atorvastatin 20mg tablet (lipids)"),
    ("00054-0165-24", "Omeprazole 20mg capsule (GERD)"),
    ("00781-2158-10", "Albuterol sulfate inhalation solution (asthma)"),
    ("50090-0781-25", "Sertraline 100mg tablet (depression/anxiety)"),
    ("68001-0217-30", "Levothyroxine 75mcg tablet (hypothyroidism)"),
    ("54889-0450-60", "Ibuprofen 600mg tablet (pain)"),
    ("00069-0061-70", "Amoxicillin 500mg capsule (infection)"),
    ("00002-1373-60", "Metoprolol 50mg tablet (hypertension/heart)"),
]


def generate_pharmacy_claims(
    patients_df: pd.DataFrame,
    providers_df: pd.DataFrame,
    seed: int = 20260812,
    date_range: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate pharmacy claims (medication fills with days_supply).

    Each row = one pharmacy fill for a patient. Realistic days_supply
    for chronic vs acute medications. Deterministic.

    Args:
        patients_df: patient master list
        providers_df: provider master list (for prescribers)
        seed: random seed for reproducibility
        date_range: dict with 'start' and 'end' date strings

    Returns:
        pandas DataFrame with columns:
        - claim_id: string
        - patient_id: string
        - fill_date: date
        - provider_id: string (prescriber)
        - code: string (NDC code)
        - code_system: string ('NDC')
        - description: string
        - days_supply: int
        - quantity: int
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

        # Number of distinct medications: 0-8 per patient (realistic)
        n_meds = rng.choice([0, 1, 1, 2, 2, 3, 4, 5], p=[0.10, 0.20, 0.20, 0.20, 0.15, 0.10, 0.03, 0.02])
        # numpy choice needs a 1-D array; NDC_CODES is a list of tuples -> pick by index
        _idx = rng.choice(len(NDC_CODES), size=min(n_meds, len(NDC_CODES)), replace=False)
        selected_ndcs = [NDC_CODES[j] for j in _idx]

        # Generate fills for each medication (some get refilled multiple times)
        for ndc_code, ndc_desc in selected_ndcs:
            # Probability this is a chronic medication (refilled multiple times)
            is_chronic = rng.random() < 0.75

            if is_chronic:
                # Multiple fills spread throughout study period
                start_date = study_start + timedelta(days=rng.randint(0, 180))
                n_fills = rng.randint(4, 24)  # ~1-2 fills per year for chronic meds
                days_supply = int(rng.choice([30, 60, 90], p=[0.60, 0.30, 0.10]))
            else:
                # Single or a few fills (acute/short-term)
                start_date = study_start + timedelta(days=rng.randint(0, (study_end - study_start).days))
                n_fills = rng.choice([1, 2], p=[0.80, 0.20])
                days_supply = int(rng.choice([7, 10, 14], p=[0.50, 0.30, 0.20]))

            fill_date = start_date
            for _ in range(n_fills):
                if fill_date > study_end:
                    break

                provider_id = rng.choice(provider_ids)
                quantity = rng.randint(10, 90)

                rows.append({
                    "claim_id": f"RX-{claim_counter:012d}",
                    "patient_id": patient_id,
                    "fill_date": fill_date,
                    "provider_id": provider_id,
                    "code": ndc_code,
                    "code_system": "NDC",
                    "description": ndc_desc,
                    "days_supply": days_supply,
                    "quantity": quantity,
                })
                claim_counter += 1

                # Next fill: supply_end + small gap (1-3 days)
                fill_date = fill_date + timedelta(days=days_supply + rng.randint(1, 3))

    df = pd.DataFrame(rows)
    return df
