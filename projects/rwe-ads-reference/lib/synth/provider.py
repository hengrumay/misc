"""Generate synthetic provider master list (deterministic, seeded)."""
from __future__ import annotations

import numpy as np
import pandas as pd
from faker import Faker


def generate_providers(
    n_providers: int = 500,
    seed: int = 20260812,
) -> pd.DataFrame:
    """Generate a provider master list.

    Each row includes: provider_id, specialty, region, organization_id.
    Deterministic: same seed => identical data.

    Args:
        n_providers: number of unique providers to generate
        seed: random seed for reproducibility

    Returns:
        pandas DataFrame with columns:
        - provider_id: string (UUID-like, seeded)
        - specialty: string (MD, DO, RN, NP, PA, Pharmacist, Lab, Other)
        - region: string (US region)
        - organization_id: string (group/hospital affiliation)
    """
    np.random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    specialties = ["MD", "DO", "RN", "NP", "PA", "Pharmacist", "Lab", "Other"]
    regions = ["NE", "MW", "S", "W", "Other"]
    orgs = [f"ORG-{i:06d}" for i in range(1, 51)]  # 50 organizations

    provider_ids = []
    for i in range(n_providers):
        fake.seed_instance(seed + i)
        provider_ids.append(fake.uuid4())

    specialty_dist = np.random.choice(specialties, size=n_providers)
    region_dist = np.random.choice(regions, size=n_providers)
    org_dist = np.random.choice(orgs, size=n_providers)

    df = pd.DataFrame({
        "provider_id": provider_ids,
        "specialty": specialty_dist,
        "region": region_dist,
        "organization_id": org_dist,
    })

    return df
