"""Tests for the deterministic protocol standardization (FIXED RULES stage).

Pure Python — no Spark / no auth. Validates the rules that turn raw ai_extract
output into the coded spec the ADS Builder consumes, and that the composition
recipes reference only known KB snippets.
"""
from __future__ import annotations

import json
import pytest

from lib.pipeline.protocol_standardize import standardize_extraction


VALID_HIGH = {
    "study_id": "poc_high", "complexity": "High",
    "ndc_codes": ["00054-0165-24"], "exclude_dx": ["N18.3", "K21.9"],
    "covariate_codes": ["E11.9", "I10"], "outcome_codes": ["I50.9"],
    "study_start": "2018-01-01", "study_end": "2024-12-31",
    "min_age": 40, "max_age": 75, "washout_days": 365, "followup_days": 730,
}


class TestStandardizeHappyPath:
    def test_valid_high_complexity_ok(self):
        spec, res = standardize_extraction(VALID_HIGH)
        assert res["ok"] is True and res["errors"] == []
        assert spec["complexity"] == "high"
        assert spec["ndc_codes"] == ["00054-0165-24"]
        assert spec["outcome_codes"] == ["I50.9"]
        assert spec["followup_days"] == 730

    def test_covariates_coded_built_from_codes(self):
        spec, _ = standardize_extraction(VALID_HIGH)
        cov = json.loads(spec["covariates_coded"])
        assert cov == [{"name": "cov_e11_9", "codes": ["E11.9"]},
                       {"name": "cov_i10", "codes": ["I10"]}]

    def test_defaults_emit_warnings(self):
        spec, res = standardize_extraction(VALID_HIGH)
        # pre_days/post_days/baseline_days not provided -> defaulted
        assert spec["pre_days"] == 90 and spec["post_days"] == 90
        assert any("pre_days" in w for w in res["warnings"])


class TestStandardizeValidation:
    def test_low_requires_dx_codes(self):
        _, res = standardize_extraction({"study_id": "x", "complexity": "low",
                                         "outcome_codes": ["I50.9"], "study_start": "2018-01-01",
                                         "study_end": "2024-12-31"})
        assert res["ok"] is False
        assert any("dx_codes" in e for e in res["errors"])

    def test_medium_requires_ndc_codes(self):
        _, res = standardize_extraction({"study_id": "x", "complexity": "medium",
                                         "outcome_codes": ["I50.9"], "study_start": "2018-01-01",
                                         "study_end": "2024-12-31"})
        assert res["ok"] is False
        assert any("ndc_codes" in e for e in res["errors"])

    def test_missing_study_id_and_dates_fail(self):
        _, res = standardize_extraction({"complexity": "low", "dx_codes": ["E11.9"]})
        assert res["ok"] is False
        assert any("study_id" in e for e in res["errors"])
        assert any("study_start" in e for e in res["errors"])

    def test_invalid_complexity_fails(self):
        _, res = standardize_extraction({"study_id": "x", "complexity": "extreme",
                                         "dx_codes": ["E11.9"], "outcome_codes": ["I50.9"],
                                         "study_start": "2018-01-01", "study_end": "2024-12-31"})
        assert res["ok"] is False
        assert any("complexity" in e for e in res["errors"])


class TestCodeNormalization:
    def test_wildcards_and_case_and_dedupe(self):
        spec, _ = standardize_extraction({**VALID_HIGH,
                                          "exclude_dx": ["n18.3", "N18.3", "i21.*"]})
        assert spec["exclude_dx"] == ["N18.3", "I21"]  # lowered->upper, wildcard stripped, deduped

    def test_age_swap_when_inverted(self):
        spec, res = standardize_extraction({**VALID_HIGH, "min_age": 80, "max_age": 40})
        assert spec["min_age"] == 40 and spec["max_age"] == 80
        assert any("swapped" in w for w in res["warnings"])


class TestCompositionRecipes:
    def test_recipes_reference_known_snippets(self):
        from waves.wave3_ads_build.ads_build_core import RECIPES
        from waves.wave0_foundation.kb_seeds import SEED_SNIPPETS
        known = {s["snippet_id"] for s in SEED_SNIPPETS}
        for complexity, recipe in RECIPES.items():
            assert recipe["cohort"] in known, f"{complexity} cohort snippet missing"
            for _, snippet_id in recipe["narrow"]:
                assert snippet_id in known, f"{complexity} narrow snippet {snippet_id} missing"

    def test_all_complexities_covered(self):
        from waves.wave3_ads_build.ads_build_core import RECIPES, POC_SPECS
        assert set(RECIPES) == {"low", "medium", "high"}
        # every offline POC spec's complexity has a recipe
        for poc, spec in POC_SPECS.items():
            assert spec["complexity"] in RECIPES
