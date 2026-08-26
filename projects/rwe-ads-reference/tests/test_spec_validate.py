"""Tests for the Stage-1 deterministic protocol-spec validators.

Pure Python — no Spark / no auth / no model. These are the hard gate: a spec
that fails here must not be auto-e-signed. The failure fixtures mirror the real
realism-test findings from the extraction-realism prototype, so
this proves the validator flags the same catastrophic classes the prototype did.
"""
from __future__ import annotations

from lib.pipeline.spec_validate import validate_code, validate_spec


CLEAN_HIGH = {
    "study_id": "poc_high", "complexity": "high",
    "ndc_codes": ["00054-0165-24"], "exclude_dx": ["N18.3", "K21.9"],
    "outcome_codes": ["I50.9"], "dx_codes": ["I21.9"],
    "study_start": "2018-01-01", "study_end": "2024-12-31",
}


class TestValidateCode:
    def test_well_formed_icd_ndc_loinc_pass(self):
        assert validate_code("I50.9", ("ICD10",)) is None
        assert validate_code("00054-0165-24", ("NDC",)) is None
        assert validate_code("2160-0", ("LOINC",)) is None

    def test_drug_name_in_ndc_is_malformed(self):
        # standardized specs are uppercased; a drug name is not an NDC literal
        assert validate_code("ATORVASTATIN", ("NDC",)) is not None

    def test_prose_in_icd_field_flagged(self):
        assert validate_code("HEART FAILURE HOSPITALIZATION", ("ICD10",)) is not None

    def test_unexpanded_range_flagged(self):
        d = validate_code("K74.3-K74.6", ("ICD10",))
        assert d is not None and "range" in d

    def test_wildcard_flagged(self):
        d = validate_code("I21*", ("ICD10",))
        assert d is not None and "wildcard" in d


class TestValidateSpecGate:
    def test_clean_spec_passes(self):
        res = validate_spec(CLEAN_HIGH)
        assert res["ok"] is True
        assert res["hard_fail_reasons"] == []

    def test_drug_names_in_ndc_hard_fail(self):
        res = validate_spec({**CLEAN_HIGH, "ndc_codes": ["ATORVASTATIN", "LISINOPRIL"]})
        assert res["ok"] is False
        assert any("ndc_codes" in r for r in res["hard_fail_reasons"])

    def test_prose_outcomes_hard_fail(self):
        res = validate_spec({**CLEAN_HIGH,
                             "outcome_codes": ["HEART FAILURE HOSPITALIZATION"]})
        assert res["ok"] is False
        assert any("outcome_codes" in r for r in res["hard_fail_reasons"])

    def test_range_and_wildcard_hard_fail(self):
        res = validate_spec({**CLEAN_HIGH, "exclude_dx": ["K74.3-K74.6", "I21*"]})
        assert res["ok"] is False
        assert res["n_hard_fails"] >= 2

    def test_missing_outcome_codes_hard_fail(self):
        spec = {**CLEAN_HIGH}
        spec["outcome_codes"] = []
        res = validate_spec(spec)
        assert res["ok"] is False
        assert any("outcome_codes" in r for r in res["hard_fail_reasons"])

    def test_missing_ndc_for_high_hard_fail(self):
        spec = {**CLEAN_HIGH}
        spec["ndc_codes"] = []
        res = validate_spec(spec)
        assert res["ok"] is False
        assert any("ndc_codes" in r for r in res["hard_fail_reasons"])

    def test_value_in_wrong_field_message(self):
        # a well-formed ICD code sitting in ndc_codes -> explicit wrong-field reason
        res = validate_spec({**CLEAN_HIGH, "ndc_codes": ["I50.9"]})
        assert res["ok"] is False
        assert any("value-in-wrong-field" in r for r in res["hard_fail_reasons"])

    def test_covariates_coded_json_codes_validated(self):
        import json
        cov = json.dumps([{"name": "cov_bad", "codes": ["NOT A CODE"]}])
        res = validate_spec({**CLEAN_HIGH, "covariates_coded": cov})
        assert res["ok"] is False
        assert any("covariates_coded" in r for r in res["hard_fail_reasons"])
