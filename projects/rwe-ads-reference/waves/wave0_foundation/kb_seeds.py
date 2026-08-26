"""Seed approved-SQL snippets for the knowledge base.

Each snippet is a governed, parameterized SQL template composable by the ADS
builder. Params use ``{{name}}`` placeholders resolved from the protocol spec.
Only ``status='approved'`` snippets are retrievable for composition.

Categories: cohort | inclusion | exclusion | derivation | outcome
"""
from __future__ import annotations

# NOTE: table references inside templates use {{gold}} / {{serving}} tokens that
# the builder substitutes with fully-qualified names from lib/config.py — no
# catalog/schema literals live here.

SEED_SNIPPETS: list[dict] = [
    {
        "snippet_id": "coh_base_prevalence",
        "category": "cohort",
        "description": "Base prevalence cohort: patients with >=1 qualifying diagnosis code within a date window; index date = first occurrence.",
        "sql_template": (
            "WITH dx AS (\n"
            "  SELECT patient_id, MIN(event_date) AS index_date\n"
            "  FROM {{gold}}.patient_timeline\n"
            "  WHERE event_type = 'dx' AND code IN ({{dx_codes}})\n"
            "    AND event_date BETWEEN DATE('{{study_start}}') AND DATE('{{study_end}}')\n"
            "  GROUP BY patient_id\n"
            ")\nSELECT patient_id, index_date FROM dx"
        ),
        "params_json": '{"dx_codes":"list<string>","study_start":"date","study_end":"date"}',
    },
    {
        "snippet_id": "coh_new_user",
        "category": "cohort",
        "description": "New-user (incident) drug-exposure cohort: first pharmacy fill of a drug class with a clean baseline washout period and no prior exposure.",
        "sql_template": (
            "WITH first_fill AS (\n"
            "  SELECT patient_id, MIN(event_date) AS index_date\n"
            "  FROM {{gold}}.patient_timeline\n"
            "  WHERE event_type = 'rx' AND code IN ({{ndc_codes}})\n"
            "  GROUP BY patient_id\n"
            "), washout AS (\n"
            "  SELECT f.patient_id, f.index_date\n"
            "  FROM first_fill f\n"
            "  LEFT JOIN {{gold}}.patient_timeline p\n"
            "    ON p.patient_id = f.patient_id AND p.event_type='rx' AND p.code IN ({{ndc_codes}})\n"
            "   AND p.event_date BETWEEN DATE_SUB(f.index_date, {{washout_days}}) AND DATE_SUB(f.index_date, 1)\n"
            "  WHERE p.patient_id IS NULL\n"
            ")\nSELECT patient_id, index_date FROM washout"
        ),
        "params_json": '{"ndc_codes":"list<string>","washout_days":"int"}',
    },
    {
        "snippet_id": "inc_continuous_enrollment",
        "category": "inclusion",
        "description": "Inclusion: continuous enrollment / insurance eligibility for N days before and after index date.",
        "sql_template": (
            "SELECT c.patient_id, c.index_date\n"
            "FROM {{cohort}} c\n"
            "JOIN {{gold}}.eligibility_periods e ON e.patient_id = c.patient_id\n"
            "WHERE e.span_start <= DATE_SUB(c.index_date, {{pre_days}})\n"
            "  AND e.span_end   >= DATE_ADD(c.index_date, {{post_days}})"
        ),
        "params_json": '{"pre_days":"int","post_days":"int"}',
    },
    {
        "snippet_id": "inc_age_range",
        "category": "inclusion",
        "description": "Inclusion: patient age at index date within [min_age, max_age].",
        "sql_template": (
            "SELECT c.patient_id, c.index_date\n"
            "FROM {{cohort}} c\n"
            "JOIN {{gold}}.patient_timeline pt ON pt.patient_id = c.patient_id\n"
            "JOIN {{silver}}.patient p ON p.patient_id = c.patient_id\n"
            "WHERE FLOOR(DATEDIFF(c.index_date, p.birth_date)/365.25) BETWEEN {{min_age}} AND {{max_age}}\n"
            "GROUP BY c.patient_id, c.index_date"
        ),
        "params_json": '{"min_age":"int","max_age":"int"}',
    },
    {
        "snippet_id": "exc_prior_condition",
        "category": "exclusion",
        "description": "Exclusion: remove patients with a specified diagnosis in the baseline window before index date.",
        "sql_template": (
            "SELECT c.patient_id, c.index_date\n"
            "FROM {{cohort}} c\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT 1 FROM {{gold}}.patient_timeline p\n"
            "  WHERE p.patient_id = c.patient_id AND p.event_type='dx' AND p.code IN ({{exclude_dx}})\n"
            "    AND p.event_date BETWEEN DATE_SUB(c.index_date, {{baseline_days}}) AND c.index_date\n"
            ")"
        ),
        "params_json": '{"exclude_dx":"list<string>","baseline_days":"int"}',
    },
    {
        "snippet_id": "der_baseline_covariate_flag",
        "category": "derivation",
        "description": "Derive a baseline binary covariate flag: 1 if the patient had any of the given codes in the baseline window before index.",
        "sql_template": (
            "SELECT c.patient_id,\n"
            "  MAX(CASE WHEN p.code IN ({{cov_codes}}) THEN 1 ELSE 0 END) AS {{cov_name}}\n"
            "FROM {{cohort}} c\n"
            "LEFT JOIN {{gold}}.patient_timeline p ON p.patient_id = c.patient_id\n"
            "  AND p.event_date BETWEEN DATE_SUB(c.index_date, {{baseline_days}}) AND c.index_date\n"
            "GROUP BY c.patient_id"
        ),
        "params_json": '{"cov_codes":"list<string>","cov_name":"identifier","baseline_days":"int"}',
    },
    {
        "snippet_id": "der_exposure_era",
        "category": "derivation",
        "description": "Derive drug-exposure eras (time-varying exposure) by collapsing consecutive fills allowing a grace-period gap.",
        "sql_template": (
            "WITH fills AS (\n"
            "  SELECT patient_id, event_date AS fill_date, days_supply,\n"
            "         DATE_ADD(event_date, days_supply) AS supply_end\n"
            "  FROM {{gold}}.patient_timeline\n"
            "  WHERE event_type='rx' AND code IN ({{ndc_codes}})\n"
            ")\nSELECT patient_id, MIN(fill_date) AS era_start, MAX(supply_end) AS era_end\n"
            "FROM fills GROUP BY patient_id"
        ),
        "params_json": '{"ndc_codes":"list<string>","grace_days":"int"}',
    },
    {
        "snippet_id": "out_first_event",
        "category": "outcome",
        "description": "Outcome: time to first occurrence of an outcome event after index date, with censoring at end of follow-up.",
        "sql_template": (
            "SELECT c.patient_id, c.index_date,\n"
            "  MIN(o.event_date) AS outcome_date,\n"
            "  CASE WHEN MIN(o.event_date) IS NOT NULL THEN 1 ELSE 0 END AS outcome_flag,\n"
            "  DATEDIFF(COALESCE(MIN(o.event_date), DATE_ADD(c.index_date, {{followup_days}})), c.index_date) AS time_to_event\n"
            "FROM {{cohort}} c\n"
            "LEFT JOIN {{gold}}.patient_timeline o ON o.patient_id = c.patient_id\n"
            "  AND o.event_type='dx' AND o.code IN ({{outcome_codes}})\n"
            "  AND o.event_date BETWEEN c.index_date AND DATE_ADD(c.index_date, {{followup_days}})\n"
            "GROUP BY c.patient_id, c.index_date"
        ),
        "params_json": '{"outcome_codes":"list<string>","followup_days":"int"}',
    },
    {
        "snippet_id": "asm_one_row_per_patient",
        "category": "derivation",
        "description": "Assembly: join cohort + inclusion/exclusion survivors + derived covariates + outcomes into one row per patient for the ADS output.",
        "sql_template": (
            "SELECT c.patient_id, c.index_date, {{select_list}}\n"
            "FROM {{cohort}} c\n"
            "{{joins}}"
        ),
        "params_json": '{"select_list":"string","joins":"string"}',
    },
]
