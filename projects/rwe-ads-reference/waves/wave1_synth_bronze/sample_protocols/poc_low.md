# Study Protocol: Simple Prevalence Cohort (poc_low)

## Study Identification
- **Study ID**: poc_low
- **Title**: Prevalence of Type 2 Diabetes in a Managed Care Population
- **Complexity**: Low

## Objective
To estimate the prevalence of Type 2 Diabetes Mellitus among adult patients in a managed care population during the study period.

## Population
- **Inclusion Criteria**:
  - Enrolled in the health plan for the entire study period (continuous enrollment required)
  - Age 18-85 years at study index date
  - At least 1 inpatient or 2 outpatient encounters in the baseline year
- **Exclusion Criteria**:
  - Prior diagnosis of Type 1 Diabetes Mellitus (ICD-10-CM: E10.*)
  - End-stage renal disease (ICD-10-CM: N18.6)

## Index Event
- **Index Date**: First occurrence of Type 2 Diabetes diagnosis (ICD-10-CM: E11.9) on or after January 1, 2018

## Covariates
- Age at index date
- Sex
- Race/Ethnicity
- Region

## Study Period
- **Start Date**: 2018-01-01
- **End Date**: 2024-12-31
- **Follow-up**: Not applicable (point-in-time prevalence)

## Study Outcomes
- **Primary Outcome**: Prevalence of Type 2 Diabetes (proportion of patients with qualifying diagnosis)

## Data Sources
- Synthetic real-world data (RWD) from ads_raw schema
- Diagnoses: ICD-10-CM codes from medical claims
- Enrollment information from insurance claims

## Statistical Considerations
- No special handling of missing data (all synthetic)
- Count patients with at least one qualifying diagnosis code
- Stratify by age group (18-40, 41-65, 66+), sex, and region
