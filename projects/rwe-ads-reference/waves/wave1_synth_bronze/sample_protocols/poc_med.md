# Study Protocol: Drug-Exposure New-User Cohort (poc_med)

## Study Identification
- **Study ID**: poc_med
- **Title**: Real-World Effectiveness and Safety of ACE Inhibitors in Hypertension Management
- **Complexity**: Medium

## Objective
To evaluate the real-world effectiveness (blood pressure control) and safety (adverse events) of ACE inhibitors (e.g., Lisinopril) versus other antihypertensive agents in patients with newly diagnosed hypertension.

## Population
- **Inclusion Criteria**:
  - Age 18-75 years at cohort entry
  - Diagnosis of essential hypertension (ICD-10-CM: I10) in the baseline year
  - Continuous enrollment for 12 months before and 12 months after index event
  - No prior antihypertensive medication use in the 12-month baseline period (new-user cohort)
- **Exclusion Criteria**:
  - Secondary hypertension (ICD-10-CM: I15.*)
  - Pregnancy-related hypertension
  - End-stage renal disease (ICD-10-CM: N18.6)
  - Prior myocardial infarction (ICD-10-CM: I21.*)

## Index Event
- **Index Date**: First pharmacy fill of an antihypertensive agent (NDC code in approved list)
- **Exposure**: ACE Inhibitor fill vs. other antihypertensive (baseline categorization)

## Covariates (Baseline = 12 months pre-index)
- Age, sex, race/ethnicity, region
- **Medical History**: 
  - Diabetes (ICD-10-CM: E11.9)
  - Heart failure (ICD-10-CM: I50.9)
  - Chronic kidney disease (ICD-10-CM: N18.3)
  - COPD (ICD-10-CM: J44.9)
- **Medication use**:
  - Diuretics
  - Beta-blockers
  - Statins
- **Utilization**: Number of outpatient visits, emergency visits

## Study Period
- **Start Date**: 2018-01-01
- **End Date**: 2024-12-31
- **Follow-up Duration**: 12 months post-index

## Study Outcomes
- **Primary Outcome**: Mean change in systolic blood pressure from baseline to 12-month follow-up (surrogate: proxy using medication intensification events)
- **Secondary Outcomes**:
  - Medication discontinuation / switch rate
  - Adverse events (cough, hyperkalemia, angioedema) in claims data

## Statistical Analysis
- Propensity score matching to balance exposed vs. unexposed groups
- Stratified analysis by diabetes status, baseline kidney disease

## Validation
- All approved NDC codes cross-checked against validated antihypertensive formularies
- Diagnoses validated against clinical coding standards (ICD-10-CM)
