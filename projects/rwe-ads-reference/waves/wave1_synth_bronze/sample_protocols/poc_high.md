# Study Protocol: Comparative Outcomes with Time-Varying Exposure (poc_high)

## Study Identification
- **Study ID**: poc_high
- **Title**: Comparative Effectiveness and Safety of Statin Intensification in Secondary Prevention of Cardiovascular Disease
- **Complexity**: High

## Objective
To compare the effectiveness and safety of high-intensity vs. moderate-intensity statin therapy in patients with established cardiovascular disease (secondary prevention), evaluating time-varying exposure with dynamic treatment strategies and competing risk adjustments.

## Population
- **Inclusion Criteria**:
  - Age 40-75 years at cohort entry
  - Documented history of coronary artery disease (ICD-10-CM: I25.10) or prior myocardial infarction (ICD-10-CM: I21.*)
  - Currently on a statin medication (NDC codes for atorvastatin, simvastatin, rosuvastatin, pravastatin) at baseline
  - Continuous enrollment for 12 months before and 24 months after cohort entry
- **Exclusion Criteria**:
  - Advanced liver disease (ICD-10-CM: K74.3-K74.6)
  - Stage 4-5 CKD (ICD-10-CM: N18.4-N18.5)
  - Active cancer treatment
  - Prior statin intolerance / myositis

## Index Event & Exposure Definition
- **Index Date**: Date of statin intensity change (from moderate to high intensity OR from high to moderate)
- **Time-Varying Exposure**: 
  - Daily medication exposure tracked from pharmacy fills + days_supply
  - Adherence calculated as proportion of days covered (PDC)
  - Exposure eras defined by collapsing consecutive fills with up to 30-day gaps
  - Changes in intensity during follow-up period are captured as dynamic exposure events

## Covariates (Baseline = 12 months pre-index)
- **Demographics**: Age, sex, race/ethnicity, region
- **Comorbidities**:
  - Diabetes (ICD-10-CM: E11.9)
  - Hypertension (ICD-10-CM: I10)
  - Heart failure (ICD-10-CM: I50.9)
  - Atrial fibrillation (ICD-10-CM: I48.91)
  - CKD stage (ICD-10-CM: N18.1-N18.3)
- **Laboratory Values (most recent baseline)**:
  - LDL cholesterol, HDL, triglycerides (LOINC: 2093-3, 2160-0)
  - eGFR (LOINC: 33914-3)
- **Medication Use**:
  - ACE inhibitors / ARBs
  - Beta-blockers
  - Diuretics
  - Antiplatelets
  - Anticoagulants

## Study Period
- **Start Date**: 2018-01-01
- **End Date**: 2024-12-31
- **Follow-up Duration**: 24 months post-index event

## Study Outcomes
- **Primary Outcome**: Cardiovascular death or non-fatal MI (composite, time-to-event)
- **Secondary Outcomes**:
  - All-cause hospitalization (competing risk)
  - Statin-associated muscle symptom events (proxy: muscle-related office visits)
  - Medication adherence trajectory (PDC by quarter)
  - Healthcare costs
- **Safety Outcomes**:
  - Liver enzyme elevation (lab-based: ALT, AST)
  - Acute kidney injury events

## Statistical Analysis Plan
- **Primary**: Time-varying Cox proportional hazards model adjusting for:
  - Baseline covariates
  - Time-updated medication exposure (statin intensity + PDC)
  - Time-updated laboratory values
  - Propensity score deciles for statin intensification decision
- **Secondary**: 
  - Fine-Gray competing risks regression (accounting for non-CV deaths)
  - Stratified analysis by diabetes status, baseline kidney function
  - Subgroup by age, sex
- **Sensitivity**: 
  - Marginal structural models (MSM) for dose-response
  - Per-protocol analysis (adhere to assigned intensity)

## Validation & Governance
- Statin intensity classification validated against FDA guidance and literature
- Cardiovascular event definitions cross-checked against AHA/ACC criteria
- All diagnosis/procedure/lab codes mapped to standardized code systems
- Approved SQL snippets reviewed for adherence to clinical logic and confidentiality
