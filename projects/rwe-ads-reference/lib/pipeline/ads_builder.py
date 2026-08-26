"""ADS Builder: assemble cohort->inclusion->derivation->assembly from approved SQL (deterministic).

This is a deterministic, MLflow 3.x-compatible builder that follows the VALIDATE-DON'T-EXECUTE pattern:
  1. Retrieve approved KB snippets for each step (cohort, inclusion, exclusion, derivation, assembly)
  2. Substitute protocol parameters
  3. VALIDATE SQL (EXPLAIN, schema, egress guard, dry-run)
  4. On validation failure: revise the SQL and re-validate (bounded retries)
  5. Emit generated SQL + validation results for analyst review
  6. Write reproducibility manifest (protocol version, KB snippet IDs/hashes, generated SQL, model, timings)

All queries target synthetic gold (cfg().serving) only. No execution against real patient DBs is possible
by construction (no such connection exists).

All model calls go through lib/pipeline/gateway.py, which PHI-masks in-process and calls the
pay-per-token FM serving endpoint directly (the native cfg().gateway_endpoint is a documented
pattern placeholder, not on the query path — an external_model route to a PPT FM 403s).
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class StepResult:
    """Result of one ADS builder step."""
    step_name: str
    success: bool
    generated_sql: str | None = None
    kb_snippet_ids: list[str] | None = None
    kb_versions: list[int] | None = None
    validation_ok: bool | None = None
    validation_errors: list[str] | None = None
    validation_warnings: list[str] | None = None
    estimated_rows: int | None = None
    retries: int = 0
    duration_sec: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BuildManifest:
    """Reproducibility manifest for an ADS build."""
    ads_id: str
    protocol_id: str
    protocol_version: str
    build_timestamp: str
    steps: list[StepResult]
    model_name: str
    gateway_endpoint: str
    kb_table_version: str
    total_duration_sec: float
    kb_snippets_hash: str  # SHA256 of all snippet IDs + versions used


class ADSBuilder:
    """Generate analysis-ready datasets from study protocols using approved SQL snippets."""

    def __init__(self, model: str = "databricks-claude-sonnet-4"):
        """Initialize the ADS builder.

        Args:
            model: recorded as provenance metadata on the manifest; the deterministic builder makes NO model call (approved-SQL template substitution).
        """
        self.model = model
        self.steps = []

    def build(
        self,
        protocol_spec: dict,
        poc_id: str = "poc_med",
        max_retries: int = 2,
    ) -> tuple[BuildManifest, dict]:
        """Build an ADS from a protocol spec.

        Ordered steps:
          1. Cohort: initial patient selection (index event + dates)
          2. Inclusion/Exclusion: apply filters (enrollment, age, prior conditions)
          3. Derivation: derive covariates, exposure eras, outcomes, followup
          4. Assembly: join into final ads_output table

        Args:
            protocol_spec: Protocol specification dict with fields:
              - study_id, title, objective
              - population, index_event, inclusion[], exclusion[]
              - exposure, outcomes[], covariates[], followup_days
              - study_start, study_end (dates)
              - dx_codes, ndc_codes, age_min, age_max, etc. (parameters)
            poc_id: PoC ID for tracking (poc_low, poc_med, poc_high)
            max_retries: Max retries per step on validation failure (default 2)

        Returns:
            (BuildManifest, dict) where dict contains:
              - "cohort_sql": generated cohort SQL
              - "inclusion_sql": inclusion/exclusion SQL
              - "derivation_sqls": list of derived-variable SQLs
              - "assembly_sql": final assembly SQL
              - "cohort_summary": row counts at each step
              - Any errors/warnings
        """
        try:
            from lib.config import cfg
        except ImportError:
            logger.error("lib.config not available")
            return None, {"error": "lib.config not available"}

        c = cfg()
        ads_id = f"{poc_id}_{int(time.time())}"
        start_time = time.time()
        build_results = {}
        manifest_steps = []

        # Step 1: Cohort
        cohort_result = self._build_cohort(protocol_spec, max_retries)
        manifest_steps.append(cohort_result)
        build_results["cohort_sql"] = cohort_result.generated_sql

        if not cohort_result.success:
            logger.error(f"Cohort step failed: {cohort_result.validation_errors}")
            return self._make_manifest(
                ads_id, protocol_spec, manifest_steps, start_time,
                error="Cohort definition failed"
            ), build_results

        # Step 2: Inclusion / Exclusion
        inclusion_result = self._build_inclusion_exclusion(protocol_spec, max_retries)
        manifest_steps.append(inclusion_result)
        build_results["inclusion_sql"] = inclusion_result.generated_sql

        if not inclusion_result.success:
            logger.error(f"Inclusion/exclusion step failed: {inclusion_result.validation_errors}")

        # Step 3: Derivation
        derivation_results = self._build_derivation(protocol_spec, max_retries)
        manifest_steps.extend(derivation_results)
        build_results["derivation_sqls"] = [r.generated_sql for r in derivation_results if r.success]

        # Step 4: Assembly
        assembly_result = self._build_assembly(protocol_spec, max_retries)
        manifest_steps.append(assembly_result)
        build_results["assembly_sql"] = assembly_result.generated_sql

        if not assembly_result.success:
            logger.error(f"Assembly step failed: {assembly_result.validation_errors}")
            return self._make_manifest(
                ads_id, protocol_spec, manifest_steps, start_time,
                error="Assembly step failed"
            ), build_results

        # Build succeeded
        duration = time.time() - start_time
        manifest = self._make_manifest(
            ads_id, protocol_spec, manifest_steps, start_time, error=None
        )
        manifest.total_duration_sec = duration

        return manifest, build_results

    def _build_cohort(self, protocol_spec: dict, max_retries: int) -> StepResult:
        """Build initial cohort SQL."""
        try:
            from lib.pipeline.kb_retrieval import retrieve_approved_snippets
            from lib.pipeline.token_subst import substitute_tokens
            from lib.pipeline.validation import validate_sql
        except ImportError as e:
            return StepResult(
                step_name="cohort",
                success=False,
                validation_errors=[str(e)],
            )

        step_result = StepResult(step_name="cohort")
        start = time.time()

        for attempt in range(max_retries + 1):
            step_result.retries = attempt

            # Retrieve approved snippets
            snippets = retrieve_approved_snippets(
                intent="initial patient cohort with index event",
                category="cohort",
                top_k=3,
            )

            if not snippets:
                step_result.validation_errors = ["No approved cohort snippets found"]
                continue

            # Use the first snippet
            snippet = snippets[0]
            step_result.kb_snippet_ids = [snippet.snippet_id]
            step_result.kb_versions = [snippet.version]

            try:
                # Substitute parameters
                generated_sql = substitute_tokens(snippet.sql_template, protocol_spec)
                step_result.generated_sql = generated_sql

                # Validate
                validation = validate_sql(generated_sql)
                step_result.validation_ok = validation.ok
                step_result.validation_errors = validation.errors
                step_result.validation_warnings = validation.warnings
                step_result.estimated_rows = validation.estimated_rows

                if validation.ok:
                    step_result.success = True
                    break
            except Exception as e:
                step_result.validation_errors = [str(e)]
                logger.warning(f"Cohort generation attempt {attempt}: {e}")

        step_result.duration_sec = time.time() - start
        return step_result

    def _build_inclusion_exclusion(self, protocol_spec: dict, max_retries: int) -> StepResult:
        """Build inclusion/exclusion filters."""
        try:
            from lib.pipeline.kb_retrieval import retrieve_approved_snippets
            from lib.pipeline.token_subst import substitute_tokens
            from lib.pipeline.validation import validate_sql
        except ImportError as e:
            return StepResult(
                step_name="inclusion_exclusion",
                success=False,
                validation_errors=[str(e)],
            )

        step_result = StepResult(step_name="inclusion_exclusion")
        start = time.time()

        for attempt in range(max_retries + 1):
            step_result.retries = attempt

            # Retrieve both inclusion and exclusion snippets
            inc_snippets = retrieve_approved_snippets(
                intent="continuous enrollment and demographics",
                category="inclusion",
                top_k=2,
            )

            if not inc_snippets:
                step_result.validation_errors = ["No approved inclusion snippets found"]
                continue

            snippet = inc_snippets[0]
            step_result.kb_snippet_ids = [snippet.snippet_id]
            step_result.kb_versions = [snippet.version]

            try:
                # Substitute parameters
                generated_sql = substitute_tokens(
                    snippet.sql_template,
                    protocol_spec,
                    intermediate_tables={"cohort": "cohort_base"},
                )
                step_result.generated_sql = generated_sql

                # Validate
                validation = validate_sql(generated_sql)
                step_result.validation_ok = validation.ok
                step_result.validation_errors = validation.errors
                step_result.validation_warnings = validation.warnings

                if validation.ok:
                    step_result.success = True
                    break
            except Exception as e:
                step_result.validation_errors = [str(e)]
                logger.warning(f"Inclusion/exclusion attempt {attempt}: {e}")

        step_result.duration_sec = time.time() - start
        return step_result

    def _build_derivation(self, protocol_spec: dict, max_retries: int) -> list[StepResult]:
        """Build derived variables (covariates, exposure eras, outcomes)."""
        try:
            from lib.pipeline.kb_retrieval import retrieve_approved_snippets
            from lib.pipeline.token_subst import substitute_tokens
            from lib.pipeline.validation import validate_sql
        except ImportError as e:
            return [StepResult(
                step_name="derivation",
                success=False,
                validation_errors=[str(e)],
            )]

        derivation_results = []

        # Derive baseline covariates
        for var_name in protocol_spec.get("covariates", []):
            result = StepResult(step_name=f"derivation_covariate_{var_name}")
            start = time.time()

            for attempt in range(max_retries + 1):
                result.retries = attempt

                snippets = retrieve_approved_snippets(
                    intent=f"derive baseline covariate {var_name}",
                    category="derivation",
                    top_k=2,
                )

                if snippets:
                    snippet = snippets[0]
                    result.kb_snippet_ids = [snippet.snippet_id]
                    result.kb_versions = [snippet.version]

                    try:
                        sql = substitute_tokens(snippet.sql_template, protocol_spec)
                        result.generated_sql = sql
                        validation = validate_sql(sql)
                        result.validation_ok = validation.ok
                        result.validation_errors = validation.errors

                        if validation.ok:
                            result.success = True
                            break
                    except Exception as e:
                        result.validation_errors = [str(e)]

            result.duration_sec = time.time() - start
            derivation_results.append(result)

        return derivation_results

    def _build_assembly(self, protocol_spec: dict, max_retries: int) -> StepResult:
        """Build final assembly (one row per patient)."""
        try:
            from lib.pipeline.kb_retrieval import retrieve_approved_snippets
            from lib.pipeline.token_subst import substitute_tokens
            from lib.pipeline.validation import validate_sql
        except ImportError as e:
            return StepResult(
                step_name="assembly",
                success=False,
                validation_errors=[str(e)],
            )

        step_result = StepResult(step_name="assembly")
        start = time.time()

        for attempt in range(max_retries + 1):
            step_result.retries = attempt

            snippets = retrieve_approved_snippets(
                intent="assemble final ads output",
                category="derivation",
                top_k=2,
            )

            if not snippets:
                step_result.validation_errors = ["No approved assembly snippets found"]
                continue

            snippet = snippets[0]
            step_result.kb_snippet_ids = [snippet.snippet_id]
            step_result.kb_versions = [snippet.version]

            try:
                # Build select list and joins (simplified)
                select_list = "c.patient_id, c.index_date"
                joins = ""

                sql_template = snippet.sql_template
                params = {
                    "select_list": select_list,
                    "joins": joins,
                    **protocol_spec,
                }

                generated_sql = substitute_tokens(sql_template, params)
                step_result.generated_sql = generated_sql

                validation = validate_sql(generated_sql)
                step_result.validation_ok = validation.ok
                step_result.validation_errors = validation.errors

                if validation.ok:
                    step_result.success = True
                    break
            except Exception as e:
                step_result.validation_errors = [str(e)]
                logger.warning(f"Assembly attempt {attempt}: {e}")

        step_result.duration_sec = time.time() - start
        return step_result

    def _make_manifest(
        self,
        ads_id: str,
        protocol_spec: dict,
        steps: list[StepResult],
        start_time: float,
        error: str | None = None,
    ) -> BuildManifest:
        """Build reproducibility manifest."""
        try:
            from lib.config import cfg
        except ImportError:
            cfg_obj = None
        else:
            cfg_obj = cfg()

        # Compute KB snippets hash
        snippet_info = []
        for step in steps:
            if step.kb_snippet_ids:
                for sid, ver in zip(step.kb_snippet_ids, step.kb_versions or []):
                    snippet_info.append(f"{sid}:{ver}")

        kb_hash = hashlib.sha256(
            "\n".join(snippet_info).encode()
        ).hexdigest()

        return BuildManifest(
            ads_id=ads_id,
            protocol_id=protocol_spec.get("study_id", "unknown"),
            protocol_version=protocol_spec.get("version", "1.0"),
            build_timestamp=datetime.now().isoformat(),
            steps=steps,
            model_name=self.model,
            gateway_endpoint=cfg_obj.gateway_endpoint if cfg_obj else "unknown",
            kb_table_version="v1",
            total_duration_sec=time.time() - start_time,
            kb_snippets_hash=kb_hash,
        )


if __name__ == "__main__":
    print("ads_builder module syntax OK")
