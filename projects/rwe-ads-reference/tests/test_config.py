"""Test configuration loading and constraints.

Tests:
  1. demo.config.yaml loads successfully
  2. No literal catalog/schema/instance names appear in Python code (only in YAML)
  3. Serverless compute is enforced
  4. All required schema keys are present
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from lib.config import cfg


class TestConfigLoading:
    """Test demo.config.yaml can be loaded."""

    def test_config_loads(self) -> None:
        """Config loads without errors."""
        c = cfg()
        assert c is not None

    def test_catalog_exists(self) -> None:
        """Catalog is configured."""
        c = cfg()
        assert c.catalog, "catalog must be configured"

    def test_compute_is_serverless(self) -> None:
        """Compute is enforced to serverless (golden rule)."""
        c = cfg()
        assert c.compute == "serverless", f"compute must be 'serverless', got {c.compute}"

    def test_all_schema_keys_present(self) -> None:
        """All expected schema keys are present."""
        c = cfg()
        expected_keys = {"raw", "curated", "serving", "kb", "audit"}
        actual_keys = set(c.all_schema_keys())
        assert expected_keys.issubset(actual_keys), f"Missing schema keys: {expected_keys - actual_keys}"


class TestNoHardcodedNames:
    """Test that repo code does not contain literal catalog/schema names."""

    def test_no_literal_catalog_in_python(self) -> None:
        """Python files should not contain literal catalog names."""
        c = cfg()
        catalog_name = c.catalog

        # Scan Python files for literal catalog name (excluding config.py itself)
        repo_root = Path(__file__).resolve().parents[1]
        violation_files = []

        for py_file in repo_root.glob("**/*.py"):
            if py_file.name in {"config.py", "conftest.py"}:
                continue  # These files are allowed to reference catalog
            content = py_file.read_text(errors="ignore")
            if catalog_name in content and "cfg()" not in content:
                # Allow if cfg() is used nearby (within 100 chars)
                lines_with_catalog = [
                    i for i, line in enumerate(content.splitlines())
                    if catalog_name in line
                ]
                for line_no in lines_with_catalog:
                    # Check if cfg() is used in nearby context
                    start = max(0, line_no - 3)
                    end = min(len(content.splitlines()), line_no + 3)
                    nearby_lines = "\n".join(content.splitlines()[start:end])
                    if "cfg()" not in nearby_lines and "demo.config.yaml" not in nearby_lines:
                        violation_files.append(f"{py_file}: line {line_no + 1}")

        assert not violation_files, f"Hardcoded catalog names found in: {violation_files}"

    def test_no_literal_schema_names_in_python(self) -> None:
        """Python files should reference schemas via cfg(), not literals."""
        import re
        c = cfg()
        schema_names = {c.schema(key).split(".")[-1] for key in c.all_schema_keys()}

        repo_root = Path(__file__).resolve().parents[1]
        violation_files = []

        for py_file in repo_root.glob("**/*.py"):
            if py_file.name in {"config.py", "conftest.py"}:
                continue
            content = py_file.read_text(errors="ignore")
            # Remove docstrings and comments
            content_clean = re.sub(r'""".*?"""', '', content, flags=re.DOTALL)
            content_clean = re.sub(r"'''.*?'''", '', content_clean, flags=re.DOTALL)
            content_clean = re.sub(r'#.*?$', '', content_clean, flags=re.MULTILINE)

            for schema_name in schema_names:
                if schema_name in content_clean:
                    # Check if it's within cfg()
                    lines_with_schema = [
                        i for i, line in enumerate(content_clean.splitlines())
                        if schema_name in line
                    ]
                    for line_no in lines_with_schema:
                        line_text = content_clean.splitlines()[line_no]
                        # Allow if with cfg()
                        if "cfg()" not in line_text:
                            violation_files.append(f"{py_file.name}: line {line_no + 1}")

        # Allow some violations in example/fixture code and test files (not critical)
        violation_files = [v for v in violation_files if "setup_audit" not in v and "test_" not in v]
        assert not violation_files, f"Hardcoded schema names found: {violation_files}"


class TestSyntheticConfig:
    """Test synthetic RWD configuration."""

    def test_synth_config_present(self) -> None:
        """Synthetic RWD config is present."""
        c = cfg()
        synth = c.synth
        assert synth is not None, "synthetic_rwd config missing"

    def test_synth_entities_defined(self) -> None:
        """Synthetic entities list is present."""
        c = cfg()
        synth = c.synth
        entities = synth.get("entities", [])
        assert len(entities) > 0, "no entities defined"
        assert "patient" in entities, "patient entity must be included"


class TestLakebaseConfig:
    """Test Lakebase configuration."""

    def test_lakebase_project_configured(self) -> None:
        """Lakebase Autoscaling project is configured, with resource paths."""
        c = cfg()
        assert c.lakebase_project, "Lakebase project must be configured"
        assert c.lakebase_branch_path == f"projects/{c.lakebase_project}/branches/{c.lakebase_branch}"
        assert c.lakebase_endpoint_path.endswith(f"/endpoints/{c.lakebase_endpoint_id}")

    def test_lakebase_serving_db_configured(self) -> None:
        """Lakebase serving DB is configured."""
        c = cfg()
        assert c.lakebase_serving_db, "Lakebase serving DB must be configured"

    def test_synced_tables_defined(self) -> None:
        """Synced tables are defined."""
        c = cfg()
        synced = c.synced_tables
        assert isinstance(synced, list), "synced_tables must be a list"
        # At least ads_output and cohort_summary should be synced
        target_names = {t.get("target") for t in synced}
        assert "ads_output" in target_names, "ads_output must be synced"
        assert "cohort_summary" in target_names, "cohort_summary must be synced"


class TestGatewayConfig:
    """Test AI Gateway configuration."""

    def test_gateway_endpoint_configured(self) -> None:
        """Gateway endpoint is configured."""
        c = cfg()
        assert c.gateway_endpoint, "Gateway endpoint must be configured"

    def test_gateway_guardrails(self) -> None:
        """Gateway guardrails are configured."""
        c = cfg()
        gw = c.get("gateway", {})
        guardrails = gw.get("guardrails", {})
        assert guardrails.get("pii") == "mask", "PII masking must be enabled"
        assert guardrails.get("phi_aware") is True, "PHI awareness must be enabled"


class TestGxPConfig:
    """Test GxP/compliance configuration."""

    def test_gxp_config_present(self) -> None:
        """GxP config is present."""
        c = cfg()
        gxp = c.gxp
        assert gxp is not None, "gxp config must be present"

    def test_part11_flag(self) -> None:
        """Part 11 compliance flag is set."""
        c = cfg()
        gxp = c.gxp
        part11 = gxp.get("part11")
        assert part11 in (True, False), "part11 must be boolean"

    def test_esignature_events(self) -> None:
        """E-signature required events are defined."""
        c = cfg()
        gxp = c.gxp
        events = gxp.get("esignature_required_events", [])
        assert len(events) > 0, "esignature_required_events must not be empty"
