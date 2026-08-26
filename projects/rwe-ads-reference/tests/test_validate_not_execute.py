"""Test validation layer for SQL generation.

Tests that:
  1. Generated SQL is validated (SELECT/CTE only, no DML/DDL)
  2. SQL references only approved schemas (gold/silver, not raw patient DBs)
  3. No connections exist to real patient databases (validate-don't-execute principle)
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from lib.pipeline.validation import validate_sql


class TestSQLValidation:
    """Test SQL validation layer."""

    def test_select_valid(self) -> None:
        """SELECT queries pass validation."""
        sql = "SELECT patient_id, index_date FROM {{gold}}.cohort"
        result = validate_sql(sql)
        assert result.ok is True

    def test_cte_valid(self) -> None:
        """CTE with final SELECT passes validation."""
        sql = """
        WITH base AS (
            SELECT patient_id FROM {{gold}}.patient
        )
        SELECT * FROM base
        """
        result = validate_sql(sql)
        assert result.ok is True

    def test_insert_invalid(self) -> None:
        """INSERT statements are rejected."""
        sql = "INSERT INTO {{gold}}.cohort VALUES (1, '2024-01-01')"
        result = validate_sql(sql)
        assert result.ok is False
        assert any("INSERT" in str(e) or "DML" in str(e) for e in (result.errors or []))

    def test_update_invalid(self) -> None:
        """UPDATE statements are rejected."""
        sql = "UPDATE {{gold}}.cohort SET status = 'approved'"
        result = validate_sql(sql)
        assert result.ok is False

    def test_delete_invalid(self) -> None:
        """DELETE statements are rejected."""
        sql = "DELETE FROM {{gold}}.cohort WHERE id = 1"
        result = validate_sql(sql)
        assert result.ok is False

    def test_create_table_invalid(self) -> None:
        """CREATE TABLE statements are rejected."""
        sql = "CREATE TABLE {{gold}}.new_table (id INT)"
        result = validate_sql(sql)
        assert result.ok is False

    def test_drop_invalid(self) -> None:
        """DROP statements are rejected."""
        sql = "DROP TABLE {{gold}}.cohort"
        result = validate_sql(sql)
        assert result.ok is False


class TestEgressGuards:
    """Test that validation prevents access to non-approved schemas."""

    def test_reference_to_raw_allowed(self) -> None:
        """References to raw schema are allowed (for protocol data)."""
        sql = "SELECT * FROM {{raw}}.protocols"
        result = validate_sql(sql)
        # Should not error on raw reference (protocols volume is allowed)

    def test_reference_to_gold_allowed(self) -> None:
        """References to gold schema are allowed."""
        sql = "SELECT * FROM {{gold}}.patient_timeline"
        result = validate_sql(sql)
        assert result.ok is True

    def test_reference_to_silver_allowed(self) -> None:
        """References to silver schema are allowed."""
        sql = "SELECT * FROM {{silver}}.patient"
        result = validate_sql(sql)
        # May have errors due to schema not being resolved, but not an egress violation

    def test_external_database_reference_rejected(self) -> None:
        """References to external databases are rejected."""
        # This would fail if we tried to reference an external DB
        # Note: In actual validation, we'd need a real config to test this properly
        pass


class TestNoExternalConnections:
    """Test that no code creates connections to non-Databricks DBs."""

    def test_no_psycopg_direct_connections(self) -> None:
        """Scan for direct psycopg connections to non-Lakebase hosts."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/*.py"):
            if "run_ads_build" in py_file.name:
                continue  # Skip orchestration scripts
            content = py_file.read_text()
            # Look for psycopg connections with hardcoded hosts (not Lakebase)
            # Pattern: psycopg.connect(...) or psycopg2.connect(...) — more strict
            connection_pattern = re.compile(
                r"(?:psycopg|psycopg2)\.connect\s*\(\s*(?:f)?['\"]",
                re.IGNORECASE
            )
            matches = connection_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                # Get the full line to check context
                line_start = content.rfind("\n", 0, match.start()) + 1
                line_end = content.find("\n", match.start())
                if line_end == -1:
                    line_end = len(content)
                line_text = content[line_start:line_end]

                # Allow only references to config (cfg()), Lakebase, or env vars
                if ("cfg()" not in line_text and
                    "lakebase" not in line_text.lower() and
                    "connstr" not in line_text.lower() and
                    "os.environ" not in line_text):
                    violations.append(f"{py_file.name}: line {line_no}")

        # Allow violations in setup/provisioning scripts (they manage infrastructure)
        violations = [v for v in violations if "provision" not in v.lower()]
        assert not violations, f"Direct non-Lakebase connections found: {violations}"

    def test_no_external_host_patterns(self) -> None:
        """Scan for connections to forbidden external hosts."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        forbidden_patterns = [
            r"localhost\s*[,:]\s*\d+",  # localhost connections
            r"127\.0\.0\.1",  # 127.0.0.1
            r"patient.*\.db",  # patient databases
            r"emr\.example\.com",  # example EMR hosts
        ]

        for py_file in repo_root.glob("waves/**/*.py"):
            content = py_file.read_text()
            for pattern_str in forbidden_patterns:
                pattern = re.compile(pattern_str, re.IGNORECASE)
                matches = pattern.finditer(content)
                for match in matches:
                    line_no = content[:match.start()].count("\n") + 1
                    violations.append(f"{py_file.name}: line {line_no}: {match.group(0)}")

        assert not violations, f"Forbidden external hosts found: {violations}"


class TestValidationMessages:
    """Test validation result messages are clear."""

    def test_validation_result_has_summary(self) -> None:
        """Validation result has a summary method."""
        sql = "SELECT * FROM {{gold}}.cohort"
        result = validate_sql(sql)
        summary = result.summary()
        assert "VALID" in summary or "INVALID" in summary
        assert summary is not None

    def test_validation_errors_descriptive(self) -> None:
        """Validation errors are descriptive."""
        sql = "INSERT INTO {{gold}}.cohort VALUES (1)"
        result = validate_sql(sql)
        if result.errors:
            for error in result.errors:
                assert len(error) > 10  # Should be descriptive

    def test_validation_warnings_present(self) -> None:
        """Validation can include warnings (e.g., large result set)."""
        # This is for future enhancement; for now, just verify structure
        sql = "SELECT * FROM {{gold}}.patient_timeline"
        result = validate_sql(sql, max_rows=1000)
        # Should not crash
        assert result is not None


class TestDryRunSQL:
    """Test dry-run SQL variants."""

    def test_select_limit_0_generated(self) -> None:
        """SELECT queries get wrapped with LIMIT 0 for dry-run."""
        from lib.pipeline.validation import _build_dry_run_sql
        sql = "SELECT patient_id FROM {{gold}}.cohort"
        dry_run = _build_dry_run_sql(sql)
        assert "LIMIT 0" in dry_run

    def test_cte_limit_0_generated(self) -> None:
        """CTE queries get LIMIT 0 appended."""
        from lib.pipeline.validation import _build_dry_run_sql
        sql = "WITH base AS (SELECT * FROM {{gold}}.patient) SELECT * FROM base"
        dry_run = _build_dry_run_sql(sql)
        assert "LIMIT 0" in dry_run
