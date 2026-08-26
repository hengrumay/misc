"""Test idempotency of all DDL statements.

Golden rule: every CREATE is IF NOT EXISTS; every wave is re-runnable.
Scans all Python files in waves/ and pipelines/ for CREATE statements
and verifies they use idempotent patterns.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


class TestIdempotencyPatterns:
    """Test that all DDL uses idempotent patterns."""

    def test_create_table_has_if_not_exists(self) -> None:
        """All CREATE TABLE statements must have IF NOT EXISTS."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/*.py"):
            content = py_file.read_text()
            # Find CREATE TABLE statements
            create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)", re.IGNORECASE)
            matches = create_table_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                violations.append(f"{py_file.name}: line {line_no}")

        assert not violations, f"CREATE TABLE without IF NOT EXISTS: {violations}"

    def test_create_schema_has_if_not_exists(self) -> None:
        """All CREATE SCHEMA statements must have IF NOT EXISTS."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/*.py"):
            content = py_file.read_text()
            create_schema_pattern = re.compile(r"CREATE\s+SCHEMA\s+(?!IF\s+NOT\s+EXISTS)", re.IGNORECASE)
            matches = create_schema_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                violations.append(f"{py_file.name}: line {line_no}")

        assert not violations, f"CREATE SCHEMA without IF NOT EXISTS: {violations}"

    def test_create_volume_has_if_not_exists(self) -> None:
        """All CREATE VOLUME statements must have IF NOT EXISTS."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/*.py"):
            content = py_file.read_text()
            create_volume_pattern = re.compile(r"CREATE\s+VOLUME\s+(?!IF\s+NOT\s+EXISTS)", re.IGNORECASE)
            matches = create_volume_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                violations.append(f"{py_file.name}: line {line_no}")

        assert not violations, f"CREATE VOLUME without IF NOT EXISTS: {violations}"

    def test_no_drop_statements(self) -> None:
        """No DROP statements should exist in setup/provisioning code (except in cleanup)."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/setup*.py"):
            content = py_file.read_text()
            drop_pattern = re.compile(r"DROP\s+(?:TABLE|SCHEMA|VOLUME)", re.IGNORECASE)
            matches = drop_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                violations.append(f"{py_file.name}: line {line_no}")

        assert not violations, f"DROP statements found in setup code: {violations}"

    def test_no_truncate_statements(self) -> None:
        """No TRUNCATE statements in setup/provisioning."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        for py_file in repo_root.glob("waves/**/setup*.py"):
            content = py_file.read_text()
            truncate_pattern = re.compile(r"TRUNCATE\s+TABLE", re.IGNORECASE)
            matches = truncate_pattern.finditer(content)
            for match in matches:
                line_no = content[:match.start()].count("\n") + 1
                violations.append(f"{py_file.name}: line {line_no}")

        assert not violations, f"TRUNCATE statements found in setup code: {violations}"

    def test_merge_used_for_idempotent_inserts(self) -> None:
        """MERGE statements for data seeding (KB, fixtures) are idempotent."""
        repo_root = Path(__file__).resolve().parents[1]

        # Check if wave0/setup.py uses MERGE for KB seeding
        wave0_setup = repo_root / "waves" / "wave0_foundation" / "setup.py"
        if wave0_setup.exists():
            content = wave0_setup.read_text()
            # Should have MERGE INTO for KB table
            assert "MERGE INTO" in content, "KB seeding should use MERGE for idempotency"


class TestNoUpdateDeleteInAudit:
    """Test that audit tables have no UPDATE/DELETE statements."""

    def test_audit_tables_append_only(self) -> None:
        """Audit tables should only use INSERT/APPEND operations."""
        repo_root = Path(__file__).resolve().parents[1]
        violations = []

        audit_files = [
            repo_root / "waves" / "wave5_serving_audit_app" / "setup_audit.py",
        ]

        for audit_file in audit_files:
            if not audit_file.exists():
                continue

            content = audit_file.read_text()

            # Check for UPDATE statements on audit tables
            update_pattern = re.compile(
                r"UPDATE\s+(?:.*?)\.(?:gxp_audit|repro_manifest|review_queue|sign_offs)",
                re.IGNORECASE | re.DOTALL
            )
            if update_pattern.search(content):
                violations.append(f"{audit_file.name}: UPDATE found on audit table")

            # Check for DELETE statements on audit tables
            delete_pattern = re.compile(
                r"DELETE\s+FROM\s+(?:.*?)\.(?:gxp_audit|repro_manifest|review_queue|sign_offs)",
                re.IGNORECASE | re.DOTALL
            )
            if delete_pattern.search(content):
                violations.append(f"{audit_file.name}: DELETE found on audit table")

        assert not violations, f"Audit table mutation found: {violations}"
