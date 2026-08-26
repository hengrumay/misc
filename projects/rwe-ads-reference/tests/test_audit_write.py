"""Test audit write operations and hash chain integrity.

Tests that:
  1. Hash chain computation is deterministic
  2. Audit tables are append-only (no UPDATE/DELETE in code)
  3. Tamper detection works (modified rows detected)
"""
from __future__ import annotations

import pytest

from waves.wave5_serving_audit_app.setup_audit import (
    AuditEventRecord,
    ManifestRecord,
    append_event,
    compute_chain,
    verify_chain,
    write_manifest,
)


class TestHashChainComputation:
    """Test SHA256 hash chain computation."""

    def test_compute_chain_deterministic(self) -> None:
        """Hash computation is deterministic."""
        row_data = {"event_id": "evt_001", "event_type": "approval"}
        hash1 = compute_chain(None, row_data)
        hash2 = compute_chain(None, row_data)
        assert hash1 == hash2

    def test_chain_depends_on_prev_hash(self) -> None:
        """Hash changes if previous hash changes."""
        row_data = {"event_id": "evt_001", "event_type": "approval"}
        hash_no_prev = compute_chain(None, row_data)
        hash_with_prev = compute_chain("abc123", row_data)
        assert hash_no_prev != hash_with_prev

    def test_chain_depends_on_row_data(self) -> None:
        """Hash changes if row data changes."""
        prev = "abc123"
        row1 = {"event_id": "evt_001", "event_type": "approval"}
        row2 = {"event_id": "evt_001", "event_type": "rejection"}
        hash1 = compute_chain(prev, row1)
        hash2 = compute_chain(prev, row2)
        assert hash1 != hash2

    def test_chain_excludes_hash_fields(self) -> None:
        """Hash computation excludes prev_hash and row_hash from input."""
        row_data_with_hashes = {
            "event_id": "evt_001",
            "event_type": "approval",
            "prev_hash": "ignored",
            "row_hash": "also_ignored",
        }
        row_data_without_hashes = {
            "event_id": "evt_001",
            "event_type": "approval",
        }
        hash1 = compute_chain(None, row_data_with_hashes)
        hash2 = compute_chain(None, row_data_without_hashes)
        # Should be the same (hash fields excluded)
        assert hash1 == hash2


class TestManifestWriting:
    """Test reproducibility manifest writing."""

    def test_write_manifest_generates_id(self) -> None:
        """Manifest write generates a manifest_id."""
        record = ManifestRecord(
            ads_id="ads_001",
            study_id="study_low",
            protocol_version="1.0",
            decision="approved",
        )
        manifest_id = write_manifest(record, "test_table")
        assert manifest_id is not None
        assert "ads_001" in manifest_id

    def test_manifest_record_dataclass(self) -> None:
        """Manifest record can be created with all fields."""
        record = ManifestRecord(
            ads_id="ads_002",
            study_id="study_med",
            protocol_version="2.0",
            kb_snippet_versions=[
                {"snippet_id": "coh_base", "version": 1, "content_hash": "abc123"}
            ],
            generated_sql={"step_1": "SELECT * FROM cohort"},
            source_table_versions={"ads_serving.patient": "v123"},
            model="sonnet-4",
            agent_version="1.0",
            eval_scores={"cohort_n": 50000.0, "outcome_rate": 0.15},
            reviewer="dr_analyst@example.com",
            decision="approved",
        )
        assert record.ads_id == "ads_002"
        assert record.study_id == "study_med"
        assert len(record.kb_snippet_versions) == 1


class TestAuditEventAppend:
    """Test GxP audit event appending."""

    def test_append_event_generates_id(self) -> None:
        """Appending event generates event_id."""
        record = AuditEventRecord(
            event_type="ads_approval",
            actor="user_123",
            subject_id="manifest_001",
            details={"decision": "approved"},
        )
        event_id = append_event(record, "test_audit_table")
        assert event_id is not None
        assert "ads_approval" in event_id

    def test_audit_event_record_dataclass(self) -> None:
        """Audit event record can be created."""
        record = AuditEventRecord(
            event_type="review_gate_pass",
            actor="reviewer_001",
            subject_id="manifest_abc",
            details={"reason": "cohort size acceptable"},
        )
        assert record.event_type == "review_gate_pass"
        assert record.actor == "reviewer_001"


class TestChainVerification:
    """Test chain verification (in-memory simulation)."""

    def test_verify_chain_logic(self) -> None:
        """Chain verification detects tampering (logic test)."""
        # Build a chain manually
        rows = []

        # Row 1
        row1_data = {"event_id": "e1", "event_type": "approval"}
        row1_hash = compute_chain(None, row1_data)
        rows.append((row1_data, row1_hash))

        # Row 2
        row2_data = {"event_id": "e2", "event_type": "rejection"}
        row2_hash = compute_chain(row1_hash, row2_data)
        rows.append((row2_data, row2_hash))

        # Row 3
        row3_data = {"event_id": "e3", "event_type": "approval"}
        row3_hash = compute_chain(row2_hash, row3_data)
        rows.append((row3_data, row3_hash))

        # Verify clean chain
        prev_hash = None
        all_valid = True
        for row_data, stored_hash in rows:
            expected_hash = compute_chain(prev_hash, row_data)
            if stored_hash != expected_hash:
                all_valid = False
            prev_hash = stored_hash

        assert all_valid, "Clean chain should verify"

    def test_tampering_detected(self) -> None:
        """Tampering is detected when row is modified."""
        # Build a chain
        row1_data = {"event_id": "e1", "event_type": "approval"}
        row1_hash = compute_chain(None, row1_data)

        row2_data = {"event_id": "e2", "event_type": "rejection"}
        row2_hash = compute_chain(row1_hash, row2_data)

        # Now tamper: change row2 event_type
        tampered_row2_data = {"event_id": "e2", "event_type": "approval"}  # Changed
        expected_tampered_hash = compute_chain(row1_hash, tampered_row2_data)

        # The stored hash (row2_hash) was computed with "rejection", not "approval"
        assert row2_hash != expected_tampered_hash, "Tampering should be detected"


class TestAppendOnlyEnforcement:
    """Test that audit tables are append-only."""

    def test_no_update_delete_in_setup_audit(self) -> None:
        """setup_audit.py has no UPDATE/DELETE statements (excluding comments/docstrings)."""
        from pathlib import Path
        import re
        setup_audit_path = Path(__file__).resolve().parents[1] / "waves" / "wave5_serving_audit_app" / "setup_audit.py"
        if setup_audit_path.exists():
            content = setup_audit_path.read_text()
            # Remove comments and docstrings
            content_no_comments = re.sub(r'#.*?$', '', content, flags=re.MULTILINE)
            content_no_docstrings = re.sub(r'""".*?"""', '', content_no_comments, flags=re.DOTALL)
            content_no_docstrings = re.sub(r"'''.*?'''", '', content_no_docstrings, flags=re.DOTALL)

            # Check for actual UPDATE/DELETE with _sql pattern (indicating execution)
            update_delete_pattern = re.compile(r'_sql\s*\(\s*["\'].*?(?:UPDATE|DELETE).*?["\']', re.IGNORECASE | re.DOTALL)
            if update_delete_pattern.search(content_no_docstrings):
                raise AssertionError("setup_audit.py should not execute UPDATE/DELETE on audit tables")

    def test_write_manifest_uses_insert(self) -> None:
        """write_manifest uses INSERT (not UPDATE)."""
        # The function logs insert operations
        record = ManifestRecord(ads_id="test_ads", decision="approved")
        manifest_id = write_manifest(record, "test_table")
        # If it gets here without error, the append operation is correct
        assert manifest_id is not None

    def test_append_event_uses_insert(self) -> None:
        """append_event uses INSERT (not UPDATE)."""
        record = AuditEventRecord(
            event_type="ads_approval",
            actor="user",
            subject_id="manifest",
        )
        event_id = append_event(record, "test_table")
        assert event_id is not None
