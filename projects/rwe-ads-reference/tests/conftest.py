"""Pytest configuration and fixtures.

Adds repo root to sys.path so lib.* and waves.* imports work.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
