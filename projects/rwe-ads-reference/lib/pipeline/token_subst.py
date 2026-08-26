"""Substitute {{token}} placeholders in KB snippet SQL templates.

Tokens include:
  - {{gold}}, {{silver}}, {{serving}} -> fully-qualified schema names from cfg()
  - {{cohort}}, {{registry}} -> intermediate result tables (local within generated SQL)
  - Protocol params: {{dx_codes}}, {{ndc_codes}}, {{min_age}}, {{max_age}}, etc.

List params are quoted and comma-joined. Dates formatted as DATE('YYYY-MM-DD').
Escapes single quotes as \' inside string literals to avoid Spark SQL issues.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def substitute_tokens(
    sql_template: str,
    protocol_spec: dict,
    intermediate_tables: dict | None = None,
) -> str:
    """Substitute {{token}} placeholders in an SQL template.

    Args:
        sql_template: SQL template with {{token}} placeholders
        protocol_spec: Protocol dict with fields like dx_codes (list), study_start (date), etc.
        intermediate_tables: Optional dict mapping {{name}} -> table name for intermediate results
            (e.g., {"cohort": "WITH_clause_cohort_cte"})

    Returns:
        Substituted SQL string with all tokens resolved.

    Raises:
        ValueError: If a required token cannot be resolved.
    """
    try:
        from lib.config import cfg
    except ImportError:
        logger.error("lib.config not available for token substitution")
        return sql_template

    c = cfg()
    intermediate_tables = intermediate_tables or {}

    # Schema tokens
    tokens = {
        "gold": c.serving,
        "silver": c.curated,
        "serving": c.serving,
        "raw": c.raw,
    }

    # Intermediate table tokens
    tokens.update(intermediate_tables)

    # Protocol parameters
    if isinstance(protocol_spec, dict):
        for key, value in protocol_spec.items():
            token_key = key
            tokens[token_key] = _format_param(value)

    # Substitute all {{token}} -> resolved value
    result = sql_template
    for token_key, token_value in tokens.items():
        placeholder = f"{{{{{token_key}}}}}"
        result = result.replace(placeholder, str(token_value))

    # Warn about unresolved tokens
    import re
    unresolved = re.findall(r"\{\{(\w+)\}\}", result)
    if unresolved:
        logger.warning(f"Unresolved tokens after substitution: {unresolved}")
        raise ValueError(f"Unresolved tokens: {unresolved}")

    return result


def _format_param(value: Any) -> str:
    """Format a protocol parameter for SQL insertion.

    Lists are quoted and comma-joined: ['A', 'B'] -> 'A', 'B'
    Dates are wrapped in DATE(): '2024-01-01' -> DATE('2024-01-01')
    Numbers and booleans pass through as strings.
    Strings are escaped (\' for single quotes).
    """
    if isinstance(value, list):
        # Quote each element and join with comma
        formatted = ", ".join(_format_param(item) for item in value)
        return formatted

    if isinstance(value, str):
        # Check if it looks like a date (YYYY-MM-DD)
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return f"DATE('{value}')"
        # Otherwise escape and return quoted
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, bool):
        return "true" if value else "false"

    if value is None:
        return "NULL"

    # Fallback: convert and escape
    return f"'{str(value)}'"


if __name__ == "__main__":
    # Syntax check
    print("token_subst module syntax OK")
