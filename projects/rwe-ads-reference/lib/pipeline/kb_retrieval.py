"""Retrieve approved SQL KB snippets by intent via Vector Search or keyword fallback.

The Knowledge Base is Delta table cfg().kb_table with approved snippets versioned and
governed. Snippets are retrieved by semantic similarity against their descriptions,
or keyword/category match if Vector Search is unavailable.

Only snippets with status='approved' are composable. Unapproved snippets are never
returned and logged for audit.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class KBSnippet:
    """Approved SQL snippet from the knowledge base."""
    snippet_id: str
    category: str  # cohort | inclusion | exclusion | derivation | outcome
    description: str
    sql_template: str
    params_json: str
    version: int
    approved_at: str  # ISO timestamp
    approved_by: str  # analyst/reviewer name

    def params(self) -> dict:
        """Parse params_json to dict."""
        try:
            return json.loads(self.params_json)
        except (json.JSONDecodeError, TypeError):
            return {}


def retrieve_approved_snippets(
    intent: str,
    category: str | None = None,
    top_k: int = 5,
) -> list[KBSnippet]:
    """Retrieve approved KB snippets by semantic intent via Vector Search or keyword fallback.

    Args:
        intent: Natural language query (e.g., "prevalence cohort with diagnosis codes")
        category: Optional filter to {cohort, inclusion, exclusion, derivation, outcome}
        top_k: Max number of snippets to return (default 5)

    Returns:
        List of KBSnippet objects (approved only), ranked by relevance. Falls back
        to local keyword match over seed snippets when Vector Search is disabled or
        unavailable; empty list only if even the seed snippets cannot be loaded.

    Raises:
        None (failures are logged; degrades to keyword match / empty list)
    """
    # Resolve config first: vector_search.enabled decides whether we attempt
    # Vector Search at all. Off (default) -> go straight to keyword match.
    try:
        from lib.config import cfg
        c = cfg()
    except Exception as e:
        logger.warning(f"lib.config unavailable ({e}); using keyword match")
        return _keyword_fallback(intent, category, top_k)

    if not c.vector_search_enabled:
        logger.info("Vector Search disabled; using keyword match for KB retrieval")
        return _keyword_fallback(intent, category, top_k)

    # VS enabled: guard the client import, then query with a keyword fallback on error.
    try:
        from databricks.vector_search.client import VectorSearchClient
    except ImportError:
        logger.warning("databricks.vector_search not installed; falling back to keyword match")
        return _keyword_fallback(intent, category, top_k)

    try:
        client = VectorSearchClient()
        results = client.query_index(
            index_name=c.kb_index,
            query_text=intent,
            columns=["snippet_id", "category", "description", "sql_template", "params_json",
                     "version", "approved_at", "approved_by", "status"],
            num_results=top_k * 2,  # Over-fetch to filter unapproved
        )
    except Exception as e:
        logger.warning(f"Vector Search query failed: {e}; falling back to keyword match")
        return _keyword_fallback(intent, category, top_k)

    # Filter to approved only; respect category filter if provided
    approved = []
    for row in results.get("result", {}).get("data_array", []):
        if row.get("status") == "approved":
            if category is None or row.get("category") == category:
                approved.append(KBSnippet(
                    snippet_id=row.get("snippet_id", ""),
                    category=row.get("category", ""),
                    description=row.get("description", ""),
                    sql_template=row.get("sql_template", ""),
                    params_json=row.get("params_json", "{}"),
                    version=row.get("version", 0),
                    approved_at=row.get("approved_at", ""),
                    approved_by=row.get("approved_by", ""),
                ))
        else:
            logger.debug(f"Skipping unapproved snippet {row.get('snippet_id')} (status={row.get('status')})")

    return approved[:top_k]


def _keyword_fallback(
    intent: str,
    category: str | None = None,
    top_k: int = 5,
) -> list[KBSnippet]:
    """Fallback to local keyword/category match over seed snippets.

    Used when Vector Search is unavailable. Matches on description text and category.
    """
    try:
        from waves.wave0_foundation.kb_seeds import SEED_SNIPPETS
    except ImportError:
        logger.error("kb_seeds not available; cannot load seed snippets")
        return []

    intent_lower = intent.lower()
    matches = []

    for snippet_dict in SEED_SNIPPETS:
        # Filter by category if specified
        if category is not None and snippet_dict.get("category") != category:
            continue

        # Match on description keywords
        description = snippet_dict.get("description", "").lower()
        if any(word in description for word in intent_lower.split()):
            matches.append(KBSnippet(
                snippet_id=snippet_dict.get("snippet_id", ""),
                category=snippet_dict.get("category", ""),
                description=snippet_dict.get("description", ""),
                sql_template=snippet_dict.get("sql_template", ""),
                params_json=snippet_dict.get("params_json", "{}"),
                version=1,  # seed version
                approved_at="2026-01-01T00:00:00",  # synthetic timestamp
                approved_by="seed",
            ))

    return matches[:top_k]


if __name__ == "__main__":
    # Local syntax check: import-level validation without executing
    print("kb_retrieval module syntax OK")
