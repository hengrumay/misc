"""Build/refresh the Vector Search index over the approved-SQL KB (flag-gated).

Serverless job entrypoint (``build_kb_index``). Vector Search is OPTIONAL and
OFF by default: keyword match in ``lib/pipeline/kb_retrieval.py`` is the default
retrieval path and needs no endpoint.

Behaviour:
  * ``vector_search.enabled`` false (default) -> clean no-op: log and return
    ``{"status": "disabled"}``. No endpoint or index is created.
  * enabled but the Databricks SDK is absent (CODE-ONLY checkout) -> return
    ``{"status": "code_only"}`` without touching the workspace.
  * enabled and SDK present -> idempotent create: ensure the Vector Search
    endpoint (``cfg().vs_endpoint``) exists, then ensure a TRIGGERED delta-sync
    index (``cfg().kb_index``) over ``cfg().kb_table`` that embeds ``description``
    with ``cfg().embedding_model`` (databricks-gte-large-en), primary key
    ``snippet_id``.

Approved-only handling: the delta-sync index syncs the whole KB table; the
``status='approved'`` filter is applied at retrieval time in kb_retrieval (which
over-fetches, then drops non-approved rows), matching the existing query path.

Idempotent: existence-checked create; safe to re-run. All names resolve through
lib/config.py (cfg()); no hardcoded literals.

NOTE: creating a Vector Search endpoint is a billable operator step. This module
is not runtime-verified in code-only checkouts; the operator flips the flag,
provides/creates the endpoint, and runs the job.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _endpoint_exists(w, endpoint_name: str) -> bool:
    """True if a Vector Search endpoint of this name already exists."""
    try:
        w.vector_search_endpoints.get_endpoint(endpoint_name=endpoint_name)
        return True
    except Exception:
        return False


def _index_exists(w, index_name: str) -> bool:
    """True if a Vector Search index of this name already exists."""
    try:
        w.vector_search_indexes.get_index(index_name=index_name)
        return True
    except Exception:
        return False


def main():
    """Job entrypoint for building/refreshing the KB Vector Search index."""
    try:
        from lib.config import cfg
    except ImportError:
        logger.error("lib.config not available")
        return {"status": "error", "message": "lib.config not available"}

    c = cfg()

    # Flag gate: off by default -> clean no-op (NOT a fake success).
    if not c.vector_search_enabled:
        logger.info("Vector Search disabled; KB retrieval uses keyword match")
        return {
            "status": "disabled",
            "message": "Vector Search disabled (vector_search.enabled=false); "
                       "KB retrieval uses keyword match",
            "index": c.kb_index,
            "endpoint": c.vs_endpoint,
            "kb_table": c.kb_table,
        }

    # CODE-ONLY guard: SDK not installed -> report, do not touch the workspace.
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        logger.warning("databricks-sdk not installed; CODE-ONLY mode, no index created")
        logger.info(f"Would create index {c.kb_index} on endpoint {c.vs_endpoint}")
        return {
            "status": "code_only",
            "message": "Vector Search enabled but databricks-sdk not installed; "
                       "index creation skipped in CODE-ONLY mode",
            "index": c.kb_index,
            "endpoint": c.vs_endpoint,
            "kb_table": c.kb_table,
        }

    w = WorkspaceClient()

    # 1. Ensure the endpoint (idempotent). Endpoint creation is asynchronous.
    endpoint_existed = _endpoint_exists(w, c.vs_endpoint)
    if not endpoint_existed:
        logger.info(f"Creating Vector Search endpoint: {c.vs_endpoint}")
        try:
            w.vector_search_endpoints.create_endpoint(
                name=c.vs_endpoint,
                endpoint_type="STANDARD",
            )
        except Exception as e:
            logger.error(f"Endpoint create failed for {c.vs_endpoint}: {e}")
            return {"status": "error", "message": f"endpoint create failed: {e}",
                    "endpoint": c.vs_endpoint}

    # 2. Ensure the index (idempotent). create_index needs the endpoint ONLINE; if
    #    it was just created it may still be provisioning -> report and let the
    #    operator re-run (safe: existence-checked, no duplicate created).
    if _index_exists(w, c.kb_index):
        logger.info(f"Index already exists: {c.kb_index}; triggering a sync")
        try:
            w.vector_search_indexes.sync_index(index_name=c.kb_index)
        except Exception as e:
            logger.warning(f"sync_index on {c.kb_index} failed (non-fatal): {e}")
        return {"status": "exists", "index": c.kb_index, "endpoint": c.vs_endpoint,
                "kb_table": c.kb_table}

    logger.info(f"Creating delta-sync index {c.kb_index} over {c.kb_table}")
    try:
        w.vector_search_indexes.create_index(
            name=c.kb_index,
            endpoint_name=c.vs_endpoint,
            primary_key="snippet_id",
            index_type="DELTA_SYNC",
            delta_sync_index_spec={
                "source_table": c.kb_table,
                "embedding_source_columns": [
                    {
                        "name": "description",
                        "embedding_model_endpoint_name": c.embedding_model,
                    }
                ],
                "pipeline_type": "TRIGGERED",
            },
        )
    except Exception as e:
        # Most common cause on a fresh endpoint: still provisioning. Re-run once
        # the endpoint is ONLINE (existence-checked, so re-run is safe).
        logger.error(f"Index create failed for {c.kb_index}: {e}")
        if not endpoint_existed:
            return {
                "status": "endpoint_provisioning",
                "message": f"endpoint {c.vs_endpoint} was just created and may still "
                           "be provisioning; re-run build_kb_index once it is ONLINE",
                "index": c.kb_index,
                "endpoint": c.vs_endpoint,
            }
        return {"status": "error", "message": f"index create failed: {e}",
                "index": c.kb_index, "endpoint": c.vs_endpoint}

    result = {
        "status": "created",
        "index": c.kb_index,
        "endpoint": c.vs_endpoint,
        "source_table": c.kb_table,
        "embedding_model": c.embedding_model,
        "primary_key": "snippet_id",
        "pipeline_type": "TRIGGERED",
    }
    logger.info(f"Index build result: {result}")
    return result


if __name__ == "__main__":
    print("build_index job entrypoint; syntax OK")
    # In a real Databricks job this is invoked via `databricks bundle run`:
    # result = main()
    # print(result)
