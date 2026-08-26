"""Single point of name resolution for the RWE ADS Automation build.

Every catalog / schema / table / endpoint / instance name in the repo resolves
through this module, which reads ``demo.config.yaml``. Nothing else in the
codebase may hardcode those literals. To migrate to another workspace, edit
``demo.config.yaml`` only.

Works both:
  * inside Databricks (jobs / SDP / notebooks) — bundle passes ``config_path``
    or it is discovered relative to this file, and
  * locally (app backend, tests) — discovered relative to the repo root.

Bundle overrides: any environment variable ``ADS_<UPPER_DOTTED>`` overrides the
matching config key, so DAB ``variables`` can inject per-target values without
editing YAML (e.g. ``ADS_CATALOG``, ``ADS_WORKSPACE__WAREHOUSE_ID``).
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - yaml always present in our envs
    yaml = None


def _find_config() -> Path:
    """Locate demo.config.yaml: explicit env, then walk up from this file."""
    env = os.environ.get("ADS_CONFIG_PATH")
    if env and Path(env).is_file():
        return Path(env)
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / "demo.config.yaml"
        if candidate.is_file():
            return candidate
    # Databricks workspace files fallback
    for candidate in (Path("/Workspace/demo.config.yaml"), Path("./demo.config.yaml")):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("demo.config.yaml not found; set ADS_CONFIG_PATH")


@lru_cache(maxsize=1)
def _raw() -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to read demo.config.yaml")
    with open(_find_config()) as fh:
        return yaml.safe_load(fh)


class Config:
    """Typed accessors over demo.config.yaml with env-var override support."""

    def __init__(self, data: dict[str, Any] | None = None):
        self._d = data if data is not None else _raw()

    # -- generic dotted access with ADS_ env override --------------------
    def get(self, dotted: str, default: Any = None) -> Any:
        env_key = "ADS_" + dotted.upper().replace(".", "__")
        if env_key in os.environ:
            return os.environ[env_key]
        node: Any = self._d
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    # -- identity --------------------------------------------------------
    @property
    def initiative(self) -> str:
        return self.get("initiative")

    @property
    def catalog(self) -> str:
        return self.get("catalog")

    @property
    def host(self) -> str:
        return self.get("workspace.host")

    @property
    def warehouse_id(self) -> str:
        return str(self.get("workspace.warehouse_id"))

    @property
    def compute(self) -> str:
        return self.get("workspace.compute", "serverless")

    # -- schemas (fully qualified: catalog.schema) -----------------------
    def schema(self, key: str) -> str:
        """key in {raw, curated, serving, kb, audit} -> 'catalog.schema'."""
        return f"{self.catalog}.{self.get(f'schemas.{key}')}"

    @property
    def raw(self) -> str:
        return self.schema("raw")

    @property
    def curated(self) -> str:
        return self.schema("curated")

    @property
    def serving(self) -> str:
        return self.schema("serving")

    @property
    def kb_schema(self) -> str:
        return self.schema("kb")

    @property
    def audit(self) -> str:
        return self.schema("audit")

    def table(self, schema_key: str, name: str) -> str:
        return f"{self.schema(schema_key)}.{name}"

    # -- volume ----------------------------------------------------------
    @property
    def protocols_volume(self) -> str:
        # config stores 'ads_raw.protocols' (schema.volume); prefix the catalog
        rel = self.get("volumes.protocols")
        return f"{self.catalog}.{rel}"

    @property
    def protocols_volume_path(self) -> str:
        parts = self.protocols_volume.split(".")
        return f"/Volumes/{parts[0]}/{parts[1]}/{parts[2]}"

    # -- lakebase (Autoscaling: project -> branch -> endpoint -> database) ---
    # The Provisioned Lakebase tier (static instances) is retired.
    # Autoscaling uses a Project whose creation auto-provisions a
    # `production` branch + `primary` read-write endpoint + the
    # `databricks_postgres` database. `databricks postgres create-project
    # <project>` is the one documented prereq (like the pre-existing-catalog
    # rule); after that `bundle deploy` binds a DB that already exists.
    @property
    def lakebase_project(self) -> str:
        """Autoscaling project id (was the Provisioned `instance` name)."""
        return self.get("lakebase.project")

    @property
    def lakebase_branch(self) -> str:
        """Branch id (the auto-created production branch by default)."""
        return self.get("lakebase.branch", "production")

    @property
    def lakebase_endpoint_id(self) -> str:
        """Endpoint id (the auto-created primary read-write endpoint by default)."""
        return self.get("lakebase.endpoint", "primary")

    @property
    def lakebase_bound_db(self) -> str:
        """Postgres DB the app's `postgres` resource binds. It exists the instant
        the project is created, so `bundle deploy` never 404s. The app reaches the
        real serving/app DBs by dbname-override on the same endpoint."""
        return self.get("lakebase.bound_database", "databricks_postgres")

    @property
    def lakebase_serving_db(self) -> str:
        """Low-latency serving DB (created in wave0, synced from gold)."""
        return self.get("lakebase.database")

    @property
    def lakebase_app_db(self) -> str:
        """App-state DB (sessions, review queue, sign-offs; created in wave0)."""
        return self.get("lakebase.app_state_db")

    @property
    def synced_tables(self) -> list[dict]:
        return self.get("lakebase.synced_tables", [])

    @property
    def lakebase_catalog(self) -> str:
        """UC catalog registered over the Lakebase serving DB."""
        return self.get("lakebase.catalog", "ads_lakebase")

    @property
    def lakebase_storage_catalog(self) -> str:
        """Regular UC catalog for the synced-table DLT pipeline metadata (must NOT
        be the Lakebase catalog). Defaults to the project's data catalog."""
        return self.get("lakebase.storage_catalog", self.catalog)

    # -- Autoscaling resource paths (projects/.../branches/.../...) ----------
    @property
    def lakebase_branch_path(self) -> str:
        return f"projects/{self.lakebase_project}/branches/{self.lakebase_branch}"

    @property
    def lakebase_endpoint_path(self) -> str:
        return f"{self.lakebase_branch_path}/endpoints/{self.lakebase_endpoint_id}"

    def lakebase_database_path(self, db: str | None = None) -> str:
        return f"{self.lakebase_branch_path}/databases/{db or self.lakebase_bound_db}"

    # -- gateway / models ------------------------------------------------
    @property
    def gateway_endpoint(self) -> str:
        return self.get("gateway.endpoint")

    @property
    def inference_table(self) -> str:
        return f"{self.catalog}.{self.get('gateway.inference_table')}"

    @property
    def model_candidates(self) -> list[str]:
        return self.get("models.candidates", [])

    @property
    def default_model(self) -> str:
        return self.get("models.default")

    @property
    def embedding_model(self) -> str:
        return self.get("models.embedding")

    # -- KB / vector search ---------------------------------------------
    @property
    def kb_table(self) -> str:
        return f"{self.catalog}.{self.get('approved_sql_kb.table')}"

    @property
    def kb_index(self) -> str:
        return f"{self.catalog}.{self.get('approved_sql_kb.index')}"

    @property
    def vs_endpoint(self) -> str:
        return self.get("approved_sql_kb.vs_endpoint")

    @property
    def vector_search_enabled(self) -> bool:
        """Optional Vector Search over the KB. False (default) => build_index is a
        no-op and kb_retrieval uses keyword match. Flip ``vector_search.enabled``
        true and create/point ``approved_sql_kb.vs_endpoint`` at an endpoint to
        activate. Honors the ``ADS_VECTOR_SEARCH__ENABLED`` env override (string)."""
        v = self.get("vector_search.enabled", False)
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on")

    # -- misc ------------------------------------------------------------
    @property
    def app_name(self) -> str:
        return self.get("app.name")

    @property
    def poc_studies(self) -> list[dict]:
        return self.get("poc_studies", [])

    @property
    def synth(self) -> dict:
        return self.get("synthetic_rwd", {})

    @property
    def branding(self) -> dict:
        return self.get("branding", {})

    @property
    def gxp(self) -> dict:
        return self.get("gxp", {})

    def all_schema_keys(self) -> list[str]:
        return list(self.get("schemas", {}).keys())


@lru_cache(maxsize=1)
def cfg() -> Config:
    return Config()


if __name__ == "__main__":
    c = cfg()
    print("initiative :", c.initiative)
    print("catalog    :", c.catalog)
    print("compute    :", c.compute, "(enforced)")
    print("schemas    :", {k: c.schema(k) for k in c.all_schema_keys()})
    print("volume     :", c.protocols_volume, "->", c.protocols_volume_path)
    print("kb table   :", c.kb_table)
    print("lakebase   :", c.lakebase_project, "/", c.lakebase_serving_db,
          "endpoint:", c.lakebase_endpoint_path)
    print("gateway    :", c.gateway_endpoint)
