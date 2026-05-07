# api/dependencies.py
from api.db.base import AbstractRepo
from api.config import settings

_repo: AbstractRepo | None = None

def init_repo() -> None:
    """Appelé dans lifespan au démarrage."""
    global _repo
    if settings.data_source == "duckdb":
        from api.db.duckdb_repo import DuckDBRepo
        _repo = DuckDBRepo()
    else:
        from api.db.databricks_repo import DatabricksRepo
        _repo = DatabricksRepo()

def get_repo() -> AbstractRepo:
    """Injectée dans les routes via Depends(get_repo)."""
    return _repo
