# api/db/databricks_repo.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from api.db.base import AbstractRepo
from api.config import settings

class DatabricksRepo(AbstractRepo):
    def __init__(self):
        self._client = WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )
        self._warehouse_id = settings.databricks_warehouse_id

    def _execute(self, sql: str) -> list[dict]:
        response = self._client.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL failed: {response.status.error}")

        columns = [col.name for col in response.manifest.schema.columns]
        rows = response.result.data_array or []
        return [dict(zip(columns, row)) for row in rows]

    def get_conformite_communes(self, annee=None, code_departement=None):
        filters = self._where(annee=annee, code_departement=code_departement)
        return self._execute(f"SELECT * FROM gold.conformite_par_commune {filters}")

    def get_qualite_departements(self, annee=None, code_region=None):
        filters = self._where(annee=annee, code_region=code_region)
        return self._execute(f"SELECT * FROM gold.qualite_par_departement {filters}")

    def get_evolution_parametres(self, annee=None, categorie=None):
        filters = self._where(annee=annee, categorie_parametre=categorie)
        return self._execute(f"SELECT * FROM gold.evolution_temporelle_parametres {filters}")

    @staticmethod
    def _where(**kwargs) -> str:
        conds = [
            f"{k} = {v}" if isinstance(v, int) else f"{k} = '{v}'"
            for k, v in kwargs.items()
            if v is not None
        ]
        return ("WHERE " + " AND ".join(conds)) if conds else ""
