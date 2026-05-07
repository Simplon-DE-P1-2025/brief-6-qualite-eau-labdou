# api/config.py
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Lit depuis les variables d'environnement ou le fichier .env
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    data_source: Literal["duckdb", "databricks"] = "duckdb"

    # Utilisé par DuckDB pour charger les données Hub'Eau
    hubeau_dept: str = "33"
    hubeau_size: int = 5000

    # Utilisé par Databricks
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str = ""

settings = Settings()