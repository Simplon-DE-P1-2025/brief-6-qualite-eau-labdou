# api/db/base.py
from abc import ABC, abstractmethod


class AbstractRepo(ABC):
    """Contrat commun à DuckDB et Databricks."""

    @abstractmethod
    def get_conformite_communes(
        self,
        annee: int | None = None,
        code_departement: str | None = None,
    ) -> list[dict]: ...

    @abstractmethod
    def get_qualite_departements(
        self,
        annee: int | None = None,
        code_region: str | None = None,
    ) -> list[dict]: ...

    @abstractmethod
    def get_evolution_parametres(
        self,
        annee: int | None = None,
        categorie: str | None = None,
    ) -> list[dict]: ...
