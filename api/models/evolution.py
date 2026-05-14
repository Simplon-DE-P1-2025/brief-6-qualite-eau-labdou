# api/models/evolution.py
from pydantic import BaseModel


class EvolutionTemporelleParametres(BaseModel):
    annee_prelevement: int
    mois_prelevement: int
    libelle_parametre: str
    categorie_parametre: str
    nb_prelevements: int
    moyenne: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    unite_mesure: str | None = None
    taux_depassement_limite_pct: float | None = None
