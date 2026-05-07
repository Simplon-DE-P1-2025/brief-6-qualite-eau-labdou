# api/models/conformite.py
from pydantic import BaseModel

class ConformiteParCommune(BaseModel):
    code_commune: str
    libelle_commune: str | None = None
    code_departement: str
    libelle_departement: str | None = None
    annee_prelevement: int
    nb_prelevements: int
    taux_bact_pct: float | None = None
    taux_pc_pct: float | None = None
    taux_global_pct: float | None = None
