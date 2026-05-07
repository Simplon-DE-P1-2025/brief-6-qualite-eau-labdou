# api/models/qualite.py
from pydantic import BaseModel

class QualiteParDepartement(BaseModel):
    code_departement: str
    libelle_departement: str | None = None
    code_region: str | None = None
    libelle_region: str | None = None
    annee_prelevement: int
    nb_prelevements: int
    nb_conformes: int
    taux_conformite_pct: float | None = None
