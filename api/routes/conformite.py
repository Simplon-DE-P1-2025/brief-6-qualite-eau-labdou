# api/routes/conformite.py
from fastapi import APIRouter, Depends, Query

from api.db.base import AbstractRepo
from api.dependencies import get_repo
from api.models.conformite import ConformiteParCommune

router = APIRouter()

@router.get("/communes", response_model=list[ConformiteParCommune])
def get_conformite_communes(
    annee: int | None = None,
    code_departement: str | None = Query(default=None, pattern=r"^[0-9A-Za-z]{1,10}$"),
    repo: AbstractRepo = Depends(get_repo),
):
    """
    Taux de conformité bactériologique, physico-chimique et global
    par commune, avec déduplication au niveau prélèvement.
    """
    return repo.get_conformite_communes(annee=annee, code_departement=code_departement)
