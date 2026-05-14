# api/routes/qualite.py
from fastapi import APIRouter, Depends, Query

from api.db.base import AbstractRepo
from api.dependencies import get_repo
from api.models.qualite import QualiteParDepartement

router = APIRouter()

@router.get("/departements", response_model=list[QualiteParDepartement])
def get_qualite_departements(
    annee: int | None = None,
    code_region: str | None = Query(default=None, pattern=r"^[0-9A-Za-z]{1,10}$"),
    repo: AbstractRepo = Depends(get_repo),
):
    return repo.get_qualite_departements(annee=annee, code_region=code_region)
