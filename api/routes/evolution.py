# api/routes/evolution.py
from typing import Literal

from fastapi import APIRouter, Depends

from api.db.base import AbstractRepo
from api.dependencies import get_repo
from api.models.evolution import EvolutionTemporelleParametres

router = APIRouter()

@router.get("/parametres", response_model=list[EvolutionTemporelleParametres])
def get_evolution_parametres(
    annee: int | None = None,
    categorie: Literal["microbiologie", "radioactivite", "organoleptique", "chimie"] | None = None,
    repo: AbstractRepo = Depends(get_repo),
):
    return repo.get_evolution_parametres(annee=annee, categorie=categorie)
