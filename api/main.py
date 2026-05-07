# api/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from api.config import settings
from api.dependencies import init_repo
from api.routes import conformite, qualite, evolution

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Exécuté AU DÉMARRAGE — initialise la connexion DuckDB ou Databricks
    init_repo()
    yield
    # Exécuté À L'ARRÊT — nettoyage si nécessaire

app = FastAPI(
    title="API Qualité de l'Eau",
    description="Expose les vues Gold du pipeline Databricks",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(conformite.router, prefix="/conformite", tags=["Conformité"])
app.include_router(qualite.router,    prefix="/qualite",    tags=["Qualité"])
app.include_router(evolution.router,  prefix="/evolution",  tags=["Évolution"])

@app.get("/health")
def health():
    return {"status": "ok", "source": settings.data_source}
