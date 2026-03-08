from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.activities import router as activities_router
from src.core.config import settings

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api_allowed_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(activities_router, prefix="/api")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
