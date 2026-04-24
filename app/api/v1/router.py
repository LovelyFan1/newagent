from fastapi import APIRouter

from app.api.v1.routes.auth import router as auth_router
from app.api.v1.routes.agent import router as agent_router
from app.api.v1.routes.files import router as files_router
from app.api.v1.routes.reports import router as reports_router
from app.api.v1.routes.scoring import router as scoring_router

router = APIRouter()
router.include_router(auth_router, prefix="/auth", tags=["auth"])
router.include_router(scoring_router, prefix="/scoring", tags=["scoring"])
router.include_router(agent_router, prefix="/agent", tags=["agent"])
router.include_router(files_router, prefix="/files", tags=["files"])
router.include_router(reports_router, prefix="/report", tags=["report"])

