from fastapi import APIRouter

from app.api.v1.routes.auth import router as auth_router
from app.api.v1.routes.agent import router as agent_router
from app.api.v1.routes.scoring import router as scoring_router

router = APIRouter()
router.include_router(auth_router, prefix="/auth", tags=["auth"])
router.include_router(scoring_router, prefix="/scoring", tags=["scoring"])
router.include_router(agent_router, prefix="/agent", tags=["agent"])

