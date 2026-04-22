from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.response import ok
from app.db.session import get_db
from app.services.scoring_service import scoring_service


router = APIRouter()


@router.get("/{stock_code}")
async def get_scoring(
    stock_code: str,
    year: int = Query(..., ge=1900, le=2100),
    force: bool = Query(False, description="强制重算，忽略缓存"),
    db: AsyncSession = Depends(get_db),
):
    result = await scoring_service.calculate(db, stock_code, year, force=force)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return ok(result)

