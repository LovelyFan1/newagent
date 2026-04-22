from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.response import ok
from app.db.session import get_db
from app.models.scoring import ScoringResult
from app.services.scoring_service import scoring_service


router = APIRouter()


@router.get("/{stock_code}")
async def get_scoring(
    stock_code: str,
    year: int = Query(..., ge=1900, le=2100),
    db: AsyncSession = Depends(get_db),
):
    existing = (
        await db.execute(select(ScoringResult).where(ScoringResult.stock_code == stock_code, ScoringResult.year == year))
    ).scalar_one_or_none()

    if existing is not None:
        return ok(
            {
                "stock_code": existing.stock_code,
                "stock_name": existing.stock_name,
                "year": existing.year,
                "total_score": existing.total_score,
                "rating": existing.rating,
                "dimension_scores": existing.dimension_scores,
            }
        )

    result = await scoring_service.calculate_score(stock_code, year)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")

    record = ScoringResult(
        stock_code=result["stock_code"],
        stock_name=result["stock_name"],
        year=year,
        dimension_scores=result["dimension_scores"],
        total_score=float(result["total_score"]),
        rating=result["rating"],
    )
    db.add(record)
    try:
        await db.commit()
    except IntegrityError:
        # concurrent requests may insert same (stock_code, year)
        await db.rollback()
        existing = (
            await db.execute(
                select(ScoringResult).where(ScoringResult.stock_code == result["stock_code"], ScoringResult.year == year)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return ok(
                {
                    "stock_code": existing.stock_code,
                    "stock_name": existing.stock_name,
                    "year": existing.year,
                    "total_score": existing.total_score,
                    "rating": existing.rating,
                    "dimension_scores": existing.dimension_scores,
                }
            )
        raise

    return ok(
        {
            "stock_code": record.stock_code,
            "stock_name": record.stock_name,
            "year": record.year,
            "total_score": record.total_score,
            "rating": record.rating,
            "dimension_scores": record.dimension_scores,
        }
    )

