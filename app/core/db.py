from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from app.db.session import get_engine


def get_core_engine() -> AsyncEngine:
    return get_engine()


async def ensure_vector_extension(engine: AsyncEngine | None = None) -> None:
    eng = engine or get_engine()
    async with eng.begin() as conn:
        await conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS vector;")

