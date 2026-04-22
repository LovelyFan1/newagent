from __future__ import annotations

from fastapi import FastAPI

from app.api import api_router
from app.api.exceptions import install_exception_handlers
from app.core.config import get_settings
from app.core.db import ensure_vector_extension
from app.db.session import get_engine


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)
    install_exception_handlers(app)
    app.include_router(api_router)

    @app.on_event("startup")
    async def _ensure_scoring_results_table() -> None:
        """
        The project may be used with CSV-imported tables; in some environments
        the scoring cache table can be missing even if services are running.
        Ensure it exists to prevent /api/v1/scoring/* returning 500.
        """
        engine = get_engine()
        await ensure_vector_extension(engine)
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                """
                CREATE TABLE IF NOT EXISTS public.users (
                    id UUID PRIMARY KEY,
                    email VARCHAR(320) NOT NULL UNIQUE,
                    hashed_password VARCHAR(255) NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_users_email ON public.users (email);"
            )
            await conn.exec_driver_sql(
                """
                CREATE TABLE IF NOT EXISTS public.scoring_results (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(64) NOT NULL,
                    stock_name VARCHAR(320) NOT NULL,
                    year INTEGER NOT NULL,
                    dimension_scores JSONB NOT NULL,
                    total_score DOUBLE PRECISION NOT NULL,
                    rating VARCHAR(8) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_scoring_results_stock_code ON public.scoring_results (stock_code);"
            )
            await conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_scoring_results_stock_code_year ON public.scoring_results (stock_code, year);"
            )
    return app


app = create_app()

