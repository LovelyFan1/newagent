from __future__ import annotations

import logging
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.api import api_router
from app.api.exceptions import install_exception_handlers
from app.core.config import get_settings
from app.core.db import ensure_vector_extension
from app.db.session import get_engine

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)
    install_exception_handlers(app)
    app.include_router(api_router)

    # static web (single-port demo): serve /web/*, and redirect "/" to login page.
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

    @app.get("/", include_in_schema=False)
    async def _root():
        return RedirectResponse(url="/web/login.html")

    @app.on_event("startup")
    async def _ensure_scoring_results_table() -> None:
        """
        The project may be used with CSV-imported tables; in some environments
        the scoring cache table can be missing even if services are running.
        Ensure it exists to prevent /api/v1/scoring/* returning 500.
        """
        engine = get_engine()
        is_pg = engine.url.drivername == "postgresql+asyncpg"

        if is_pg:
            try:
                await ensure_vector_extension(engine)
            except Exception:
                # Silent fallback to avoid startup crash in constrained envs.
                logger.exception("startup: ensure_vector_extension failed")

        async with engine.begin() as conn:
            if is_pg:
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
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_users_email ON public.users (email);")
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
                        data_hash VARCHAR(32) NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_scoring_results_stock_code ON public.scoring_results (stock_code);")
                await conn.exec_driver_sql(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_scoring_results_stock_code_year ON public.scoring_results (stock_code, year);"
                )
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_scoring_results_data_hash ON public.scoring_results (data_hash);")
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS public.core_metrics_summary (
                        stock_code VARCHAR(64) NOT NULL,
                        enterprise_name VARCHAR(320) NOT NULL,
                        year INTEGER NOT NULL,
                        revenue DOUBLE PRECISION NULL,
                        net_profit DOUBLE PRECISION NULL,
                        total_assets DOUBLE PRECISION NULL,
                        sales_volume DOUBLE PRECISION NULL,
                        roe DOUBLE PRECISION NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (stock_code, year)
                    );
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_core_metrics_summary_enterprise_year ON public.core_metrics_summary (enterprise_name, year);"
                )
                # Pre-aggregate yearly core metrics for fast metric queries.
                await conn.exec_driver_sql(
                    """
                    INSERT INTO public.core_metrics_summary (
                        stock_code, enterprise_name, year, revenue, net_profit, total_assets, sales_volume, roe, updated_at
                    )
                    SELECT
                        COALESCE(NULLIF(de.stock_code, ''), de.stock_name) AS stock_code,
                        de.stock_name AS enterprise_name,
                        ff.year::int AS year,
                        MAX(ff.revenue::double precision) AS revenue,
                        MAX(ff.net_profit::double precision) AS net_profit,
                        MAX(ff.total_assets::double precision) AS total_assets,
                        MAX(fs.total_sales_volume::double precision) AS sales_volume,
                        MAX(ff.roe::double precision) AS roe,
                        now() AS updated_at
                    FROM dim_enterprise de
                    JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
                    LEFT JOIN fact_sales fs
                      ON fs.enterprise_id = de.enterprise_id
                     AND fs.year::int = ff.year::int
                    GROUP BY COALESCE(NULLIF(de.stock_code, ''), de.stock_name), de.stock_name, ff.year::int
                    ON CONFLICT (stock_code, year) DO UPDATE
                    SET
                        enterprise_name = EXCLUDED.enterprise_name,
                        revenue = EXCLUDED.revenue,
                        net_profit = EXCLUDED.net_profit,
                        total_assets = EXCLUDED.total_assets,
                        sales_volume = EXCLUDED.sales_volume,
                        roe = EXCLUDED.roe,
                        updated_at = now();
                    """
                )
            else:
                # SQLite-compatible bootstrap for local acceptance tests.
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id TEXT PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE,
                        hashed_password TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_users_email ON users (email);")
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS scoring_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT NOT NULL,
                        year INTEGER NOT NULL,
                        dimension_scores TEXT NOT NULL,
                        total_score REAL NOT NULL,
                        rating TEXT NOT NULL,
                        data_hash TEXT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_scoring_results_stock_code ON scoring_results (stock_code);")
                await conn.exec_driver_sql(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_scoring_results_stock_code_year ON scoring_results (stock_code, year);"
                )
                await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_scoring_results_data_hash ON scoring_results (data_hash);")
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS core_metrics_summary (
                        stock_code TEXT NOT NULL,
                        enterprise_name TEXT NOT NULL,
                        year INTEGER NOT NULL,
                        revenue REAL NULL,
                        net_profit REAL NULL,
                        total_assets REAL NULL,
                        sales_volume REAL NULL,
                        roe REAL NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (stock_code, year)
                    );
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_core_metrics_summary_enterprise_year ON core_metrics_summary (enterprise_name, year);"
                )
                await conn.exec_driver_sql(
                    """
                    INSERT INTO core_metrics_summary (
                        stock_code, enterprise_name, year, revenue, net_profit, total_assets, sales_volume, roe, updated_at
                    )
                    SELECT
                        COALESCE(NULLIF(de.stock_code, ''), de.stock_name) AS stock_code,
                        de.stock_name AS enterprise_name,
                        CAST(ff.year AS INTEGER) AS year,
                        MAX(CAST(ff.revenue AS REAL)) AS revenue,
                        MAX(CAST(ff.net_profit AS REAL)) AS net_profit,
                        MAX(CAST(ff.total_assets AS REAL)) AS total_assets,
                        MAX(CAST(fs.total_sales_volume AS REAL)) AS sales_volume,
                        MAX(CAST(ff.roe AS REAL)) AS roe,
                        CURRENT_TIMESTAMP AS updated_at
                    FROM dim_enterprise de
                    JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
                    LEFT JOIN fact_sales fs
                      ON fs.enterprise_id = de.enterprise_id
                     AND CAST(fs.year AS INTEGER) = CAST(ff.year AS INTEGER)
                    GROUP BY COALESCE(NULLIF(de.stock_code, ''), de.stock_name), de.stock_name, CAST(ff.year AS INTEGER)
                    ON CONFLICT(stock_code, year) DO UPDATE SET
                        enterprise_name = excluded.enterprise_name,
                        revenue = excluded.revenue,
                        net_profit = excluded.net_profit,
                        total_assets = excluded.total_assets,
                        sales_volume = excluded.sales_volume,
                        roe = excluded.roe,
                        updated_at = CURRENT_TIMESTAMP;
                    """
                )
    return app


app = create_app()

