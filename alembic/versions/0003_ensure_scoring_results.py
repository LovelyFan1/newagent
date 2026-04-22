"""ensure scoring_results table exists

Revision ID: 0003_ensure_scoring_results
Revises: 0002_create_scoring_results
Create Date: 2026-04-22

"""

from __future__ import annotations

from alembic import op


revision = "0003_ensure_scoring_results"
down_revision = "0002_create_scoring_results"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'scoring_results'
            ) THEN
                CREATE TABLE public.scoring_results (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(64) NOT NULL,
                    stock_name VARCHAR(320) NOT NULL,
                    year INTEGER NOT NULL,
                    dimension_scores JSONB NOT NULL,
                    total_score DOUBLE PRECISION NOT NULL,
                    rating VARCHAR(8) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE INDEX ix_scoring_results_stock_code
                    ON public.scoring_results (stock_code);

                CREATE UNIQUE INDEX ix_scoring_results_stock_code_year
                    ON public.scoring_results (stock_code, year);
            END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.scoring_results;")

