"""optimize retrieval indexes (hnsw + trgm + fact composite)

Revision ID: 0007_optimize_retrieval_indexes
Revises: 0006_scoring_results_data_hash
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op


revision = "0007_optimize_retrieval_indexes"
down_revision = "0006_scoring_results_data_hash"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Extensions are required for the indexes below.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # documents: vector ANN index (HNSW) + trigram index for fallback similarity(content, q)
    op.execute(
        """
        DO $$
        BEGIN
          IF to_regclass('public.documents') IS NOT NULL THEN
            CREATE INDEX IF NOT EXISTS idx_documents_embedding_hnsw
              ON public.documents
              USING hnsw (embedding vector_cosine_ops)
              WITH (m = 16, ef_construction = 64);

            CREATE INDEX IF NOT EXISTS idx_documents_content_trgm
              ON public.documents
              USING gin (content gin_trgm_ops);
          END IF;
        END $$;
        """
    )

    # fact_* tables: composite index (enterprise_id, year) if table exists
    op.execute(
        """
        DO $$
        BEGIN
          IF to_regclass('public.fact_financials') IS NOT NULL THEN
            CREATE INDEX IF NOT EXISTS idx_fact_financials_enterprise_year
              ON public.fact_financials (enterprise_id, year);
          END IF;

          IF to_regclass('public.fact_sales') IS NOT NULL THEN
            CREATE INDEX IF NOT EXISTS idx_fact_sales_enterprise_year
              ON public.fact_sales (enterprise_id, year);
          END IF;

          IF to_regclass('public.fact_legal') IS NOT NULL THEN
            CREATE INDEX IF NOT EXISTS idx_fact_legal_enterprise_year
              ON public.fact_legal (enterprise_id, year);
          END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Drop indexes if they exist (guarded for safety)
    op.execute("DROP INDEX IF EXISTS public.idx_documents_embedding_hnsw;")
    op.execute("DROP INDEX IF EXISTS public.idx_documents_content_trgm;")
    op.execute("DROP INDEX IF EXISTS public.idx_fact_financials_enterprise_year;")
    op.execute("DROP INDEX IF EXISTS public.idx_fact_sales_enterprise_year;")
    op.execute("DROP INDEX IF EXISTS public.idx_fact_legal_enterprise_year;")

