"""create documents vector table

Revision ID: 0004_documents_vector
Revises: 0003_ensure_scoring_results
Create Date: 2026-04-22
"""

from __future__ import annotations

from alembic import op


revision = "0004_documents_vector"
down_revision = "0003_ensure_scoring_results"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.documents (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            source TEXT,
            embedding vector(1536),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_documents_source ON public.documents (source);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_documents_embedding_cosine ON public.documents USING ivfflat (embedding vector_cosine_ops);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_documents_embedding_cosine;")
    op.execute("DROP INDEX IF EXISTS ix_documents_source;")
    op.execute("DROP TABLE IF EXISTS public.documents;")

