"""add content_hash for documents dedupe

Revision ID: 0005_documents_hash
Revises: 0004_documents_vector
Create Date: 2026-04-22
"""

from __future__ import annotations

from alembic import op


revision = "0005_documents_hash"
down_revision = "0004_documents_vector"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS content_hash TEXT;")
    op.execute("CREATE INDEX IF NOT EXISTS ix_documents_content_hash ON public.documents (content_hash);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_documents_content_hash;")
    op.execute("ALTER TABLE public.documents DROP COLUMN IF EXISTS content_hash;")

