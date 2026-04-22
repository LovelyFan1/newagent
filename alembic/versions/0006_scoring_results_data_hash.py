"""add data_hash to scoring_results

Revision ID: 0006_scoring_results_data_hash
Revises: 0005_documents_hash
Create Date: 2026-04-22

"""

from __future__ import annotations

from alembic import op


revision = "0006_scoring_results_data_hash"
down_revision = "0005_documents_hash"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.scoring_results ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_scoring_results_data_hash ON public.scoring_results (data_hash);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_scoring_results_data_hash;")
    op.execute("ALTER TABLE public.scoring_results DROP COLUMN IF EXISTS data_hash;")

