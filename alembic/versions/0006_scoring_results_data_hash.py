<<<<<<< HEAD
"""add data_hash to scoring_results

Revision ID: 0006_scoring_results_data_hash
Revises: 0005_documents_hash
Create Date: 2026-04-22

=======
"""placeholder for scoring_results_data_hash

Revision ID: 0006_scoring_results_data_hash
Revises: 0005_documents_hash
Create Date: 2026-04-23
>>>>>>> cdba99b (refactor: 合并Agent架构并添加性能优化索引)
"""

from __future__ import annotations

from alembic import op


revision = "0006_scoring_results_data_hash"
down_revision = "0005_documents_hash"
branch_labels = None
depends_on = None


def upgrade() -> None:
<<<<<<< HEAD
    op.execute("ALTER TABLE public.scoring_results ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_scoring_results_data_hash ON public.scoring_results (data_hash);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_scoring_results_data_hash;")
    op.execute("ALTER TABLE public.scoring_results DROP COLUMN IF EXISTS data_hash;")
=======
    # This revision is present in some environments' alembic_version table.
    # It is intentionally a no-op to restore a consistent migration graph.
    op.execute("SELECT 1;")


def downgrade() -> None:
    op.execute("SELECT 1;")
>>>>>>> cdba99b (refactor: 合并Agent架构并添加性能优化索引)

