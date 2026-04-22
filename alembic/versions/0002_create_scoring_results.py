"""create scoring results table

Revision ID: 0002_create_scoring_results
Revises: 0001_create_users_and_metrics
Create Date: 2026-04-22

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "0002_create_scoring_results"
down_revision = "0001_create_users_and_metrics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scoring_results",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("stock_code", sa.String(length=64), nullable=False),
        sa.Column("stock_name", sa.String(length=320), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("dimension_scores", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("total_score", sa.Float(), nullable=False),
        sa.Column("rating", sa.String(length=8), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_scoring_results_stock_code", "scoring_results", ["stock_code"])
    op.create_index("ix_scoring_results_stock_code_year", "scoring_results", ["stock_code", "year"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_scoring_results_stock_code_year", table_name="scoring_results")
    op.drop_index("ix_scoring_results_stock_code", table_name="scoring_results")
    op.drop_table("scoring_results")

