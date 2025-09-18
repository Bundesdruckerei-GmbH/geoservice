"""remove adm1_code column from 'vg250_attributes'

Revision ID: 0013
Revises: 0012
Create Date: 2025-02-27 11:09:14.301005

"""
from alembic import op
import sqlalchemy as sa

revision = '0013'
down_revision = '0012'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('vg250_attributes', 'adm1_code')


def downgrade():
    op.add_column('vg250_attributes', sa.Column(
        'adm1_code', sa.Unicode(), nullable=True))
