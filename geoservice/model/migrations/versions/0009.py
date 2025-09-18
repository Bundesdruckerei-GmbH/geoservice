"""adds column 'sources'
Revision ID: 0009
Revises: 0008
Create Date: 2024-08-21 11:15:04.714434

"""
from alembic import op
import sqlalchemy as sa


revision = '0009'
down_revision = '0008'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('adm1', sa.Column('adm0_name', sa.Unicode(), nullable=True))
    op.drop_column('adm0', 'rasterdata')
    op.drop_column('adm1', 'rasterdata')


def downgrade():
    op.drop_column('adm1', 'adm0_name')
    op.add_column('adm0', sa.Column('rasterdata', sa.LargeBinary(), nullable=True))
    op.add_column('adm1', sa.Column('rasterdata', sa.LargeBinary(), nullable=True))
