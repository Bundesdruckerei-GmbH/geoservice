"""adds column 'sources'
Revision ID: 0002
Revises: 0001
Create Date: 2024-03-15 11:15:04.714434

"""
from alembic import op
import sqlalchemy as sa


revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('adm0', sa.Column('source', sa.Unicode(), nullable=True))
    op.add_column('adm1', sa.Column('source', sa.Unicode(), nullable=True))


def downgrade():
    op.drop_column('adm0', 'source')
    op.drop_column('adm1', 'source')
