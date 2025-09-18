"""new table 'population'

Revision ID: 0006
Revises: 0005
Create Date: 2024-05-14 11:09:14.301005

"""
from alembic import op
import sqlalchemy as sa

revision = '0006'
down_revision = '0005'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'population',
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('value', sa.Integer(), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('population')
