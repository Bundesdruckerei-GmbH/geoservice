"""initial

Revision ID: 0001
Revises: 
Create Date: 2024-02-01 16:49:59.404080

"""
from alembic import op
import sqlalchemy as sa


revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('adm0',
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('name', sa.Unicode(), nullable=False),
        sa.Column('geometry_level', sa.Integer(), nullable=False),
        sa.Column('geometry', sa.JSON(), nullable=True),
        sa.Column('rasterdata', sa.LargeBinary(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table('adm1',
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('adm1_code', sa.Unicode(), nullable=False),
        sa.Column('name', sa.Unicode(), nullable=False),
        sa.Column('geometry_level', sa.Integer(), nullable=False),
        sa.Column('geometry', sa.JSON(), nullable=True),
        sa.Column('rasterdata', sa.LargeBinary(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('adm1')
    op.drop_table('adm0')
