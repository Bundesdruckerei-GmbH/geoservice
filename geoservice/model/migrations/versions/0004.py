"""new table 'consulates'

Revision ID: 0004
Revises: 0003
Create Date: 2024-04-16 08:29:35.507041

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2

revision = '0004'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'consulates',
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('sovereign_code', sa.Unicode(), nullable=False),
        sa.Column('consulate_code', sa.Unicode(), nullable=False),
        sa.Column('name_de', sa.Unicode(), nullable=False),
        sa.Column('name_en', sa.Unicode(), nullable=False),
        sa.Column('url', sa.Unicode(), nullable=False),
        sa.Column('geometry', geoalchemy2.types.Geometry(srid=4326), nullable=True),
        sa.Column('source', sa.Unicode(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('consulates')
