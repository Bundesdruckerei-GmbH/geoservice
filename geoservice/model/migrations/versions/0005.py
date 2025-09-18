"""new table 'wahlkreise'

Revision ID: 0005
Revises: 0004
Create Date: 2024-05-14 11:09:14.301005

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2

revision = '0005'
down_revision = '0004'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'wahlkreise',
        sa.Column('adm1_code', sa.Unicode(), nullable=False),
        sa.Column('wkr_name', sa.Unicode(), nullable=False),
        sa.Column('wkr_nr', sa.Integer(), nullable=False),
        sa.Column('land_name', sa.Unicode(), nullable=False),
        sa.Column('land_nr', sa.Integer(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=True),
        sa.Column('geometry', geoalchemy2.types.Geometry(srid=4326), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('wahlkreise')

