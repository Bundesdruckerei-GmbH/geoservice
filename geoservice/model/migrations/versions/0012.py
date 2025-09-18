""""populated_places table

Revision ID: 0012
Revises: 0011
Create Date: 2025-02-18 13:45:21.661238

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '0012'
down_revision = '0011'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'populated_places',
        sa.Column('capital_level', sa.Unicode(), nullable=False),
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('nameascii', sa.Unicode(), nullable=False),
        sa.Column('name_de', sa.Unicode(), nullable=False),
        sa.Column('name_en', sa.Unicode(), nullable=False),
        sa.Column('name_fr', sa.Unicode(), nullable=False),
        sa.Column('population', sa.Integer(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=True),
        sa.Column('geometry', geoalchemy2.types.Geometry(srid=4326), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('populated_places')
