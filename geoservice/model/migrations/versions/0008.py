"""new table 'vg250'

Revision ID: 0008
Revises: 0007
Create Date: 2024-08-12 11:09:14.301005

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2

revision = '0008'
down_revision = '0007'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'vg250',
        sa.Column('code', sa.Unicode(), nullable=False),
        sa.Column('name', sa.Unicode(), nullable=False),
        sa.Column('geometry_level', sa.Integer(), nullable=False),
        sa.Column('agg_level', sa.Unicode(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=False),
        sa.Column('geometry', geoalchemy2.types.Geometry(srid=4326), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'vg250_attributes',
        sa.Column('arsg', sa.Unicode(), nullable=False),
        sa.Column('geng', sa.Unicode(), nullable=False),
        sa.Column('arsv', sa.Unicode(), nullable=False),
        sa.Column('genv', sa.Unicode(), nullable=False),
        sa.Column('arsk', sa.Unicode(), nullable=False),
        sa.Column('genk', sa.Unicode(), nullable=False),
        sa.Column('arsr', sa.Unicode(), nullable=False),
        sa.Column('genr', sa.Unicode(), nullable=False),
        sa.Column('arsl', sa.Unicode(), nullable=False),
        sa.Column('genl', sa.Unicode(), nullable=False),
        sa.Column('nuts1code', sa.Unicode(), nullable=False),
        sa.Column('nuts1name', sa.Unicode(), nullable=False),
        sa.Column('nuts2code', sa.Unicode(), nullable=False),
        sa.Column('nuts2name', sa.Unicode(), nullable=False),
        sa.Column('nuts3code', sa.Unicode(), nullable=False),
        sa.Column('nuts3name', sa.Unicode(), nullable=False),      
        sa.Column('ewz', sa.Integer(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=False),
        sa.Column('adm1_code', sa.Unicode(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),  
        sa.PrimaryKeyConstraint('id')
    )

def downgrade():
    op.drop_table('vg250')
    op.drop_table('vg250_attributes')
