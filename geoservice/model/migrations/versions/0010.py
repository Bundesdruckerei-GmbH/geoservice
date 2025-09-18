"""adds relation 'metadata'
Revision ID: 0010
Revises: 0009
Create Date: 2025-01-09 11:15:04.714434

"""
from alembic import op
import sqlalchemy as sa


revision = '0010'
down_revision = '0009'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'metadata',
        sa.Column('title', sa.Unicode(), nullable=False),
        sa.Column('abstract', sa.Unicode(), nullable=False),
        sa.Column('lineage', sa.Unicode(), nullable=False),
        sa.Column('responsibleParty', sa.Unicode(), nullable=False),
        sa.Column('crs', sa.Unicode(), nullable=True),
        sa.Column('format', sa.Unicode(), nullable=False),
        sa.Column('geoBox', sa.ARRAY(sa.Numeric()), nullable=True),
        sa.Column('datatype', sa.Unicode(), nullable=False),
        sa.Column('adaptionDate', sa.DateTime(), nullable=True),
        sa.Column('source', sa.Unicode(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'metadatakeywords',
        sa.Column('keywords', sa.Unicode(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'metadataorigin',
        sa.Column('originName', sa.Unicode(), nullable=False),
        sa.Column('originSource', sa.Unicode(), nullable=False),
        sa.Column('originAttribution', sa.Unicode(), nullable=False),
        sa.Column('originLicence', sa.Unicode(), nullable=False),
        sa.Column('originLicenceSource', sa.Unicode(), nullable=False),
        sa.Column('originVersion', sa.Unicode(), nullable=False),
        sa.Column('source', sa.Unicode(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('metadata')
    op.drop_table('metadatakeywords')
    op.drop_table('metadataorigin')
