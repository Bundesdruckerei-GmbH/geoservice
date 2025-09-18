"""empty message

Revision ID: 0011
Revises: 0010
Create Date: 2025-01-22 16:36:53.442110

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0011'
down_revision = '0010'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('link_table',
        sa.Column('iso_name', sa.Unicode(), nullable=False),
        sa.Column('iso_3166_1_a2', sa.Unicode(), nullable=False),
        sa.Column('iso_3166_1_a3', sa.Unicode(), nullable=False),
        sa.Column('iso_3166_1_n3', sa.Integer(), nullable=False),
        sa.Column('independent', sa.Unicode(), nullable=False),
        sa.Column('iso_3166_2', sa.Unicode(), nullable=False),
        sa.Column('link_to_aerial_level', sa.Unicode(), nullable=False),
        sa.Column('link_to_source', sa.Unicode(), nullable=False),
        sa.Column('link_to_code', sa.Unicode(), nullable=False),
        sa.Column('link_to_name', sa.Unicode(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )



def downgrade():
    op.drop_table('link_table')
