"""new table 'settlingadm0'

Revision ID: 0007
Revises: 0006
Create Date: 2024-07-04 11:09:14.301005

"""
from alembic import op
import sqlalchemy as sa

revision = '0007'
down_revision = '0006'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'settlingadm0',
        sa.Column('adm0_code', sa.Unicode(), nullable=False),
        sa.Column('swx', sa.Integer(), nullable=False),
        sa.Column('swy', sa.Integer(), nullable=False),
        sa.Column('nex', sa.Integer(), nullable=False),
        sa.Column('ney', sa.Integer(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('wsf_pop_factor', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

def downgrade():
    op.drop_table('settlingadm0')
