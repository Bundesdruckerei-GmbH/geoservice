"""Switch to PostGis

Revision ID: 0003
Revises: 0002
Create Date: 2024-03-22 11:16:07.673803

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


revision = '0003'
down_revision = '0002'
branch_labels = None
depends_on = None


def upgrade():
    for level in ['0', '1']:
        with op.batch_alter_table(f'adm{level}', schema=None) as batch_op:
            batch_op.drop_column('geometry')
            batch_op.add_column(
                sa.Column('geometry',
                    geoalchemy2.types.Geometry(srid=4326, from_text='ST_GeomFromEWKT', name='geometry'),
                    nullable=True)
            )
            #batch_op.create_index(f'idx_adm{level}_geometry', ['geometry'], unique=False, postgresql_using='gist')


def downgrade():
    for level in ['0', '1']:
        with op.batch_alter_table(f'adm{level}', schema=None) as batch_op:
            #batch_op.drop_index(f'idx_adm{level}_geometry')
            batch_op.drop_column('geometry')
            batch_op.add_column(
                sa.Column('geometry', sa.JSON(), nullable=True)
            )
