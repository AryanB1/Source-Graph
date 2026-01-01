"""Initial migration: create all tables.

Revision ID: 001
Revises: 
Create Date: 2025-12-29

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create runs table
    op.create_table(
        'runs',
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text('gen_random_uuid()')),
        sa.Column('mode', sa.Text(), nullable=False),
        sa.Column('query', sa.Text(), nullable=True),
        sa.Column('seed_uri', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.Column('params_json', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint('run_id')
    )
    
    # Create posts table
    op.create_table(
        'posts',
        sa.Column('uri', sa.Text(), nullable=False),
        sa.Column('cid', sa.Text(), nullable=True),
        sa.Column('author_did', sa.Text(), nullable=False),
        sa.Column('author_handle', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('text', sa.Text(), nullable=False, server_default=''),
        sa.Column('like_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('repost_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('reply_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('quote_count', sa.Integer(), nullable=False, server_default='0'),
        sa.PrimaryKeyConstraint('uri')
    )
    op.create_index('ix_posts_created_at', 'posts', ['created_at'])
    
    # Create edges table
    op.create_table(
        'edges',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('src_uri', sa.Text(), nullable=False),
        sa.Column('dst_uri', sa.Text(), nullable=False),
        sa.Column('edge_type', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('src_uri', 'dst_uri', 'edge_type', name='uq_edges_src_dst_type')
    )
    op.create_index('ix_edges_src_uri', 'edges', ['src_uri'])
    op.create_index('ix_edges_dst_uri', 'edges', ['dst_uri'])
    
    # Create run_posts link table
    op.create_table(
        'run_posts',
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('uri', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('run_id', 'uri')
    )
    op.create_index('ix_run_posts_run_id', 'run_posts', ['run_id'])
    
    # Create run_edges link table
    op.create_table(
        'run_edges',
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('src_uri', sa.Text(), nullable=False),
        sa.Column('dst_uri', sa.Text(), nullable=False),
        sa.Column('edge_type', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('run_id', 'src_uri', 'dst_uri', 'edge_type')
    )
    op.create_index('ix_run_edges_run_id', 'run_edges', ['run_id'])


def downgrade() -> None:
    op.drop_table('run_edges')
    op.drop_table('run_posts')
    op.drop_table('edges')
    op.drop_table('posts')
    op.drop_table('runs')
