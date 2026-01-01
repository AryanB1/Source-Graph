"""Tests for repository layer."""
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from app.repositories import runs_repository as repo
from bsky.models import Edge, Post, PostMetrics


def test_upsert_posts_idempotency(session: Session):
    """Test that upserting the same posts twice is idempotent."""
    # Create test posts
    posts = [
        Post(
            uri="at://did:plc:123/app.bsky.feed.post/abc",
            cid="cid123",
            author_did="did:plc:123",
            author_handle="user1.bsky.social",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="First post",
            metrics=PostMetrics(like_count=10, repost_count=5, reply_count=2, quote_count=1),
        ),
        Post(
            uri="at://did:plc:456/app.bsky.feed.post/def",
            cid="cid456",
            author_did="did:plc:456",
            author_handle="user2.bsky.social",
            created_at=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            text="Second post",
            metrics=PostMetrics(like_count=20, repost_count=10, reply_count=4, quote_count=2),
        ),
    ]
    
    # First upsert
    count1 = repo.upsert_posts(session, posts)
    session.commit()
    assert count1 == 2
    
    # Verify posts are in database
    db_posts = repo.get_run_posts(session, uuid.uuid4())  # No run, but posts exist
    # Note: Without a run link, we can't fetch via get_run_posts
    # Let's verify by checking the table directly
    from app.db.models import Post as DBPost
    from sqlalchemy import select
    
    stmt = select(DBPost)
    result = session.execute(stmt)
    all_posts = list(result.scalars().all())
    assert len(all_posts) == 2
    
    # Update one post
    updated_posts = [
        Post(
            uri="at://did:plc:123/app.bsky.feed.post/abc",
            cid="cid123_new",
            author_did="did:plc:123",
            author_handle="user1.bsky.social",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="First post updated",
            metrics=PostMetrics(like_count=15, repost_count=6, reply_count=3, quote_count=1),
        ),
    ]
    
    # Second upsert with updated data
    count2 = repo.upsert_posts(session, updated_posts)
    session.commit()
    assert count2 == 1
    
    # Verify still only 2 posts total
    stmt = select(DBPost)
    result = session.execute(stmt)
    all_posts = list(result.scalars().all())
    assert len(all_posts) == 2
    
    # Verify the post was updated
    stmt = select(DBPost).where(DBPost.uri == "at://did:plc:123/app.bsky.feed.post/abc")
    result = session.execute(stmt)
    updated_post = result.scalar_one()
    assert updated_post.text == "First post updated"
    assert updated_post.like_count == 15
    assert updated_post.cid == "cid123_new"


def test_run_linking(session: Session):
    """Test that the same post can be linked to multiple runs."""
    # Create a post
    posts = [
        Post(
            uri="at://did:plc:123/app.bsky.feed.post/shared",
            cid="cid_shared",
            author_did="did:plc:123",
            author_handle="user.bsky.social",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="Shared post",
            metrics=PostMetrics(like_count=10),
        ),
    ]
    
    # Upsert post
    repo.upsert_posts(session, posts)
    
    # Create two runs
    run_id_1 = repo.create_run(
        session,
        mode="query",
        query="test query 1",
        seed_uri=None,
        params_json={},
    )
    run_id_2 = repo.create_run(
        session,
        mode="query",
        query="test query 2",
        seed_uri=None,
        params_json={},
    )
    
    # Link the same post to both runs
    uris = ["at://did:plc:123/app.bsky.feed.post/shared"]
    repo.link_run_posts(session, run_id_1, uris)
    repo.link_run_posts(session, run_id_2, uris)
    session.commit()
    
    # Verify both runs have the post
    posts_run_1 = repo.get_run_posts(session, run_id_1)
    posts_run_2 = repo.get_run_posts(session, run_id_2)
    
    assert len(posts_run_1) == 1
    assert len(posts_run_2) == 1
    assert posts_run_1[0].uri == uris[0]
    assert posts_run_2[0].uri == uris[0]
    
    # Verify run_posts table has correct counts
    from app.db.models import RunPost
    from sqlalchemy import select, func
    
    stmt = select(func.count()).select_from(RunPost)
    result = session.execute(stmt)
    total_links = result.scalar()
    assert total_links == 2


def test_edge_deduplication(session: Session):
    """Test that edges are deduplicated correctly."""
    # Create edges
    edges = [
        Edge(
            src_uri="at://did:plc:123/app.bsky.feed.post/src",
            dst_uri="at://did:plc:456/app.bsky.feed.post/dst",
            edge_type="QUOTE",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),
        Edge(
            src_uri="at://did:plc:123/app.bsky.feed.post/src",
            dst_uri="at://did:plc:456/app.bsky.feed.post/dst",
            edge_type="QUOTE",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),  # Duplicate
        Edge(
            src_uri="at://did:plc:123/app.bsky.feed.post/src",
            dst_uri="at://did:plc:456/app.bsky.feed.post/dst",
            edge_type="REPLY",  # Different type
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),
    ]
    
    # Upsert edges
    count = repo.upsert_edges(session, edges)
    session.commit()
    assert count == 3
    
    # Verify only 2 unique edges in database (duplicate ignored)
    from app.db.models import Edge as DBEdge
    from sqlalchemy import select, func
    
    stmt = select(func.count()).select_from(DBEdge)
    result = session.execute(stmt)
    total_edges = result.scalar()
    assert total_edges == 2
