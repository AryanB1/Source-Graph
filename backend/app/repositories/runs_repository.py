"""Repository layer for database operations."""
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from bsky.models import Edge as BskyEdge
from bsky.models import Post as BskyPost

from ..db.models import Edge, Post, Run, RunEdge, RunPost


def create_run(
    session: Session,
    mode: str,
    query: Optional[str],
    seed_uri: Optional[str],
    params_json: dict,
) -> uuid.UUID:
    """
    Create a new run record.
    
    Args:
        session: Database session
        mode: "query" or "seed"
        query: Search query (for query mode)
        seed_uri: Seed post URI (for seed mode)
        params_json: Ingestion parameters as JSON
        
    Returns:
        UUID of created run
    """
    run_id = uuid.uuid4()
    stmt = insert(Run).values(
        run_id=run_id,
        mode=mode,
        query=query,
        seed_uri=seed_uri,
        params_json=params_json,
    )
    session.execute(stmt)
    return run_id


def upsert_posts(session: Session, posts: list[BskyPost]) -> int:
    """
    Upsert posts into the database.
    Updates all fields on conflict.
    
    Args:
        session: Database session
        posts: List of normalized post objects from ingestion
        
    Returns:
        Number of posts processed
    """
    if not posts:
        return 0
    
    # Convert Pydantic models to dicts
    values = []
    for post in posts:
        values.append({
            "uri": post.uri,
            "cid": post.cid,
            "author_did": post.author_did,
            "author_handle": post.author_handle,
            "created_at": post.created_at,
            "text": post.text,
            "like_count": post.metrics.like_count,
            "repost_count": post.metrics.repost_count,
            "reply_count": post.metrics.reply_count,
            "quote_count": post.metrics.quote_count,
        })
    
    # Postgres-specific upsert
    stmt = pg_insert(Post).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["uri"],
        set_={
            "cid": stmt.excluded.cid,
            "author_did": stmt.excluded.author_did,
            "author_handle": stmt.excluded.author_handle,
            "created_at": stmt.excluded.created_at,
            "text": stmt.excluded.text,
            "like_count": stmt.excluded.like_count,
            "repost_count": stmt.excluded.repost_count,
            "reply_count": stmt.excluded.reply_count,
            "quote_count": stmt.excluded.quote_count,
        },
    )
    session.execute(stmt)
    return len(posts)


def upsert_edges(session: Session, edges: list[BskyEdge]) -> int:
    """
    Upsert edges into the database.
    Does nothing on conflict (edges are immutable).
    
    Args:
        session: Database session
        edges: List of normalized edge objects from ingestion
        
    Returns:
        Number of edges processed
    """
    if not edges:
        return 0
    
    # Check if we're using SQLite (for testing)
    dialect_name = session.bind.dialect.name
    
    if dialect_name == 'sqlite':
        # SQLite doesn't handle autoincrement with ON CONFLICT well
        # Use a manual approach: check existence and insert only new edges
        # Also deduplicate within the batch itself
        seen = set()
        unique_edges = []
        for edge in edges:
            key = (edge.src_uri, edge.dst_uri, edge.edge_type)
            if key not in seen:
                seen.add(key)
                unique_edges.append(edge)
        
        inserted = 0
        for edge in unique_edges:
            # Check if edge already exists in database
            stmt = select(Edge).where(
                and_(
                    Edge.src_uri == edge.src_uri,
                    Edge.dst_uri == edge.dst_uri,
                    Edge.edge_type == edge.edge_type,
                )
            )
            existing = session.execute(stmt).scalar_one_or_none()
            
            if not existing:
                # Insert new edge - let SQLAlchemy handle autoincrement
                new_edge = Edge(
                    src_uri=edge.src_uri,
                    dst_uri=edge.dst_uri,
                    edge_type=edge.edge_type,
                    created_at=edge.created_at,
                )
                session.add(new_edge)
                inserted += 1
        
        # Flush to ensure IDs are assigned
        if inserted > 0:
            session.flush()
        return len(edges)
    else:
        # PostgreSQL: use native UPSERT
        values = []
        for edge in edges:
            values.append({
                "src_uri": edge.src_uri,
                "dst_uri": edge.dst_uri,
                "edge_type": edge.edge_type,
                "created_at": edge.created_at,
            })
        
        stmt = pg_insert(Edge).values(values)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["src_uri", "dst_uri", "edge_type"],
        )
        session.execute(stmt)
        return len(edges)


def link_run_posts(session: Session, run_id: uuid.UUID, uris: list[str]) -> int:
    """
    Link posts to a run.
    
    Args:
        session: Database session
        run_id: Run UUID
        uris: List of post URIs
        
    Returns:
        Number of links created
    """
    if not uris:
        return 0
    
    values = [{"run_id": run_id, "uri": uri} for uri in uris]
    
    stmt = pg_insert(RunPost).values(values)
    stmt = stmt.on_conflict_do_nothing()
    session.execute(stmt)
    return len(uris)


def link_run_edges(session: Session, run_id: uuid.UUID, edges: list[BskyEdge]) -> int:
    """
    Link edges to a run.
    
    Args:
        session: Database session
        run_id: Run UUID
        edges: List of normalized edge objects
        
    Returns:
        Number of links created
    """
    if not edges:
        return 0
    
    values = []
    for edge in edges:
        values.append({
            "run_id": run_id,
            "src_uri": edge.src_uri,
            "dst_uri": edge.dst_uri,
            "edge_type": edge.edge_type,
            "created_at": edge.created_at,
        })
    
    stmt = pg_insert(RunEdge).values(values)
    stmt = stmt.on_conflict_do_nothing()
    session.execute(stmt)
    return len(edges)


def get_run(session: Session, run_id: uuid.UUID) -> Optional[Run]:
    """
    Fetch a run by ID.
    
    Args:
        session: Database session
        run_id: Run UUID
        
    Returns:
        Run object or None if not found
    """
    stmt = select(Run).where(Run.run_id == run_id)
    result = session.execute(stmt)
    return result.scalar_one_or_none()


def get_run_posts(session: Session, run_id: uuid.UUID) -> list[Post]:
    """
    Fetch all posts linked to a run.
    
    Args:
        session: Database session
        run_id: Run UUID
        
    Returns:
        List of Post objects
    """
    stmt = (
        select(Post)
        .join(RunPost, Post.uri == RunPost.uri)
        .where(RunPost.run_id == run_id)
    )
    result = session.execute(stmt)
    return list(result.scalars().all())


def get_run_edges(session: Session, run_id: uuid.UUID) -> list[tuple[str, str, str, Optional[datetime]]]:
    """
    Fetch all edges linked to a run.
    Returns tuples of (src_uri, dst_uri, edge_type, created_at).
    
    Args:
        session: Database session
        run_id: Run UUID
        
    Returns:
        List of edge tuples
    """
    stmt = select(
        RunEdge.src_uri,
        RunEdge.dst_uri,
        RunEdge.edge_type,
        RunEdge.created_at,
    ).where(RunEdge.run_id == run_id)
    
    result = session.execute(stmt)
    return list(result.all())
