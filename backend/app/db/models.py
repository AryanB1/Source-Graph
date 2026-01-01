"""SQLAlchemy database models for Source Graph."""
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


class Run(Base):
    """
    Represents an ingestion run.
    Each run captures the inputs and results of a single ingestion operation.
    """
    __tablename__ = "runs"

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    mode: Mapped[str] = mapped_column(Text, nullable=False)  # "query" or "seed"
    query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    seed_uri: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )
    params_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class Post(Base):
    """
    Represents a Bluesky post.
    Posts are deduplicated by URI across all runs.
    """
    __tablename__ = "posts"

    uri: Mapped[str] = mapped_column(Text, primary_key=True)
    cid: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author_did: Mapped[str] = mapped_column(Text, nullable=False)
    author_handle: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    text: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    like_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    repost_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    reply_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    quote_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    __table_args__ = (
        Index("ix_posts_created_at", "created_at"),
    )


class Edge(Base):
    """
    Represents a relationship between two posts.
    Edges are deduplicated by (src_uri, dst_uri, edge_type).
    """
    __tablename__ = "edges"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    src_uri: Mapped[str] = mapped_column(Text, nullable=False)
    dst_uri: Mapped[str] = mapped_column(Text, nullable=False)
    edge_type: Mapped[str] = mapped_column(Text, nullable=False)  # "QUOTE" or "REPLY"
    created_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("src_uri", "dst_uri", "edge_type", name="uq_edges_src_dst_type"),
        Index("ix_edges_src_uri", "src_uri"),
        Index("ix_edges_dst_uri", "dst_uri"),
    )


class RunPost(Base):
    """
    Link table between runs and posts.
    Enables deterministic graph retrieval per run.
    """
    __tablename__ = "run_posts"

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    uri: Mapped[str] = mapped_column(Text, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )

    __table_args__ = (
        Index("ix_run_posts_run_id", "run_id"),
    )


class RunEdge(Base):
    """
    Link table between runs and edges.
    Enables deterministic graph retrieval per run.
    Note: We store edge components directly to avoid FK constraint complexity.
    """
    __tablename__ = "run_edges"

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    src_uri: Mapped[str] = mapped_column(Text, primary_key=True)
    dst_uri: Mapped[str] = mapped_column(Text, primary_key=True)
    edge_type: Mapped[str] = mapped_column(Text, primary_key=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        Index("ix_run_edges_run_id", "run_id"),
    )
