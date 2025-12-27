from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field


class PostMetrics(BaseModel):
    """Post engagement metrics."""
    like_count: int = 0
    repost_count: int = 0
    reply_count: int = 0
    quote_count: int = 0


class Post(BaseModel):
    """Normalized post object."""
    uri: str
    cid: Optional[str] = None
    author_did: str
    author_handle: str
    created_at: datetime
    text: str = ""
    metrics: PostMetrics = Field(default_factory=PostMetrics)

    def __hash__(self):
        return hash(self.uri)

    def __eq__(self, other):
        if not isinstance(other, Post):
            return False
        return self.uri == other.uri


class Edge(BaseModel):
    """Normalized edge representing relationships between posts."""
    src_uri: str
    dst_uri: str
    edge_type: Literal["QUOTE", "REPLY"]
    created_at: Optional[datetime] = None

    def __hash__(self):
        return hash((self.src_uri, self.dst_uri, self.edge_type))

    def __eq__(self, other):
        if not isinstance(other, Edge):
            return False
        return (
            self.src_uri == other.src_uri
            and self.dst_uri == other.dst_uri
            and self.edge_type == other.edge_type
        )


class IngestConfig(BaseModel):
    """Configuration for Bluesky ingestion."""
    # Redis settings
    redis_enabled: bool = True
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # Cache TTLs (seconds)
    search_ttl: int = 120
    thread_ttl: int = 600
    quotes_ttl: int = 600
    posts_ttl: int = 1800
    
    # Rate limiting
    connect_timeout: float = 5.0
    read_timeout: float = 20.0
    max_retries: int = 3
    initial_backoff: float = 1.0
    max_backoff: float = 60.0
    
    # Request budget
    max_requests_per_run: int = 500
    
    # API defaults
    default_page_size: int = 25
    max_page_size: int = 100


class QueryModeInputs(BaseModel):
    """Inputs for query mode."""
    query: str
    time_window_hours: Optional[int] = None
    max_pages: int = 4
    page_size: int = 25
    lang: Optional[str] = None


class SeedModeInputs(BaseModel):
    """Inputs for seed mode."""
    seed_uri: str
    max_depth: int = 2
    max_quote_pages: int = 3
    max_nodes: int = 500


class IngestResult(BaseModel):
    """Result from ingestion."""
    posts: list[Post]
    edges: list[Edge]
    total_requests: int
    cache_hits: int
    cache_misses: int
