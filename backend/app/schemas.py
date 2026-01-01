"""DTOs (Data Transfer Objects) for API responses."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class PostMetrics(BaseModel):
    """Post engagement metrics."""
    like_count: int = 0
    repost_count: int = 0
    reply_count: int = 0
    quote_count: int = 0


class GraphNode(BaseModel):
    """A node in the graph response."""
    model_config = ConfigDict(populate_by_name=True)
    
    uri: str
    text: str
    author_handle: str = Field(alias="authorHandle")
    author_did: str = Field(alias="authorDid")
    created_at: datetime = Field(alias="createdAt")
    metrics: PostMetrics
    in_degree: int = Field(alias="inDegree", default=0)
    out_degree: int = Field(alias="outDegree", default=0)


class GraphEdge(BaseModel):
    """An edge in the graph response."""
    src: str
    dst: str
    type: str


class GraphStats(BaseModel):
    """Statistics about the graph."""
    model_config = ConfigDict(populate_by_name=True)
    
    node_count: int = Field(alias="nodeCount")
    edge_count: int = Field(alias="edgeCount")
    time_min: Optional[datetime] = Field(alias="timeMin", default=None)
    time_max: Optional[datetime] = Field(alias="timeMax", default=None)


class GraphDTO(BaseModel):
    """Complete graph response."""
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    stats: GraphStats


class CreateRunRequest(BaseModel):
    """Request to create a new run."""
    model_config = ConfigDict(populate_by_name=True)
    
    mode: str  # "query" or "seed"
    query: Optional[str] = None
    seed_uri: Optional[str] = Field(None, alias="seedUri")
    params: Optional[dict] = Field(default_factory=dict)


class CreateRunResponse(BaseModel):
    """Response from creating a run."""
    model_config = ConfigDict(populate_by_name=True)
    
    run_id: str = Field(alias="runId")
