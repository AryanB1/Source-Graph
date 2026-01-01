"""Service layer for run orchestration and graph assembly."""
import logging
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from bsky.ingest import query_mode, seed_mode
from bsky.models import IngestConfig, QueryModeInputs, SeedModeInputs

from ..db.models import Post
from ..repositories import runs_repository as repo
from ..schemas import GraphDTO, GraphEdge, GraphNode, GraphStats, PostMetrics

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


class IngestionError(Exception):
    """Raised when ingestion fails."""
    pass


class NotFoundError(Exception):
    """Raised when a resource is not found."""
    pass


def create_run_and_ingest(session: Session, payload: dict) -> uuid.UUID:
    """
    Create a run, execute ingestion, and persist results.
    
    This is the main orchestration function that:
    1. Validates input
    2. Calls the appropriate ingestion function
    3. Persists posts, edges, and run links in a single transaction
    
    Args:
        session: Database session (transaction)
        payload: Request payload with mode, query/seedUri, and params
        
    Returns:
        UUID of the created run
        
    Raises:
        ValidationError: If input is invalid
        IngestionError: If ingestion fails
    """
    mode = payload.get("mode")
    query = payload.get("query")
    seed_uri = payload.get("seedUri") or payload.get("seed_uri")
    params = payload.get("params", {})
    
    # Validate inputs
    if mode not in ("query", "seed"):
        raise ValidationError(f"Invalid mode: {mode}. Must be 'query' or 'seed'.")
    
    if mode == "query" and not query:
        raise ValidationError("Query mode requires 'query' field.")
    
    if mode == "seed" and not seed_uri:
        raise ValidationError("Seed mode requires 'seedUri' field.")
    
    # Create run record first
    run_id = repo.create_run(
        session=session,
        mode=mode,
        query=query,
        seed_uri=seed_uri,
        params_json=params,
    )
    
    logger.info(f"Created run {run_id} in {mode} mode")
    
    # Execute ingestion
    try:
        if mode == "query":
            inputs = QueryModeInputs(
                query=query,
                time_window_hours=params.get("timeWindowHours"),
                max_pages=params.get("maxPages", 4),
                page_size=params.get("pageSize", 25),
                lang=params.get("lang"),
            )
            config = IngestConfig()
            result = query_mode(inputs=inputs, config=config)
        else:  # seed mode
            inputs = SeedModeInputs(
                seed_uri=seed_uri,
                max_depth=params.get("maxDepth", 2),
                max_quote_pages=params.get("maxQuotePages", 3),
                max_nodes=params.get("maxNodes", 500),
            )
            config = IngestConfig()
            result = seed_mode(inputs=inputs, config=config)
        
        logger.info(
            f"Ingestion complete for run {run_id}: "
            f"{len(result.posts)} posts, {len(result.edges)} edges, "
            f"{result.total_requests} requests, {result.cache_hits} cache hits"
        )
    except Exception as e:
        logger.error(f"Ingestion failed for run {run_id}: {e}")
        raise IngestionError(f"Ingestion failed: {str(e)}") from e
    
    # Persist results
    try:
        # Upsert posts and edges
        posts_count = repo.upsert_posts(session, result.posts)
        edges_count = repo.upsert_edges(session, result.edges)
        
        # Link posts and edges to run
        uris = [post.uri for post in result.posts]
        repo.link_run_posts(session, run_id, uris)
        repo.link_run_edges(session, run_id, result.edges)
        
        # Commit transaction
        session.commit()
        
        logger.info(
            f"Persisted run {run_id}: "
            f"{posts_count} posts, {edges_count} edges"
        )
        
        return run_id
    except Exception as e:
        logger.error(f"Persistence failed for run {run_id}: {e}")
        session.rollback()
        raise


def get_run_graph(
    session: Session,
    run_id: uuid.UUID,
    max_nodes: Optional[int] = None,
) -> GraphDTO:
    """
    Retrieve and assemble the graph for a run.
    
    This function:
    1. Fetches posts and edges linked to the run
    2. Computes in/out degrees for each node
    3. Optionally filters to top N nodes by engagement
    4. Returns structured graph DTO
    
    Args:
        session: Database session
        run_id: Run UUID
        max_nodes: Optional limit on number of nodes to return
        
    Returns:
        GraphDTO with nodes, edges, and stats
        
    Raises:
        NotFoundError: If run does not exist
    """
    # Verify run exists
    run = repo.get_run(session, run_id)
    if not run:
        raise NotFoundError(f"Run {run_id} not found")
    
    # Fetch posts and edges
    posts = repo.get_run_posts(session, run_id)
    edge_tuples = repo.get_run_edges(session, run_id)
    
    logger.info(
        f"Retrieved graph for run {run_id}: "
        f"{len(posts)} posts, {len(edge_tuples)} edges"
    )
    
    # Build graph
    graph = _build_graph(posts, edge_tuples, max_nodes)
    
    logger.info(
        f"Assembled graph for run {run_id}: "
        f"{graph.stats.node_count} nodes, {graph.stats.edge_count} edges"
    )
    
    return graph


def _build_graph(
    posts: list[Post],
    edge_tuples: list[tuple[str, str, str, Optional[datetime]]],
    max_nodes: Optional[int] = None,
) -> GraphDTO:
    """
    Build a graph DTO from posts and edges.
    
    This helper function:
    1. Optionally filters to top N nodes by engagement score
    2. Filters edges to only include nodes in the graph
    3. Computes in/out degrees
    4. Computes time range statistics
    
    Args:
        posts: List of Post objects
        edge_tuples: List of (src_uri, dst_uri, edge_type, created_at)
        max_nodes: Optional limit on number of nodes
        
    Returns:
        GraphDTO
    """
    # Apply node limit if specified
    if max_nodes and len(posts) > max_nodes:
        logger.info(f"Applying max_nodes filter: {len(posts)} -> {max_nodes}")
        
        # Score nodes by total engagement
        scored_posts = []
        for post in posts:
            score = (
                post.like_count +
                post.repost_count +
                post.reply_count +
                post.quote_count
            )
            scored_posts.append((score, post))
        
        # Sort by score descending and take top N
        scored_posts.sort(key=lambda x: x[0], reverse=True)
        posts = [post for _, post in scored_posts[:max_nodes]]
    
    # Build URI set for filtering edges
    uri_set = {post.uri for post in posts}
    
    # Filter edges to only include nodes in the graph
    filtered_edges = [
        (src, dst, etype, created)
        for src, dst, etype, created in edge_tuples
        if src in uri_set and dst in uri_set
    ]
    
    logger.info(
        f"Filtered edges: {len(edge_tuples)} -> {len(filtered_edges)} "
        f"(kept edges with both endpoints in node set)"
    )
    
    # Compute degrees
    in_degree = {}
    out_degree = {}
    
    for src, dst, _, _ in filtered_edges:
        out_degree[src] = out_degree.get(src, 0) + 1
        in_degree[dst] = in_degree.get(dst, 0) + 1
    
    # Build node DTOs
    nodes = []
    for post in posts:
        node = GraphNode(
            uri=post.uri,
            text=post.text,
            authorHandle=post.author_handle,
            authorDid=post.author_did,
            createdAt=post.created_at,
            metrics=PostMetrics(
                like_count=post.like_count,
                repost_count=post.repost_count,
                reply_count=post.reply_count,
                quote_count=post.quote_count,
            ),
            inDegree=in_degree.get(post.uri, 0),
            outDegree=out_degree.get(post.uri, 0),
        )
        nodes.append(node)
    
    # Build edge DTOs
    edges = [
        GraphEdge(src=src, dst=dst, type=etype)
        for src, dst, etype, _ in filtered_edges
    ]
    
    # Compute stats
    time_min = None
    time_max = None
    if posts:
        timestamps = [post.created_at for post in posts]
        time_min = min(timestamps)
        time_max = max(timestamps)
    
    stats = GraphStats(
        nodeCount=len(nodes),
        edgeCount=len(edges),
        timeMin=time_min,
        timeMax=time_max,
    )
    
    return GraphDTO(nodes=nodes, edges=edges, stats=stats)
