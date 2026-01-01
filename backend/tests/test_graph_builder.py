"""Tests for graph builder logic."""
from datetime import datetime, timezone

import pytest

from app.db.models import Post
from app.services.runs_service import _build_graph


def test_graph_builder_basic():
    """Test basic graph building with degrees."""
    # Create test posts
    posts = [
        Post(
            uri="post1",
            author_did="did:plc:1",
            author_handle="user1",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="Post 1",
            like_count=10,
            repost_count=5,
            reply_count=2,
            quote_count=1,
        ),
        Post(
            uri="post2",
            author_did="did:plc:2",
            author_handle="user2",
            created_at=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            text="Post 2",
            like_count=20,
            repost_count=10,
            reply_count=4,
            quote_count=2,
        ),
        Post(
            uri="post3",
            author_did="did:plc:3",
            author_handle="user3",
            created_at=datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc),
            text="Post 3",
            like_count=5,
            repost_count=2,
            reply_count=1,
            quote_count=0,
        ),
    ]
    
    # Create test edges
    # post2 quotes post1, post3 quotes post1
    edges = [
        ("post2", "post1", "QUOTE", datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc)),
        ("post3", "post1", "QUOTE", datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)),
    ]
    
    # Build graph
    graph = _build_graph(posts, edges, max_nodes=None)
    
    # Verify node count
    assert graph.stats.node_count == 3
    assert graph.stats.edge_count == 2
    
    # Verify degrees
    # post1: in_degree=2 (quoted by post2 and post3), out_degree=0
    # post2: in_degree=0, out_degree=1 (quotes post1)
    # post3: in_degree=0, out_degree=1 (quotes post1)
    node_map = {node.uri: node for node in graph.nodes}
    
    assert node_map["post1"].in_degree == 2
    assert node_map["post1"].out_degree == 0
    assert node_map["post2"].in_degree == 0
    assert node_map["post2"].out_degree == 1
    assert node_map["post3"].in_degree == 0
    assert node_map["post3"].out_degree == 1
    
    # Verify time range
    assert graph.stats.time_min == datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert graph.stats.time_max == datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)


def test_graph_builder_max_nodes_filtering():
    """Test that max_nodes filtering works correctly."""
    # Create test posts with varying engagement
    posts = [
        Post(
            uri="post1",
            author_did="did:plc:1",
            author_handle="user1",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="High engagement",
            like_count=100,
            repost_count=50,
            reply_count=20,
            quote_count=10,
        ),  # Score: 180
        Post(
            uri="post2",
            author_did="did:plc:2",
            author_handle="user2",
            created_at=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            text="Medium engagement",
            like_count=50,
            repost_count=25,
            reply_count=10,
            quote_count=5,
        ),  # Score: 90
        Post(
            uri="post3",
            author_did="did:plc:3",
            author_handle="user3",
            created_at=datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc),
            text="Low engagement",
            like_count=5,
            repost_count=2,
            reply_count=1,
            quote_count=0,
        ),  # Score: 8
    ]
    
    # Create edges between all posts
    edges = [
        ("post2", "post1", "QUOTE", None),
        ("post3", "post1", "QUOTE", None),
        ("post3", "post2", "REPLY", None),
    ]
    
    # Build graph with max_nodes=2
    graph = _build_graph(posts, edges, max_nodes=2)
    
    # Should keep post1 and post2 (highest engagement)
    assert graph.stats.node_count == 2
    uris = {node.uri for node in graph.nodes}
    assert uris == {"post1", "post2"}
    
    # Edges should be filtered to only include edges between remaining nodes
    # post2 -> post1 (kept)
    # post3 -> post1 (removed, post3 not in graph)
    # post3 -> post2 (removed, post3 not in graph)
    assert graph.stats.edge_count == 1
    assert graph.edges[0].src == "post2"
    assert graph.edges[0].dst == "post1"
    
    # Verify degrees are recomputed after filtering
    node_map = {node.uri: node for node in graph.nodes}
    assert node_map["post1"].in_degree == 1  # Only from post2
    assert node_map["post2"].out_degree == 1  # Only to post1


def test_graph_builder_isolated_nodes():
    """Test graph with nodes that have no edges."""
    posts = [
        Post(
            uri="post1",
            author_did="did:plc:1",
            author_handle="user1",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="Isolated post",
            like_count=10,
            repost_count=5,
            reply_count=2,
            quote_count=1,
        ),
        Post(
            uri="post2",
            author_did="did:plc:2",
            author_handle="user2",
            created_at=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            text="Another isolated post",
            like_count=20,
            repost_count=10,
            reply_count=4,
            quote_count=2,
        ),
    ]
    
    edges = []  # No edges
    
    graph = _build_graph(posts, edges, max_nodes=None)
    
    assert graph.stats.node_count == 2
    assert graph.stats.edge_count == 0
    
    # All nodes should have degree 0
    for node in graph.nodes:
        assert node.in_degree == 0
        assert node.out_degree == 0


def test_graph_builder_edge_filtering():
    """Test that edges pointing to nodes outside the graph are filtered out."""
    posts = [
        Post(
            uri="post1",
            author_did="did:plc:1",
            author_handle="user1",
            created_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            text="Post 1",
            like_count=10,
            repost_count=5,
            reply_count=2,
            quote_count=1,
        ),
        Post(
            uri="post2",
            author_did="did:plc:2",
            author_handle="user2",
            created_at=datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            text="Post 2",
            like_count=20,
            repost_count=10,
            reply_count=4,
            quote_count=2,
        ),
    ]
    
    # Edges include references to a post not in the graph
    edges = [
        ("post1", "post2", "QUOTE", None),
        ("post1", "post_external", "REPLY", None),  # Should be filtered
        ("post_external", "post2", "QUOTE", None),  # Should be filtered
    ]
    
    graph = _build_graph(posts, edges, max_nodes=None)
    
    # Only 1 edge should remain
    assert graph.stats.edge_count == 1
    assert graph.edges[0].src == "post1"
    assert graph.edges[0].dst == "post2"
    
    # Verify degrees
    node_map = {node.uri: node for node in graph.nodes}
    assert node_map["post1"].out_degree == 1
    assert node_map["post2"].in_degree == 1
