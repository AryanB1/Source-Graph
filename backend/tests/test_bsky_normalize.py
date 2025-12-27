"""
Unit tests for Bluesky ingestion module.
"""

import pytest
from datetime import datetime

from bsky.models import Post, PostMetrics, Edge
from bsky.normalize import (
    normalize_post,
    parse_timestamp,
    extract_quote_edges,
    deduplicate_posts,
    deduplicate_edges,
    normalize_thread_node,
)


class TestParseTimestamp:
    """Tests for timestamp parsing."""

    def test_parse_iso8601_with_z(self):
        """Test parsing ISO 8601 timestamp with Z."""
        result = parse_timestamp("2024-01-15T10:30:45.123Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_iso8601_with_offset(self):
        """Test parsing ISO 8601 timestamp with timezone offset."""
        result = parse_timestamp("2024-01-15T10:30:45.123+00:00")
        assert result is not None
        assert result.year == 2024

    def test_parse_invalid_timestamp(self):
        """Test parsing invalid timestamp returns None."""
        result = parse_timestamp("invalid")
        assert result is None

    def test_parse_none(self):
        """Test parsing None returns None."""
        result = parse_timestamp(None)
        assert result is None


class TestNormalizePost:
    """Tests for post normalization."""

    def test_normalize_valid_post(self):
        """Test normalizing a valid post."""
        raw_post = {
            "uri": "at://did:plc:xyz/app.bsky.feed.post/abc123",
            "cid": "bafyxyz",
            "author": {
                "did": "did:plc:xyz",
                "handle": "alice.bsky.social",
            },
            "record": {
                "text": "Hello Bluesky!",
                "createdAt": "2024-01-15T10:30:45.123Z",
            },
            "likeCount": 10,
            "repostCount": 5,
            "replyCount": 3,
            "quoteCount": 2,
        }

        post = normalize_post(raw_post)
        assert post is not None
        assert post.uri == "at://did:plc:xyz/app.bsky.feed.post/abc123"
        assert post.cid == "bafyxyz"
        assert post.author_did == "did:plc:xyz"
        assert post.author_handle == "alice.bsky.social"
        assert post.text == "Hello Bluesky!"
        assert post.metrics.like_count == 10
        assert post.metrics.repost_count == 5
        assert post.metrics.reply_count == 3
        assert post.metrics.quote_count == 2

    def test_normalize_post_missing_text(self):
        """Test normalizing a post with missing text."""
        raw_post = {
            "uri": "at://did:plc:xyz/app.bsky.feed.post/abc123",
            "cid": "bafyxyz",
            "author": {
                "did": "did:plc:xyz",
                "handle": "alice.bsky.social",
            },
            "record": {
                "createdAt": "2024-01-15T10:30:45.123Z",
            },
        }

        post = normalize_post(raw_post)
        assert post is not None
        assert post.text == ""

    def test_normalize_post_missing_metrics(self):
        """Test normalizing a post with missing metrics."""
        raw_post = {
            "uri": "at://did:plc:xyz/app.bsky.feed.post/abc123",
            "author": {
                "did": "did:plc:xyz",
                "handle": "alice.bsky.social",
            },
            "record": {
                "text": "Test",
                "createdAt": "2024-01-15T10:30:45.123Z",
            },
        }

        post = normalize_post(raw_post)
        assert post is not None
        assert post.metrics.like_count == 0
        assert post.metrics.repost_count == 0
        assert post.metrics.reply_count == 0
        assert post.metrics.quote_count == 0

    def test_normalize_post_missing_uri(self):
        """Test normalizing a post without URI returns None."""
        raw_post = {
            "author": {
                "did": "did:plc:xyz",
                "handle": "alice.bsky.social",
            },
            "record": {
                "text": "Test",
                "createdAt": "2024-01-15T10:30:45.123Z",
            },
        }

        post = normalize_post(raw_post)
        assert post is None

    def test_normalize_post_missing_author(self):
        """Test normalizing a post without author returns None."""
        raw_post = {
            "uri": "at://did:plc:xyz/app.bsky.feed.post/abc123",
            "record": {
                "text": "Test",
                "createdAt": "2024-01-15T10:30:45.123Z",
            },
        }

        post = normalize_post(raw_post)
        assert post is None


class TestExtractQuoteEdges:
    """Tests for quote edge extraction."""

    def test_extract_quote_edges(self):
        """Test extracting quote edges from posts."""
        target_uri = "at://did:plc:target/app.bsky.feed.post/target123"
        
        quote_posts = [
            {
                "uri": "at://did:plc:quote1/app.bsky.feed.post/quote1",
                "author": {
                    "did": "did:plc:quote1",
                    "handle": "quoter1.bsky.social",
                },
                "record": {
                    "text": "Quoting this!",
                    "createdAt": "2024-01-15T10:30:45.123Z",
                },
            },
            {
                "uri": "at://did:plc:quote2/app.bsky.feed.post/quote2",
                "author": {
                    "did": "did:plc:quote2",
                    "handle": "quoter2.bsky.social",
                },
                "record": {
                    "text": "Also quoting!",
                    "createdAt": "2024-01-15T11:30:45.123Z",
                },
            },
        ]

        posts, edges = extract_quote_edges(quote_posts, target_uri)

        assert len(posts) == 2
        assert len(edges) == 2

        # Check edges
        assert edges[0].src_uri == "at://did:plc:quote1/app.bsky.feed.post/quote1"
        assert edges[0].dst_uri == target_uri
        assert edges[0].edge_type == "QUOTE"

        assert edges[1].src_uri == "at://did:plc:quote2/app.bsky.feed.post/quote2"
        assert edges[1].dst_uri == target_uri
        assert edges[1].edge_type == "QUOTE"

    def test_extract_quote_edges_empty(self):
        """Test extracting quote edges from empty list."""
        posts, edges = extract_quote_edges([], "at://test")
        assert len(posts) == 0
        assert len(edges) == 0


class TestDeduplication:
    """Tests for deduplication logic."""

    def test_deduplicate_posts(self):
        """Test deduplicating posts by URI."""
        posts = [
            Post(
                uri="at://did:plc:xyz/app.bsky.feed.post/1",
                author_did="did:plc:xyz",
                author_handle="alice.bsky.social",
                created_at=datetime.now(),
                text="Post 1",
            ),
            Post(
                uri="at://did:plc:xyz/app.bsky.feed.post/2",
                author_did="did:plc:xyz",
                author_handle="alice.bsky.social",
                created_at=datetime.now(),
                text="Post 2",
            ),
            Post(
                uri="at://did:plc:xyz/app.bsky.feed.post/1",  # Duplicate
                author_did="did:plc:xyz",
                author_handle="alice.bsky.social",
                created_at=datetime.now(),
                text="Post 1 (duplicate)",
            ),
        ]

        deduped = deduplicate_posts(posts)
        assert len(deduped) == 2
        assert deduped[0].uri == "at://did:plc:xyz/app.bsky.feed.post/1"
        assert deduped[1].uri == "at://did:plc:xyz/app.bsky.feed.post/2"

    def test_deduplicate_edges(self):
        """Test deduplicating edges."""
        edges = [
            Edge(
                src_uri="at://post1",
                dst_uri="at://post2",
                edge_type="REPLY",
            ),
            Edge(
                src_uri="at://post3",
                dst_uri="at://post4",
                edge_type="QUOTE",
            ),
            Edge(
                src_uri="at://post1",  # Duplicate
                dst_uri="at://post2",
                edge_type="REPLY",
            ),
        ]

        deduped = deduplicate_edges(edges)
        assert len(deduped) == 2
        assert deduped[0].src_uri == "at://post1"
        assert deduped[0].edge_type == "REPLY"
        assert deduped[1].src_uri == "at://post3"
        assert deduped[1].edge_type == "QUOTE"

    def test_deduplicate_edges_different_types(self):
        """Test that edges with same URIs but different types are kept."""
        edges = [
            Edge(
                src_uri="at://post1",
                dst_uri="at://post2",
                edge_type="REPLY",
            ),
            Edge(
                src_uri="at://post1",
                dst_uri="at://post2",
                edge_type="QUOTE",
            ),
        ]

        deduped = deduplicate_edges(edges)
        assert len(deduped) == 2


class TestNormalizeThreadNode:
    """Tests for thread node normalization."""

    def test_normalize_simple_thread(self):
        """Test normalizing a simple thread with replies."""
        thread_node = {
            "$type": "app.bsky.feed.defs#threadViewPost",
            "post": {
                "uri": "at://did:plc:root/app.bsky.feed.post/root",
                "author": {
                    "did": "did:plc:root",
                    "handle": "root.bsky.social",
                },
                "record": {
                    "text": "Root post",
                    "createdAt": "2024-01-15T10:00:00.000Z",
                },
            },
            "replies": [
                {
                    "$type": "app.bsky.feed.defs#threadViewPost",
                    "post": {
                        "uri": "at://did:plc:reply/app.bsky.feed.post/reply1",
                        "author": {
                            "did": "did:plc:reply",
                            "handle": "replier.bsky.social",
                        },
                        "record": {
                            "text": "Reply 1",
                            "createdAt": "2024-01-15T10:05:00.000Z",
                        },
                    },
                }
            ],
        }

        posts = {}
        edges = []
        normalize_thread_node(thread_node, posts, edges)

        assert len(posts) == 2
        assert len(edges) == 1

        # Check edge: reply -> root
        assert edges[0].src_uri == "at://did:plc:reply/app.bsky.feed.post/reply1"
        assert edges[0].dst_uri == "at://did:plc:root/app.bsky.feed.post/root"
        assert edges[0].edge_type == "REPLY"

    def test_normalize_thread_with_blocked_post(self):
        """Test normalizing a thread with blocked posts."""
        thread_node = {
            "$type": "app.bsky.feed.defs#threadViewPost",
            "post": {
                "uri": "at://did:plc:root/app.bsky.feed.post/root",
                "author": {
                    "did": "did:plc:root",
                    "handle": "root.bsky.social",
                },
                "record": {
                    "text": "Root post",
                    "createdAt": "2024-01-15T10:00:00.000Z",
                },
            },
            "replies": [
                {
                    "$type": "app.bsky.feed.defs#blockedPost",
                }
            ],
        }

        posts = {}
        edges = []
        normalize_thread_node(thread_node, posts, edges)

        # Should only have the root post, blocked reply is skipped
        assert len(posts) == 1
        assert len(edges) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
