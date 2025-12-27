import logging
from datetime import datetime
from typing import Any, Optional

from .models import Edge, Post, PostMetrics

logger = logging.getLogger(__name__)


def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    if not timestamp_str:
        return None

    try:
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
        return None


def normalize_post(post_data: dict[str, Any]) -> Optional[Post]:
    try:
        uri = post_data.get("uri")
        if not uri:
            logger.warning("Post missing URI, skipping")
            return None

        cid = post_data.get("cid")

        author = post_data.get("author", {})
        author_did = author.get("did")
        author_handle = author.get("handle")

        if not author_did or not author_handle:
            logger.warning(f"Post {uri} missing author info, skipping")
            return None

        record = post_data.get("record", {})
        text = record.get("text", "")
        created_at_str = record.get("createdAt") or post_data.get("indexedAt")
        created_at = parse_timestamp(created_at_str)

        if not created_at:
            logger.warning(f"Post {uri} has invalid timestamp, using current time")
            created_at = datetime.now()

        metrics = PostMetrics(
            like_count=post_data.get("likeCount", 0),
            repost_count=post_data.get("repostCount", 0),
            reply_count=post_data.get("replyCount", 0),
            quote_count=post_data.get("quoteCount", 0),
        )

        return Post(
            uri=uri,
            cid=cid,
            author_did=author_did,
            author_handle=author_handle,
            created_at=created_at,
            text=text,
            metrics=metrics,
        )

    except Exception as e:
        logger.error(f"Failed to normalize post: {e}", exc_info=True)
        return None




def normalize_thread_node(
    node: dict[str, Any],
    posts: dict[str, Post],
    edges: list[Edge],
    parent_uri: Optional[str] = None,
    max_depth: int = 10,
    current_depth: int = 0,
) -> Optional[str]:
    if current_depth >= max_depth:
        return None

    node_type = node.get("$type")

    if node_type == "app.bsky.feed.defs#threadViewPost":
        post_data = node.get("post")
        if not post_data:
            return None

        post = normalize_post(post_data)
        if not post:
            return None

        posts[post.uri] = post

        if parent_uri:
            edge = Edge(
                src_uri=post.uri,
                dst_uri=parent_uri,
                edge_type="REPLY",
                created_at=post.created_at,
            )
            if edge not in edges:
                edges.append(edge)

        parent_node = node.get("parent")
        if parent_node:
            normalize_thread_node(
                parent_node,
                posts,
                edges,
                parent_uri=post.uri,
                max_depth=max_depth,
                current_depth=current_depth + 1,
            )

        replies = node.get("replies", [])
        for reply_node in replies:
            normalize_thread_node(
                reply_node,
                posts,
                edges,
                parent_uri=post.uri,
                max_depth=max_depth,
                current_depth=current_depth + 1,
            )

        return post.uri

    elif node_type == "app.bsky.feed.defs#blockedPost":
        logger.debug("Encountered blocked post in thread")
        return None

    elif node_type == "app.bsky.feed.defs#notFoundPost":
        logger.debug("Encountered not found post in thread")
        return None

    else:
        logger.warning(f"Unknown thread node type: {node_type}")
        return None


def extract_thread_posts_and_edges(
    thread_response: dict[str, Any],
    max_depth: int = 10,
) -> tuple[dict[str, Post], list[Edge]]:
    posts: dict[str, Post] = {}
    edges: list[Edge] = []

    thread = thread_response.get("thread")
    if not thread:
        return posts, edges

    normalize_thread_node(thread, posts, edges, max_depth=max_depth)

    return posts, edges


def extract_quote_edges(
    quote_posts: list[dict[str, Any]],
    target_uri: str,
) -> tuple[list[Post], list[Edge]]:
    posts: list[Post] = []
    edges: list[Edge] = []

    for post_data in quote_posts:
        post = normalize_post(post_data)
        if not post:
            continue

        posts.append(post)

        edge = Edge(
            src_uri=post.uri,
            dst_uri=target_uri,
            edge_type="QUOTE",
            created_at=post.created_at,
        )
        edges.append(edge)

    return posts, edges


def deduplicate_posts(posts: list[Post]) -> list[Post]:
    seen = set()
    deduped = []

    for post in posts:
        if post.uri not in seen:
            seen.add(post.uri)
            deduped.append(post)

    return deduped


def deduplicate_edges(edges: list[Edge]) -> list[Edge]:
    seen = set()
    deduped = []

    for edge in edges:
        key = (edge.src_uri, edge.dst_uri, edge.edge_type)
        if key not in seen:
            seen.add(key)
            deduped.append(edge)

    return deduped
