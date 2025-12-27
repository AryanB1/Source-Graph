import logging
from typing import Any, Optional

from .client import BlueskyClient

logger = logging.getLogger(__name__)


def search_posts(
    client: BlueskyClient,
    query: str,
    limit: int = 25,
    cursor: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    lang: Optional[str] = None,
) -> dict[str, Any]:
    params = {"q": query, "limit": min(limit, 100)}
    if cursor:
        params["cursor"] = cursor
    if since:
        params["since"] = since
    if until:
        params["until"] = until
    if lang:
        params["lang"] = lang

    logger.debug(f"Searching posts: query='{query}', limit={limit}")
    return client.get("app.bsky.feed.searchPosts", params)


def get_post_thread(
    client: BlueskyClient,
    uri: str,
    depth: int = 6,
    parent_height: int = 80,
) -> dict[str, Any]:
    params = {"uri": uri, "depth": depth, "parentHeight": parent_height}
    logger.debug(f"Fetching thread: uri={uri}, depth={depth}")
    return client.get("app.bsky.feed.getPostThread", params)


def get_quotes(
    client: BlueskyClient,
    uri: str,
    limit: int = 50,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    params = {"uri": uri, "limit": min(limit, 100)}
    if cursor:
        params["cursor"] = cursor

    logger.debug(f"Fetching quotes: uri={uri}, limit={limit}")
    return client.get("app.bsky.feed.getQuotes", params)


def get_posts(client: BlueskyClient, uris: list[str]) -> dict[str, Any]:
    if not uris:
        return {"posts": []}

    params = {"uris": uris[:25]}
    logger.debug(f"Fetching {len(uris)} posts by URI")
    return client.get("app.bsky.feed.getPosts", params)


def batch_get_posts(client: BlueskyClient, uris: list[str]) -> list[dict[str, Any]]:
    if not uris:
        return []

    all_posts = []
    chunk_size = 25

    for i in range(0, len(uris), chunk_size):
        chunk = uris[i : i + chunk_size]
        try:
            response = get_posts(client, chunk)
            all_posts.extend(response.get("posts", []))
        except Exception as e:
            logger.error(f"Failed to fetch post chunk {i // chunk_size}: {e}")

    return all_posts
