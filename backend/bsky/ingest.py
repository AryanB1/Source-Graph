import logging
from datetime import datetime, timedelta
from typing import Optional

from . import api, normalize
from .client import BlueskyClient
from .models import IngestConfig, IngestResult, QueryModeInputs, SeedModeInputs

logger = logging.getLogger(__name__)


def query_mode(
    inputs: QueryModeInputs,
    config: Optional[IngestConfig] = None,
) -> IngestResult:
    config = config or IngestConfig()
    page_size = min(inputs.page_size, config.max_page_size)
    
    logger.info(
        f"Starting query mode: query='{inputs.query}', "
        f"pages={inputs.max_pages}, page_size={page_size}"
    )

    all_posts = []

    with BlueskyClient(config) as client:
        since = None
        until = None
        if inputs.time_window_hours:
            now = datetime.utcnow()
            since = (now - timedelta(hours=inputs.time_window_hours)).isoformat() + "Z"
            until = now.isoformat() + "Z"

        cursor = None
        pages_fetched = 0

        while pages_fetched < inputs.max_pages:
            try:
                response = api.search_posts(
                    client=client,
                    query=inputs.query,
                    limit=page_size,
                    cursor=cursor,
                    since=since,
                    until=until,
                    lang=inputs.lang,
                )

                raw_posts = response.get("posts", [])
                for raw_post in raw_posts:
                    post = normalize.normalize_post(raw_post)
                    if post:
                        all_posts.append(post)

                pages_fetched += 1
                logger.info(
                    f"Fetched page {pages_fetched}/{inputs.max_pages} "
                    f"({len(raw_posts)} posts)"
                )

                cursor = response.get("cursor")
                if not cursor:
                    logger.info("No more pages available")
                    break

                if client.get_remaining_budget() < 10:
                    logger.warning("Request budget low, stopping pagination")
                    break

            except Exception as e:
                logger.error(f"Error fetching page {pages_fetched + 1}: {e}")
                break

        all_posts = normalize.deduplicate_posts(all_posts)

        logger.info(
            f"Query mode complete: {len(all_posts)} unique posts, "
            f"{client.stats.total_requests} requests, "
            f"{client.stats.cache_hits} cache hits"
        )

        return IngestResult(
            posts=all_posts,
            edges=[],
            total_requests=client.stats.total_requests,
            cache_hits=client.stats.cache_hits,
            cache_misses=client.stats.cache_misses,
        )


def seed_mode(
    inputs: SeedModeInputs,
    config: Optional[IngestConfig] = None,
) -> IngestResult:
    config = config or IngestConfig()

    logger.info(
        f"Starting seed mode: seed_uri={inputs.seed_uri}, "
        f"max_depth={inputs.max_depth}, max_nodes={inputs.max_nodes}"
    )

    all_posts: dict[str, normalize.Post] = {}
    all_edges: list[normalize.Edge] = []

    with BlueskyClient(config) as client:
        try:
            logger.info("Fetching seed post thread...")
            thread_response = api.get_post_thread(
                client=client,
                uri=inputs.seed_uri,
                depth=inputs.max_depth,
                parent_height=3,
            )

            thread_posts, thread_edges = normalize.extract_thread_posts_and_edges(
                thread_response,
                max_depth=inputs.max_depth,
            )

            all_posts.update(thread_posts)
            all_edges.extend(thread_edges)

            logger.info(
                f"Thread extraction: {len(thread_posts)} posts, "
                f"{len(thread_edges)} edges"
            )

        except Exception as e:
            logger.error(f"Failed to fetch thread for {inputs.seed_uri}: {e}")

        if len(all_posts) >= inputs.max_nodes:
            logger.warning(f"Reached max_nodes ({inputs.max_nodes}) after thread")
            all_posts_list = list(all_posts.values())[: inputs.max_nodes]
            all_edges = normalize.deduplicate_edges(all_edges)

            return IngestResult(
                posts=all_posts_list,
                edges=all_edges,
                total_requests=client.stats.total_requests,
                cache_hits=client.stats.cache_hits,
                cache_misses=client.stats.cache_misses,
            )

        try:
            logger.info("Fetching quote posts...")
            cursor = None
            quote_pages_fetched = 0

            while quote_pages_fetched < inputs.max_quote_pages:
                try:
                    quote_response = api.get_quotes(
                        client=client,
                        uri=inputs.seed_uri,
                        limit=50,
                        cursor=cursor,
                    )

                    raw_quote_posts = quote_response.get("posts", [])
                    quote_posts, quote_edges = normalize.extract_quote_edges(
                        raw_quote_posts,
                        inputs.seed_uri,
                    )

                    for post in quote_posts:
                        if len(all_posts) >= inputs.max_nodes:
                            logger.warning(f"Reached max_nodes ({inputs.max_nodes})")
                            break
                        all_posts[post.uri] = post

                    all_edges.extend(quote_edges)

                    quote_pages_fetched += 1
                    logger.info(
                        f"Fetched quote page {quote_pages_fetched}/{inputs.max_quote_pages} "
                        f"({len(quote_posts)} posts)"
                    )

                    cursor = quote_response.get("cursor")
                    if not cursor:
                        logger.info("No more quote pages available")
                        break

                    if client.get_remaining_budget() < 10:
                        logger.warning("Request budget low, stopping quote pagination")
                        break

                    if len(all_posts) >= inputs.max_nodes:
                        break

                except Exception as e:
                    logger.error(f"Error fetching quote page {quote_pages_fetched + 1}: {e}")
                    break

        except Exception as e:
            logger.error(f"Failed to fetch quotes for {inputs.seed_uri}: {e}")

        all_edges = normalize.deduplicate_edges(all_edges)
        all_posts_list = list(all_posts.values())[: inputs.max_nodes]

        logger.info(
            f"Seed mode complete: {len(all_posts_list)} unique posts, "
            f"{len(all_edges)} unique edges, "
            f"{client.stats.total_requests} requests, "
            f"{client.stats.cache_hits} cache hits"
        )

        return IngestResult(
            posts=all_posts_list,
            edges=all_edges,
            total_requests=client.stats.total_requests,
            cache_hits=client.stats.cache_hits,
            cache_misses=client.stats.cache_misses,
        )
