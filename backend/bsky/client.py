import hashlib
import json
import logging
import random
import time
from typing import Any, Optional

import httpx
from redis import Redis
from redis.exceptions import RedisError

from .models import IngestConfig

logger = logging.getLogger(__name__)


class RequestStats:
    """Track request statistics."""
    def __init__(self):
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.failed_requests = 0

    def reset(self):
        """Reset statistics."""
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.failed_requests = 0


class BlueskyClient:
    """HTTP client with caching and retry logic for Bluesky API."""

    BASE_URL = "https://api.bsky.app"

    def __init__(self, config: Optional[IngestConfig] = None):
        self.config = config or IngestConfig()
        self.stats = RequestStats()
        self._request_count = 0
        
        # Initialize Redis client
        self._redis: Optional[Redis] = None
        if self.config.redis_enabled:
            try:
                self._redis = Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    db=self.config.redis_db,
                    decode_responses=True,
                    socket_connect_timeout=2,
                )
                # Test connection
                self._redis.ping()
                logger.info("Redis connection established")
            except (RedisError, Exception) as e:
                logger.warning(f"Redis unavailable, caching disabled: {e}")
                self._redis = None

        # Initialize HTTP client
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            timeout=httpx.Timeout(
                connect=self.config.connect_timeout,
                read=self.config.read_timeout,
                write=self.config.connect_timeout,
                pool=self.config.connect_timeout,
            ),
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; SourceGraph/1.0)",
            },
        )

    def close(self):
        self._client.close()
        if self._redis:
            self._redis.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _make_cache_key(self, endpoint: str, params: dict[str, Any]) -> str:
        """Create a cache key from endpoint and params.

        Args:
            endpoint: API endpoint name
            params: Query parameters (will be normalized)

        Returns:
            Cache key string
        """
        # Normalize params by sorting keys
        normalized = json.dumps(params, sort_keys=True)
        param_hash = hashlib.md5(normalized.encode()).hexdigest()
        return f"bsky:{endpoint}:{param_hash}"

    def _get_ttl_for_endpoint(self, endpoint: str) -> int:
        """Get TTL for a specific endpoint.

        Args:
            endpoint: API endpoint name

        Returns:
            TTL in seconds
        """
        if "searchPosts" in endpoint:
            return self.config.search_ttl
        elif "getPostThread" in endpoint or "getQuotes" in endpoint:
            return self.config.thread_ttl
        elif "getPosts" in endpoint:
            return self.config.posts_ttl
        return self.config.search_ttl

    def _get_from_cache(self, cache_key: str) -> Optional[dict]:
        """Get response from cache.

        Args:
            cache_key: Cache key

        Returns:
            Cached response or None
        """
        if not self._redis:
            return None

        try:
            cached = self._redis.get(cache_key)
            if cached:
                self.stats.cache_hits += 1
                logger.debug(f"Cache hit: {cache_key}")
                return json.loads(cached)
        except (RedisError, Exception) as e:
            logger.warning(f"Cache read error: {e}")

        return None

    def _set_cache(self, cache_key: str, data: dict, ttl: int):
        """Set response in cache.

        Args:
            cache_key: Cache key
            data: Data to cache
            ttl: Time to live in seconds
        """
        if not self._redis:
            return

        try:
            self._redis.setex(cache_key, ttl, json.dumps(data))
            logger.debug(f"Cached: {cache_key} (TTL: {ttl}s)")
        except (RedisError, Exception) as e:
            logger.warning(f"Cache write error: {e}")

    def _check_budget(self):
        if self._request_count >= self.config.max_requests_per_run:
            raise RuntimeError(
                f"Request budget exhausted ({self.config.max_requests_per_run})"
            )

    def get(self, endpoint: str, params: Optional[dict[str, Any]] = None) -> dict:
        self._check_budget()

        params = params or {}
        cache_key = self._make_cache_key(endpoint, params)
        ttl = self._get_ttl_for_endpoint(endpoint)

        # Try cache first
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        self.stats.cache_misses += 1

        # Make request with retry logic
        attempt = 0
        backoff = self.config.initial_backoff

        while attempt < self.config.max_retries:
            try:
                start_time = time.time()
                self._request_count += 1
                self.stats.total_requests += 1

                response = self._client.get(f"/xrpc/{endpoint}", params=params)
                latency = time.time() - start_time

                logger.info(
                    f"API request: {endpoint} | status={response.status_code} | "
                    f"latency={latency:.2f}s | attempt={attempt + 1}"
                )

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = (
                        float(retry_after)
                        if retry_after
                        else backoff + random.uniform(0, 1)
                    )
                    logger.warning(f"Rate limited, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    attempt += 1
                    continue

                # Handle server errors with retry
                if response.status_code >= 500:
                    logger.warning(
                        f"Server error {response.status_code}, retrying in {backoff:.2f}s"
                    )
                    time.sleep(backoff + random.uniform(0, 1))
                    backoff = min(backoff * 2, self.config.max_backoff)
                    attempt += 1
                    continue

                # Raise for other errors
                response.raise_for_status()

                # Parse and cache response
                data = response.json()
                self._set_cache(cache_key, data, ttl)
                return data

            except httpx.HTTPError as e:
                self.stats.failed_requests += 1
                logger.error(f"HTTP error on attempt {attempt + 1}: {e}")
                if attempt >= self.config.max_retries - 1:
                    raise RuntimeError(f"Max retries reached for {endpoint}: {e}")
                backoff = min(backoff * 2, self.config.max_backoff)
                time.sleep(backoff + random.uniform(0, 1))
                attempt += 1

        raise RuntimeError(f"Failed to fetch {endpoint} after {self.config.max_retries} attempts")

    def reset_stats(self):
        self.stats.reset()

    def reset_budget(self):
        self._request_count = 0

    def get_remaining_budget(self) -> int:
        return max(0, self.config.max_requests_per_run - self._request_count)
