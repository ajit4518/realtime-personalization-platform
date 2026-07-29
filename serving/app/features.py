"""Online feature store client.

Reads the feature vectors that the Flink job writes. Everything here exists to
make a dependency that can be slow, unavailable, or missing a key not take down
the recommendation path.
"""

from __future__ import annotations

import json
import logging
import time

import redis.asyncio as redis

from .settings import settings

log = logging.getLogger(__name__)


class CircuitBreaker:
    """Stops hammering a dependency that is already failing.

    Without this, a Redis outage means every request waits for its full timeout
    before falling back. At a few thousand requests per second that queues
    faster than it drains and the API falls over from a failure it was supposed
    to tolerate. Opening the circuit converts a hard outage into an instant,
    cheap fallback.
    """

    def __init__(self, threshold: int = 10, reset_seconds: float = 15.0):
        self._threshold = threshold
        self._reset_seconds = reset_seconds
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at >= self._reset_seconds:
            # Half-open: let the next request through to test recovery.
            self._opened_at = None
            self._failures = 0
            return False
        return True

    def record_success(self) -> None:
        self._failures = 0

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self._threshold and self._opened_at is None:
            self._opened_at = time.monotonic()
            log.error("feature store circuit opened after %d failures", self._failures)


class FeatureStore:
    def __init__(self, url: str):
        self._url = url
        self._client: redis.Redis | None = None
        self._breaker = CircuitBreaker()

    async def connect(self) -> None:
        self._client = redis.from_url(
            self._url,
            decode_responses=True,
            # Aggressive timeouts on purpose. This call has a single-digit
            # millisecond budget; waiting a full second for a degraded Redis is
            # never the right trade when a fallback exists.
            socket_timeout=settings.feature_timeout_seconds,
            socket_connect_timeout=1.0,
            max_connections=settings.redis_pool_size,
            health_check_interval=30,
            retry_on_timeout=False,
        )
        await self._client.ping()

    async def get(self, profile_id: int) -> dict | None:
        """Fresh session features written by the streaming job."""
        if self._breaker.is_open or self._client is None:
            return None
        try:
            raw = await self._client.get(f"feat:session:{profile_id}")
            self._breaker.record_success()
            return json.loads(raw) if raw else None
        except (redis.RedisError, OSError) as exc:
            self._breaker.record_failure()
            log.warning("online feature read failed for %s: %s", profile_id, exc)
            return None

    async def get_offline(self, profile_id: int) -> dict:
        """Nightly features from the warehouse, loaded into Redis by Airflow.

        These are up to 24 hours stale but always present, which makes them a
        good floor: a viewer who has not generated events today still gets
        personalised results based on their history.
        """
        if self._breaker.is_open or self._client is None:
            return {}
        try:
            raw = await self._client.get(f"feat:daily:{profile_id}")
            self._breaker.record_success()
            return json.loads(raw) if raw else {}
        except (redis.RedisError, OSError):
            self._breaker.record_failure()
            return {}

    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except (redis.RedisError, OSError):
            return False

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
