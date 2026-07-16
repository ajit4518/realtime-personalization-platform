"""Redis sink for the online feature store.

Written as a custom sink rather than using a connector because the write needs
to be a pipelined MSET with per-key TTL, and because a failure to write a
feature must not fail the whole job: a stale feature is recoverable, a crashed
streaming job is not.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis
from pyflink.datastream.connectors import Sink
from pyflink.datastream.functions import RuntimeContext, SinkFunction

log = logging.getLogger(__name__)


class RedisFeatureSink(SinkFunction):
    """Writes feature vectors to the online store.

    Semantics are deliberately at-least-once with last-write-wins. Feature
    writes are idempotent by key, so replaying a checkpoint simply rewrites the
    same value; paying for two-phase commit here would cost latency for no
    correctness gain.
    """

    def __init__(self, redis_url: str, ttl_seconds: int = 86_400, batch_size: int = 100):
        self._url = redis_url
        self._ttl = ttl_seconds
        self._batch_size = batch_size
        self._client: redis.Redis | None = None
        self._buffer: list[tuple[str, str]] = []

    def open(self, ctx: RuntimeContext) -> None:
        self._client = redis.from_url(
            self._url,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
            # The recommendation API is the hot reader; keep this pool small so
            # the sink cannot starve it of connections during a burst.
            max_connections=16,
            health_check_interval=30,
        )
        self._written = ctx.get_metrics_group().counter("redis_writes")
        self._errors = ctx.get_metrics_group().counter("redis_write_errors")

    def invoke(self, value: str, context: Any) -> None:
        record = json.loads(value)
        self._buffer.append((record["key"], record["value"]))
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer or self._client is None:
            return
        try:
            pipe = self._client.pipeline(transaction=False)
            for key, value in self._buffer:
                pipe.set(key, value, ex=self._ttl)
            pipe.execute()
            self._written.inc(len(self._buffer))
        except redis.RedisError as exc:
            # Drop the batch rather than stalling the pipeline. The API falls
            # back to offline features when a key is missing, so the failure
            # mode is degraded personalisation, not an outage.
            self._errors.inc(len(self._buffer))
            log.warning("redis batch write failed, dropping %d features: %s", len(self._buffer), exc)
        finally:
            self._buffer.clear()

    def close(self) -> None:
        self._flush()
        if self._client is not None:
            self._client.close()
