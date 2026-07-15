"""Flink job: sessionize playback telemetry and publish online features.

This is the low-latency half of the feature platform. It reads raw player
events, maintains per-session and per-profile state, and writes a compact
feature vector to Redis that the recommendation API reads on every request.

The contract that makes this whole design work: the feature names and the
arithmetic here must match `warehouse/dbt/models/features/` exactly. The
offline models train on dbt output, the online API serves from Redis, and if
the two drift the model degrades silently in production. Parity is asserted by
`airflow/dags/feature_parity_check.py`, which samples both paths daily.

Latency budget for this job, measured end to end (player emit -> Redis visible):
    p50  1.4s
    p99  3.8s
Dominated by the 2-second watermark bound, which is deliberate: allowing more
lateness would raise correctness but push us past the 5s SLO.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from typing import Iterable

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    CheckpointingMode,
    KeyedProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.state import (
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)

log = logging.getLogger(__name__)

# Sessions end after this much silence. Chosen from the observed distribution of
# heartbeat gaps: 30 minutes captures a viewer pausing to make dinner but still
# closes out a client that died without sending a terminal event.
SESSION_GAP_MS = 30 * 60 * 1000

# How long a profile's rolling features stay warm in Flink state. Beyond this we
# fall back to the offline features dbt computes nightly.
PROFILE_STATE_TTL_DAYS = 30

BOUNDED_LATENESS_MS = 2_000


@dataclass
class SessionFeatures:
    """The online feature vector. Mirrors `fct_session_features` in dbt.

    Kept deliberately small: this is fetched on every recommendation request,
    so each additional field costs p99 latency across the whole service.
    """

    profile_id: int
    session_id: str
    title_id: int
    region_code: str
    device_type: str

    # Engagement signals
    seconds_watched: int = 0
    completion_ratio: float = 0.0
    seek_count: int = 0
    pause_count: int = 0

    # Quality signals. A viewer being served a degraded stream should not be
    # recommended a 4K-heavy row, so the ranker consumes these directly.
    rebuffer_count: int = 0
    rebuffer_seconds: float = 0.0
    startup_ms: int = 0
    avg_bitrate_kbps: float = 0.0
    had_error: bool = False

    # Rolling profile context, carried across sessions
    sessions_last_7d: int = 0
    distinct_titles_last_7d: int = 0
    genre_affinity: dict = field(default_factory=dict)

    updated_ts: int = 0

    def to_redis_value(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


class SessionizeAndEnrich(KeyedProcessFunction):
    """Builds session state keyed by session_id, with a profile-level side state.

    Uses an event-time timer rather than a Flink session window because we need
    to emit *incremental* updates while the session is still open. A window
    would only fire at session close, which is far too late to personalise the
    next row the viewer sees.
    """

    def open(self, ctx: RuntimeContext):
        session_desc = ValueStateDescriptor("session", Types.STRING())
        # Session state expires shortly after the gap: nothing reads it after
        # the session closes, and unbounded state is how streaming jobs die.
        session_ttl = (
            StateTtlConfig.new_builder(Duration.of_millis(SESSION_GAP_MS * 2))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .cleanup_incrementally(1000, True)
            .build()
        )
        session_desc.enable_time_to_live(session_ttl)
        self._session = ctx.get_state(session_desc)

        profile_desc = MapStateDescriptor("profile_rollup", Types.STRING(), Types.STRING())
        profile_ttl = (
            StateTtlConfig.new_builder(Duration.of_days(PROFILE_STATE_TTL_DAYS))
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .cleanup_incrementally(1000, True)
            .build()
        )
        profile_desc.enable_time_to_live(profile_ttl)
        self._profile = ctx.get_map_state(profile_desc)

        self._seen_events = ctx.get_map_state(
            MapStateDescriptor("dedup", Types.STRING(), Types.BOOLEAN())
        )
        self._emitted = ctx.get_metrics_group().counter("features_emitted")
        self._duplicates = ctx.get_metrics_group().counter("duplicate_events_dropped")

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context) -> Iterable[str]:
        event = json.loads(value)
        event_id = event["event_id"]

        # Client SDKs retry on network failure, so the same event genuinely
        # arrives twice. Deduplicating here keeps seconds_watched honest.
        if self._seen_events.contains(event_id):
            self._duplicates.inc()
            return
        self._seen_events.put(event_id, True)

        state_json = self._session.value()
        if state_json is None:
            features = SessionFeatures(
                profile_id=event["profile_id"],
                session_id=event["session_id"],
                title_id=event["title_id"],
                region_code=event["region_code"],
                device_type=event["device_type"],
            )
        else:
            features = SessionFeatures(**json.loads(state_json))

        self._apply(features, event)
        self._merge_profile_context(features)

        features.updated_ts = event["event_ts"]
        self._session.update(json.dumps(asdict(features)))

        # Close the session out after the inactivity gap even if the client
        # never sends COMPLETE or ABANDON. Mobile clients frequently do not.
        ctx.timer_service().register_event_time_timer(event["event_ts"] + SESSION_GAP_MS)

        self._emitted.inc()
        yield json.dumps(
            {"key": f"feat:session:{features.profile_id}", "value": features.to_redis_value()}
        )

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext) -> Iterable[str]:
        """Session gap elapsed: finalise and let state expire."""
        state_json = self._session.value()
        if state_json is None:
            return
        features = SessionFeatures(**json.loads(state_json))
        self._session.clear()
        yield json.dumps(
            {
                "key": f"feat:session:{features.profile_id}",
                "value": features.to_redis_value(),
                "final": True,
            }
        )

    @staticmethod
    def _apply(f: SessionFeatures, event: dict) -> None:
        etype = event["event_type"]

        if etype == "START":
            f.startup_ms = event.get("startup_ms") or 0
        elif etype == "HEARTBEAT":
            f.seconds_watched += 10
        elif etype == "SEEK":
            f.seek_count += 1
        elif etype == "PAUSE":
            f.pause_count += 1
        elif etype == "REBUFFER":
            f.rebuffer_count += 1
            f.rebuffer_seconds += (event.get("rebuffer_ms") or 0) / 1000.0
        elif etype == "ERROR":
            f.had_error = True
        elif etype == "COMPLETE":
            f.completion_ratio = 1.0

        bitrate = event.get("bitrate_kbps")
        if bitrate:
            # Running mean. Storing the full series would blow up state for a
            # number the ranker only reads as a scalar.
            n = max(f.seconds_watched / 10, 1)
            f.avg_bitrate_kbps += (bitrate - f.avg_bitrate_kbps) / n

        position = event.get("position_seconds") or 0
        if position and f.completion_ratio < 1.0:
            f.seconds_watched = max(f.seconds_watched, position)

    def _merge_profile_context(self, f: SessionFeatures) -> None:
        raw = self._profile.get("rollup")
        rollup = json.loads(raw) if raw else {"sessions": 0, "titles": [], "genres": {}}

        if f.seconds_watched <= 10:  # first meaningful tick of a new session
            rollup["sessions"] += 1
            if f.title_id not in rollup["titles"]:
                rollup["titles"].append(f.title_id)
                rollup["titles"] = rollup["titles"][-200:]

        f.sessions_last_7d = rollup["sessions"]
        f.distinct_titles_last_7d = len(rollup["titles"])
        f.genre_affinity = rollup["genres"]
        self._profile.put("rollup", json.dumps(rollup))


def build_env() -> StreamExecutionEnvironment:
    config = Configuration()
    # RocksDB because profile state is far larger than heap: ~200 bytes per
    # profile across tens of millions of profiles.
    config.set_string("state.backend.type", "rocksdb")
    config.set_string("state.backend.incremental", "true")
    config.set_string("execution.checkpointing.unaligned", "true")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "4")))

    # Exactly-once against Kafka. Checkpoint interval is a direct tradeoff:
    # shorter means faster recovery, longer means less checkpoint overhead.
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    checkpointing = env.get_checkpoint_config()
    checkpointing.set_min_pause_between_checkpoints(10_000)
    checkpointing.set_checkpoint_timeout(120_000)
    checkpointing.set_tolerable_checkpoint_failure_number(3)
    checkpointing.enable_externalized_checkpoints_cleanup(False)
    return env


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    env = build_env()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKERS"])
        .set_topics("playback.events")
        .set_group_id("flink-session-features")
        # Committed offsets on restart, earliest on a brand new deployment.
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(BOUNDED_LATENESS_MS))
        .with_timestamp_assigner(lambda event, _: json.loads(event)["event_ts"])
        # Without this, a quiet partition stalls the watermark for every other
        # partition and session timers never fire.
        .with_idleness(Duration.of_minutes(1))
    )

    events = env.from_source(source, watermark, "playback-events")

    features = (
        events.key_by(lambda e: json.loads(e)["session_id"])
        .process(SessionizeAndEnrich(), output_type=Types.STRING())
        .name("sessionize-and-enrich")
        .uid("sessionize-v1")  # pinned so state survives a job upgrade
    )

    from redis_sink import RedisFeatureSink  # local module, see streaming/jobs/redis_sink.py

    features.sink_to(RedisFeatureSink(os.environ["REDIS_URL"], ttl_seconds=86_400)).name("redis-online-store")

    env.execute("session-features")


if __name__ == "__main__":
    main()
