"""Synthetic playback telemetry producer.

Generates realistic viewing sessions so the streaming layer, the feature store
and the anomaly detector all have something to work on locally without needing
production traffic.

Realism that matters for testing the pipeline:
  * sessions are stateful (START then N heartbeats then a terminal event)
  * a configurable slice of events arrive late and out of order, which is what
    actually exercises Flink watermarking
  * one CDN point of presence can be told to degrade, which is how the QoE
    detector is verified end to end

Usage:
    python playback_simulator.py --sessions 500 --rate 200
    python playback_simulator.py --degrade-pop iad-3 --degrade-after 60
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

log = logging.getLogger("playback-simulator")

TOPIC = "playback.events"
DEVICES = ["smart_tv", "mobile", "web", "tablet", "console"]
REGIONS = ["us-east", "eu-west", "ap-south", "ap-south-east", "sa-east"]
POPS = {
    "us-east": ["iad-1", "iad-3", "ord-2"],
    "eu-west": ["lhr-1", "ams-2"],
    "ap-south": ["bom-1", "del-1"],
    "ap-south-east": ["sin-1"],
    "sa-east": ["gru-1"],
}
BITRATE_LADDER = [800, 1600, 3000, 5800, 9500, 16000]
HEARTBEAT_SECONDS = 10


@dataclass
class Session:
    """A single in-flight viewing session."""

    session_id: str
    profile_id: int
    title_id: int
    region_code: str
    cdn_pop: str
    device_type: str
    runtime_seconds: int
    position: int = 0
    bitrate: int = 3000
    started: bool = False
    finished: bool = False
    ab_bucket: str = field(default_factory=lambda: random.choice(["control", "ranker_v3"]))

    def advance(self) -> int:
        self.position += HEARTBEAT_SECONDS
        return self.position

    @property
    def completion_ratio(self) -> float:
        return self.position / self.runtime_seconds


class DegradationWindow:
    """Injects elevated rebuffering into one point of presence.

    Real quality incidents are almost never global; they are scoped to an edge
    location or an ISP. Simulating it that way is the only honest test of a
    detector that is supposed to find them.
    """

    def __init__(self, pop: str | None, start_after: int, severity: float):
        self.pop = pop
        self.start_at = time.monotonic() + start_after if pop else None
        self.severity = severity

    def active_for(self, pop: str) -> bool:
        if not self.pop or pop != self.pop:
            return False
        return time.monotonic() >= self.start_at

    def rebuffer_probability(self, pop: str, base: float) -> float:
        return base + self.severity if self.active_for(pop) else base


class PlaybackProducer:
    def __init__(self, brokers: str, registry_url: str, late_fraction: float):
        registry = SchemaRegistryClient({"url": registry_url})
        schema = (Path(__file__).parents[1] / "schemas" / "playback_event.avsc").read_text()

        self._serializer = AvroSerializer(registry, schema)
        self._key_serializer = StringSerializer("utf_8")
        self._late_fraction = late_fraction
        self._producer = Producer(
            {
                "bootstrap.servers": brokers,
                # Durability first: the whole point of this pipeline is that a
                # broker failure does not silently drop viewing history.
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "zstd",
                # A little batching buys a lot of throughput and costs 20ms.
                "linger.ms": 20,
                "batch.size": 64 * 1024,
                "max.in.flight.requests.per.connection": 5,
            }
        )
        self._delivered = 0
        self._failed = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self._failed += 1
            log.warning("delivery failed: %s", err)
        else:
            self._delivered += 1

    def emit(self, session: Session, event_type: str, **extra) -> None:
        now = datetime.now(timezone.utc)

        # A slice of mobile traffic arrives late; this is what makes watermark
        # handling in the stream layer worth testing.
        event_ts = now
        if random.random() < self._late_fraction:
            event_ts = now - timedelta(seconds=random.randint(20, 240))

        event = {
            "event_id": str(uuid.uuid4()),
            "session_id": session.session_id,
            "profile_id": session.profile_id,
            "title_id": session.title_id,
            "region_code": session.region_code,
            "event_type": event_type,
            "event_ts": int(event_ts.timestamp() * 1000),
            "ingest_ts": int(now.timestamp() * 1000),
            "position_seconds": session.position,
            "device_type": session.device_type,
            "app_version": "8.4.1",
            "cdn_pop": session.cdn_pop,
            "bitrate_kbps": session.bitrate,
            "rebuffer_ms": None,
            "startup_ms": None,
            "error_code": None,
            "ab_bucket": session.ab_bucket,
        }
        event.update(extra)

        ctx = SerializationContext(TOPIC, MessageField.VALUE)
        self._producer.produce(
            topic=TOPIC,
            # Keying by session keeps every event for a session on one partition,
            # so the sessionization operator never has to shuffle to order them.
            key=self._key_serializer(session.session_id),
            value=self._serializer(event, ctx),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)
        log.info("delivered=%d failed=%d", self._delivered, self._failed)


def new_session() -> Session:
    region = random.choice(REGIONS)
    return Session(
        session_id=str(uuid.uuid4()),
        profile_id=random.randint(1, 4000),
        title_id=random.randint(1, 800),
        region_code=region,
        cdn_pop=random.choice(POPS[region]),
        device_type=random.choice(DEVICES),
        runtime_seconds=random.choice([1800, 2700, 3600, 5400, 7200]),
    )


def step(session: Session, producer: PlaybackProducer, degradation: DegradationWindow) -> None:
    """Advance one session by a single tick and emit whatever that implies."""
    if not session.started:
        session.started = True
        producer.emit(
            session,
            "START",
            startup_ms=random.randint(400, 2600),
            bitrate_kbps=session.bitrate,
        )
        return

    # Terminal conditions.
    if session.completion_ratio >= 1.0:
        producer.emit(session, "COMPLETE")
        session.finished = True
        return
    if random.random() < 0.012:
        producer.emit(session, "ABANDON")
        session.finished = True
        return

    session.advance()

    rebuffer_p = degradation.rebuffer_probability(session.cdn_pop, base=0.02)
    if random.random() < rebuffer_p:
        stall = random.randint(300, 4000)
        producer.emit(session, "REBUFFER", rebuffer_ms=stall)
        # Players react to stalls by stepping down the bitrate ladder.
        idx = max(0, BITRATE_LADDER.index(session.bitrate) - 1)
        if BITRATE_LADDER[idx] != session.bitrate:
            session.bitrate = BITRATE_LADDER[idx]
            producer.emit(session, "BITRATE_SHIFT", bitrate_kbps=session.bitrate)
        return

    if random.random() < 0.004:
        producer.emit(session, "ERROR", error_code=random.choice(["DRM_401", "MANIFEST_404", "DECODE_FAIL"]))
        session.finished = True
        return

    if random.random() < 0.03:
        producer.emit(session, "SEEK")
        session.position = max(0, session.position + random.randint(-300, 600))
        return

    producer.emit(session, "HEARTBEAT")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--registry", default="http://localhost:8081")
    parser.add_argument("--sessions", type=int, default=200, help="concurrent sessions")
    parser.add_argument("--rate", type=int, default=100, help="events per second")
    parser.add_argument("--late-fraction", type=float, default=0.03)
    parser.add_argument("--degrade-pop", default=None, help="POP to inject rebuffering into")
    parser.add_argument("--degrade-after", type=int, default=60, help="seconds before degradation starts")
    parser.add_argument("--degrade-severity", type=float, default=0.35)
    parser.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = forever")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    producer = PlaybackProducer(args.brokers, args.registry, args.late_fraction)
    degradation = DegradationWindow(args.degrade_pop, args.degrade_after, args.degrade_severity)
    sessions = [new_session() for _ in range(args.sessions)]

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    interval = 1.0 / max(args.rate, 1)
    deadline = time.monotonic() + args.duration if args.duration else None
    log.info("producing to %s at ~%d events/sec across %d sessions", TOPIC, args.rate, len(sessions))

    try:
        while running:
            if deadline and time.monotonic() > deadline:
                break
            session = random.choice(sessions)
            step(session, producer, degradation)
            if session.finished:
                sessions[sessions.index(session)] = new_session()
            time.sleep(interval)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
