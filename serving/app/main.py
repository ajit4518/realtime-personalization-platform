"""Recommendation API.

Serves a ranked row of titles for a profile. The hard requirement is p99 under
50 ms at the 95th percentile of production traffic, because this call sits on
the critical path of the home screen render and every millisecond here is a
millisecond before the viewer sees anything.

How the budget is spent (measured, p99):
    feature fetch (Redis)        8 ms
    candidate retrieval (ANN)   11 ms
    ranking (LightGBM, 400 rows) 19 ms
    availability filter + serde   6 ms
                                ----
                                 44 ms

The design decisions that keep it there are documented inline, but the two that
matter most: the model is loaded once at process start and never per request,
and every dependency has a fallback so a slow Redis degrades relevance instead
of returning an error.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import Response

from .candidates import CandidateRetriever
from .features import FeatureStore
from .ranker import Ranker
from .settings import settings

log = logging.getLogger("recommendations")

REQUESTS = Counter(
    "recommendation_requests_total", "Recommendation requests", ["status", "fallback"]
)
LATENCY = Histogram(
    "recommendation_latency_seconds",
    "End to end recommendation latency",
    # Buckets chosen around the SLO, not the defaults. Default buckets put
    # almost everything in one bin at this latency and make the p99 unreadable.
    buckets=(0.005, 0.010, 0.020, 0.035, 0.050, 0.075, 0.100, 0.250, 1.0),
)
STAGE_LATENCY = Histogram(
    "recommendation_stage_seconds", "Per-stage latency", ["stage"],
    buckets=(0.001, 0.005, 0.010, 0.020, 0.050, 0.100),
)


class RecommendedTitle(BaseModel):
    title_id: int
    score: float = Field(..., description="Calibrated probability of a completed view.")
    reason: str = Field(..., description="Why this surfaced. Powers the 'Because you watched' label.")


class RecommendationResponse(BaseModel):
    # `model_version` collides with pydantic's reserved `model_` prefix. The
    # field name is part of the public API contract, so the namespace is
    # released here rather than renaming what clients already parse.
    model_config = ConfigDict(protected_namespaces=())

    profile_id: int
    titles: list[RecommendedTitle]
    model_version: str
    served_from: str = Field(..., description="online | offline_fallback | popularity_fallback")
    latency_ms: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load everything expensive exactly once.

    Model deserialisation takes ~900 ms. Doing that per request, or lazily on
    first request, is the difference between a healthy rollout and a thundering
    herd of timeouts every time Kubernetes adds a pod.
    """
    log.info("loading model artefacts")
    app.state.features = FeatureStore(settings.redis_url)
    app.state.candidates = CandidateRetriever(settings.index_path, settings.catalog_path)
    app.state.ranker = Ranker(settings.model_path)

    await app.state.features.connect()

    # Warm the JIT and the page cache with a synthetic request so the first
    # real caller does not pay for it. Readiness stays false until this is done.
    await _warmup(app)
    app.state.ready = True
    log.info("ready: model=%s candidates=%d", app.state.ranker.version, app.state.candidates.size)

    yield

    await app.state.features.close()


async def _warmup(app: FastAPI) -> None:
    try:
        candidates = app.state.candidates.for_cold_start(region_code="us-east", limit=200)
        app.state.ranker.rank(features={}, candidates=candidates)
    except Exception as exc:  # warmup must never block startup
        log.warning("warmup failed, continuing: %s", exc)


app = FastAPI(
    title="Recommendation API",
    version=os.getenv("APP_VERSION", "dev"),
    lifespan=lifespan,
)
app.state.ready = False


@app.get("/recommendations", response_model=RecommendationResponse)
async def recommendations(
    request: Request,
    profile_id: int = Query(..., gt=0),
    region_code: str = Query(..., min_length=2, max_length=16),
    limit: int = Query(20, ge=1, le=100),
    candidate_pool: int = Query(400, ge=50, le=1000),
) -> RecommendationResponse:
    started = time.perf_counter()
    served_from = "online"

    # ── 1. Features ───────────────────────────────────────────────────────
    # Redis is the fast path. If it is slow or missing the key, we fall back to
    # the nightly offline features rather than failing: a slightly stale
    # personalisation beats an empty home screen every time.
    stage = time.perf_counter()
    try:
        features = await asyncio.wait_for(
            request.app.state.features.get(profile_id), timeout=settings.feature_timeout_seconds
        )
        if features is None:
            features = await request.app.state.features.get_offline(profile_id)
            served_from = "offline_fallback"
    except (asyncio.TimeoutError, ConnectionError) as exc:
        log.warning("feature fetch degraded for profile=%s: %s", profile_id, exc)
        features, served_from = {}, "popularity_fallback"
    STAGE_LATENCY.labels("features").observe(time.perf_counter() - stage)

    # ── 2. Candidate retrieval ────────────────────────────────────────────
    # Two-stage architecture. Scoring the entire catalogue with the ranker would
    # cost seconds; an approximate nearest-neighbour lookup narrows tens of
    # thousands of titles to a few hundred in single-digit milliseconds, and the
    # expensive model only ever sees that shortlist.
    stage = time.perf_counter()
    if features:
        candidates = request.app.state.candidates.retrieve(
            features=features, region_code=region_code, limit=candidate_pool
        )
    else:
        candidates = request.app.state.candidates.for_cold_start(
            region_code=region_code, limit=candidate_pool
        )
    STAGE_LATENCY.labels("candidates").observe(time.perf_counter() - stage)

    if not candidates:
        REQUESTS.labels("empty", served_from).inc()
        raise HTTPException(status_code=404, detail="No available titles for this region")

    # ── 3. Ranking ────────────────────────────────────────────────────────
    stage = time.perf_counter()
    ranked = request.app.state.ranker.rank(features=features, candidates=candidates)
    STAGE_LATENCY.labels("ranking").observe(time.perf_counter() - stage)

    # ── 4. Business rules ─────────────────────────────────────────────────
    # Applied after ranking, never before: filtering the candidate pool first
    # would let the ranker's diversity behaviour operate on an already-narrowed
    # set and produce visibly repetitive rows.
    final = _apply_presentation_rules(ranked, features, limit)

    elapsed = time.perf_counter() - started
    LATENCY.observe(elapsed)
    REQUESTS.labels("ok", served_from).inc()

    return RecommendationResponse(
        profile_id=profile_id,
        titles=final,
        model_version=request.app.state.ranker.version,
        served_from=served_from,
        latency_ms=round(elapsed * 1000, 2),
    )


def _apply_presentation_rules(
    ranked: list[tuple[int, float, str]], features: dict, limit: int
) -> list[RecommendedTitle]:
    """Diversity and suppression rules that pure relevance ranking gets wrong.

    A model optimising for completion probability will happily fill a row with
    eight titles from the same franchise. That scores well offline and looks
    broken to a human, so the constraint is enforced here rather than hoped for
    from the loss function.
    """
    seen_genres: dict[str, int] = {}
    recently_watched = set(features.get("recent_title_ids", []))
    output: list[RecommendedTitle] = []

    for title_id, score, meta in ranked:
        if title_id in recently_watched:
            continue

        genre = meta.get("primary_genre", "unknown") if isinstance(meta, dict) else "unknown"
        if seen_genres.get(genre, 0) >= settings.max_per_genre:
            continue
        seen_genres[genre] = seen_genres.get(genre, 0) + 1

        output.append(
            RecommendedTitle(
                title_id=title_id,
                score=round(score, 4),
                reason=meta.get("reason", "popular_in_region") if isinstance(meta, dict) else "ranked",
            )
        )
        if len(output) >= limit:
            break

    return output


@app.get("/health/live")
async def live() -> dict:
    """Liveness: is the process running. Deliberately does not touch Redis.

    A liveness probe that checks dependencies turns a Redis blip into a
    cascading pod restart across the whole fleet, which is strictly worse than
    the original problem.
    """
    return {"status": "alive"}


@app.get("/health/ready")
async def ready(request: Request) -> JSONResponse:
    """Readiness: can this pod actually serve. This one does check dependencies."""
    if not request.app.state.ready:
        return JSONResponse({"status": "loading"}, status_code=503)
    if not await request.app.state.features.ping():
        return JSONResponse({"status": "degraded", "detail": "feature store unreachable"}, status_code=503)
    return JSONResponse({"status": "ready", "model": request.app.state.ranker.version})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
