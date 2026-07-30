"""Tests for the recommendation API.

The emphasis is on degradation behaviour rather than the happy path. A
recommender that returns good results when everything works is easy; one that
keeps returning *something* when Redis is down is the difference between a
degraded home screen and an outage.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from app.features import CircuitBreaker


# ── circuit breaker ───────────────────────────────────────────────────────

def test_circuit_stays_closed_below_threshold():
    breaker = CircuitBreaker(threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    assert not breaker.is_open


def test_circuit_opens_at_threshold():
    breaker = CircuitBreaker(threshold=3)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.is_open


def test_success_resets_failure_count():
    breaker = CircuitBreaker(threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert not breaker.is_open, "a success should clear accumulated failures"


def test_circuit_half_opens_after_reset_window(monkeypatch):
    breaker = CircuitBreaker(threshold=1, reset_seconds=5.0)
    clock = {"now": 1000.0}
    monkeypatch.setattr("app.features.time.monotonic", lambda: clock["now"])

    breaker.record_failure()
    assert breaker.is_open

    clock["now"] += 6.0
    assert not breaker.is_open, "circuit should allow a probe request after the reset window"


# ── ranking ───────────────────────────────────────────────────────────────

def test_ranker_builds_matrix_in_declared_feature_order(tmp_path):
    """The failure this guards against is silent, which is why it is tested.

    If serving assembles features in a different order than training, the model
    returns confident nonsense and nothing raises.
    """
    from app.ranker import Ranker

    (tmp_path / "metadata.json").write_text(
        json.dumps(
            {
                "version": "test-1",
                "feature_names": ["b_feature", "a_feature", "c_feature"],
                "feature_defaults": {"a_feature": 0.5, "b_feature": 1.5, "c_feature": 2.5},
            }
        )
    )

    ranker = Ranker.__new__(Ranker)
    ranker._feature_names = ["b_feature", "a_feature", "c_feature"]
    ranker._defaults = {"a_feature": 0.5, "b_feature": 1.5, "c_feature": 2.5}

    matrix = ranker._build_matrix(
        profile_features={"a_feature": 10.0},
        candidates=[{"title_id": 1, "b_feature": 20.0}],
    )

    assert matrix[0][0] == 20.0, "b_feature must land in column 0, matching the declared order"
    assert matrix[0][1] == 10.0
    assert matrix[0][2] == 2.5, "missing feature must fall back to its training default"


def test_missing_features_use_training_defaults_not_zero():
    """Zero is a meaningful value for most of these columns, so imputing it
    would teach the model that an absent feature means an inactive viewer."""
    from app.ranker import Ranker

    ranker = Ranker.__new__(Ranker)
    ranker._feature_names = ["sessions_7d"]
    ranker._defaults = {"sessions_7d": 3.0}

    matrix = ranker._build_matrix(profile_features={}, candidates=[{"title_id": 1}])
    assert matrix[0][0] == 3.0


# ── presentation rules ────────────────────────────────────────────────────

def test_genre_diversity_cap_is_enforced():
    from app.main import _apply_presentation_rules

    ranked = [(i, 0.9 - i * 0.01, {"primary_genre": "thriller"}) for i in range(10)]
    result = _apply_presentation_rules(ranked, features={}, limit=10)

    assert len(result) <= 3, "a single genre must not fill the whole row"


def test_recently_watched_titles_are_suppressed():
    from app.main import _apply_presentation_rules

    ranked = [(1, 0.99, {"primary_genre": "drama"}), (2, 0.98, {"primary_genre": "comedy"})]
    result = _apply_presentation_rules(
        ranked, features={"recent_title_ids": [1]}, limit=10
    )

    assert [r.title_id for r in result] == [2]


def test_diversity_cap_preserves_relative_ranking():
    from app.main import _apply_presentation_rules

    ranked = [
        (1, 0.9, {"primary_genre": "drama"}),
        (2, 0.8, {"primary_genre": "comedy"}),
        (3, 0.7, {"primary_genre": "drama"}),
    ]
    result = _apply_presentation_rules(ranked, features={}, limit=10)

    scores = [r.score for r in result]
    assert scores == sorted(scores, reverse=True)


# ── degradation ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_feature_store_timeout_falls_back_rather_than_failing():
    """A slow feature store must produce popularity-based results, not a 500."""
    from app.features import FeatureStore

    store = FeatureStore("redis://unused")
    store._client = AsyncMock()
    store._client.get = AsyncMock(side_effect=asyncio.TimeoutError)

    result = await store.get(profile_id=42)
    assert result is None, "a timeout must return None so the caller can fall back"


@pytest.mark.asyncio
async def test_open_circuit_short_circuits_without_calling_redis():
    from app.features import FeatureStore

    store = FeatureStore("redis://unused")
    store._client = AsyncMock()
    for _ in range(10):
        store._breaker.record_failure()

    await store.get(profile_id=1)
    store._client.get.assert_not_called()
