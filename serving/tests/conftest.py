"""Test configuration.

`Settings` validates at import and has no default for `redis_url`, which is
deliberate: a service that silently starts against the wrong Redis is worse
than one that refuses to start at all. That does mean the environment has to be
populated before any application module is imported, which is what this does.
"""

import os

# Must run before the first `from app...` import anywhere in the suite.
os.environ.setdefault("RECO_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RECO_MODEL_PATH", "/tmp/models/current")
os.environ.setdefault("RECO_INDEX_PATH", "/tmp/models/index")
os.environ.setdefault("RECO_CATALOG_PATH", "/tmp/models/catalog.json")

import pytest  # noqa: E402


@pytest.fixture
def profile_features() -> dict:
    """A representative online feature vector, matching what Flink writes."""
    return {
        "profile_id": 101,
        "sessions_7d": 12,
        "sessions_30d": 41,
        "avg_completion_7d": 0.62,
        "completion_rate_30d": 0.48,
        "days_since_last_session": 1,
        "tenure_days": 430,
        "engagement_segment": "regular",
        "tier": "premium",
        "genre_affinity": {"thriller": 0.4, "drama": 0.35, "comedy": 0.25},
        "recent_title_ids": [11, 12, 13],
    }
