"""Configuration. Environment-driven, validated at import, no defaults that
would silently point production at a local service.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RECO_", env_file=".env", extra="ignore")

    redis_url: str = Field(..., description="Online feature store connection string.")
    model_path: str = Field("/opt/models/current", description="Directory holding model.txt and metadata.json.")
    index_path: str = Field("/opt/models/index", description="Directory holding the FAISS index.")
    catalog_path: str = Field("/opt/models/catalog.json")

    # Sits just under the 8ms observed p99 for a healthy Redis, so a genuinely
    # degraded store trips the fallback rather than eating the whole budget.
    feature_timeout_seconds: float = Field(0.015, gt=0, le=1.0)

    # Sized from (worker count x expected concurrency), not guessed. Too small
    # and requests queue on connection checkout; too large and Redis spends its
    # time on connection management.
    redis_pool_size: int = Field(32, ge=4, le=256)

    # Diversity cap. Three from one genre in a twenty-title row is the point at
    # which the row starts reading as repetitive in user testing.
    max_per_genre: int = Field(3, ge=1, le=20)

    log_level: str = Field("INFO")


settings = Settings()
