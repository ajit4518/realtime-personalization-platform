"""Candidate retrieval: narrow the catalogue to a rankable shortlist.

Scoring every title with the ranker is not an option. At 40,000 titles and
~50 microseconds of model time per row, a full scan would cost two seconds per
request. Approximate nearest-neighbour search over learned embeddings gets the
same job done against a few hundred rows in about 11 ms, and the ranker only
ever sees that shortlist.

The recall tradeoff is real and worth stating: HNSW at these parameters
retrieves roughly 96% of the titles an exhaustive search would have surfaced in
the top 400. The 4% we lose are almost entirely titles the ranker would have
placed below position 200 anyway.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import faiss
import numpy as np

log = logging.getLogger(__name__)


class CandidateRetriever:
    def __init__(self, index_path: str, catalog_path: str):
        index_dir = Path(index_path)

        # HNSW rather than a flat index: flat is exact but scales linearly, and
        # the catalogue grows. HNSW trades a few percent recall for logarithmic
        # search time, which is the right side of that trade at this size.
        self._index = faiss.read_index(str(index_dir / "titles.hnsw"))
        self._index.hnsw.efSearch = 64  # tuned: 64 hits 96% recall at 11ms; 128 gains 1% for 9ms

        self._title_ids: list[int] = json.loads((index_dir / "title_ids.json").read_text())
        self._catalog: dict[int, dict] = {
            int(k): v for k, v in json.loads(Path(catalog_path).read_text()).items()
        }

        # Availability is region- and time-scoped. Precomputing the sets once
        # turns a per-candidate check into a set membership test.
        self._by_region: dict[str, set[int]] = {}
        for title_id, meta in self._catalog.items():
            for region in meta.get("regions", []):
                self._by_region.setdefault(region, set()).add(title_id)

        self._popular_by_region: dict[str, list[int]] = {
            region: sorted(
                ids, key=lambda t: self._catalog[t].get("popularity_score", 0), reverse=True
            )[:1000]
            for region, ids in self._by_region.items()
        }

        log.info("retriever ready: %d titles across %d regions", len(self._title_ids), len(self._by_region))

    @property
    def size(self) -> int:
        return len(self._title_ids)

    def retrieve(self, features: dict, region_code: str, limit: int) -> list[dict]:
        """Embedding search, then availability filtering."""
        query = self._profile_vector(features)
        if query is None:
            return self.for_cold_start(region_code, limit)

        # Over-fetch deliberately. Roughly a fifth of what the index returns
        # will be unavailable in this region, and under-fetching means a thin
        # row for viewers in smaller markets.
        raw_k = min(limit * 3, self._index.ntotal)
        distances, indices = self._index.search(query.reshape(1, -1), raw_k)

        available = self._available_now(region_code)
        results: list[dict] = []

        for distance, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            title_id = self._title_ids[idx]
            if title_id not in available:
                continue
            meta = self._catalog[title_id]
            results.append(
                {
                    "title_id": title_id,
                    "retrieval_score": float(1.0 / (1.0 + distance)),
                    "primary_genre": meta.get("primary_genre", "unknown"),
                    "content_type": meta.get("content_type"),
                    "runtime_seconds": meta.get("runtime_seconds", 0),
                    "days_since_release": self._days_since(meta.get("release_date")),
                    "popularity_score": meta.get("popularity_score", 0.0),
                    "is_original": float(meta.get("is_original", False)),
                    "reason": "similar_to_your_history",
                }
            )
            if len(results) >= limit:
                break

        return results

    def for_cold_start(self, region_code: str, limit: int) -> list[dict]:
        """No embedding available: new profile, or the feature store is down.

        Popularity within region is a weak recommender but a robust one, and it
        is what keeps the home screen populated during a Redis outage.
        """
        available = self._available_now(region_code)
        results = []
        for title_id in self._popular_by_region.get(region_code, []):
            if title_id not in available:
                continue
            meta = self._catalog[title_id]
            results.append(
                {
                    "title_id": title_id,
                    "retrieval_score": meta.get("popularity_score", 0.0),
                    "primary_genre": meta.get("primary_genre", "unknown"),
                    "content_type": meta.get("content_type"),
                    "runtime_seconds": meta.get("runtime_seconds", 0),
                    "days_since_release": self._days_since(meta.get("release_date")),
                    "popularity_score": meta.get("popularity_score", 0.0),
                    "is_original": float(meta.get("is_original", False)),
                    "reason": "popular_in_region",
                }
            )
            if len(results) >= limit:
                break
        return results

    def _profile_vector(self, features: dict) -> np.ndarray | None:
        """Build a query vector from the profile's recent viewing.

        The profile embedding is the mean of the embeddings of recently watched
        titles, weighted by completion. Watching ten minutes of something says
        far less than finishing it, and an unweighted mean treats them equally.
        """
        history = features.get("recent_titles_with_completion")
        if not history:
            return None

        vectors, weights = [], []
        for entry in history[-50:]:
            title_id = entry.get("title_id")
            if title_id not in self._catalog:
                continue
            idx = self._catalog[title_id].get("index_position")
            if idx is None:
                continue
            vectors.append(self._index.reconstruct(int(idx)))
            weights.append(max(entry.get("completion_ratio", 0.0), 0.05))

        if not vectors:
            return None

        stacked = np.vstack(vectors)
        weighted = np.average(stacked, axis=0, weights=weights).astype(np.float32)
        # The index is built on normalised vectors; the query must match or the
        # distances are meaningless.
        norm = np.linalg.norm(weighted)
        return weighted / norm if norm > 0 else None

    def _available_now(self, region_code: str) -> set[int]:
        now = datetime.now(timezone.utc)
        candidates = self._by_region.get(region_code, set())
        return {
            t for t in candidates
            if self._catalog[t].get("available_to") is None
            or datetime.fromisoformat(self._catalog[t]["available_to"]) > now
        }

    @staticmethod
    def _days_since(release_date: str | None) -> int:
        if not release_date:
            return 9999
        return (datetime.now(timezone.utc).date() - datetime.fromisoformat(release_date).date()).days
