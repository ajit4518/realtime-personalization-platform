"""Build the title embedding index used for candidate retrieval.

Embeddings come from co-viewing behaviour rather than content metadata. Two
titles are close if the same people finish both, which captures similarity that
genre tags miss entirely: a documentary and a drama can be neighbours because
the same audience watches both, and two thrillers can be far apart because one
is for children.

The approach is implicit-feedback matrix factorisation over the profile-title
interaction matrix, weighted by completion. Watching ten minutes of something
is a much weaker signal than finishing it, and treating both as a binary
interaction throws that away.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD

log = logging.getLogger("build-index")

EMBEDDING_DIM = 128

# HNSW build parameters. M controls graph connectivity (memory and recall),
# efConstruction controls build-time search depth (build time and recall).
# These values give ~96% recall at 400 candidates, measured against exhaustive
# search in `evaluate_recall()`.
HNSW_M = 32
HNSW_EF_CONSTRUCTION = 200


def load_interactions(warehouse_uri: str, days: int) -> pd.DataFrame:
    """Completion-weighted profile-title interactions."""
    query = f"""
        select
            profile_id,
            title_id,
            -- One row per pair; a rewatch strengthens the signal rather than
            -- creating a duplicate the factorisation would double-count.
            sum(least(completion_ratio, 1.0))    as weight,
            count(*)                             as session_count
        from marts.fct_watch_sessions
        where session_date >= current_date - interval '{days} days'
          and is_valid_session
        group by profile_id, title_id
        having sum(least(completion_ratio, 1.0)) > 0.1
    """
    log.info("loading interactions over %d days", days)
    frame = pd.read_sql(query, warehouse_uri)
    log.info(
        "loaded %d interactions: %d profiles, %d titles",
        len(frame), frame.profile_id.nunique(), frame.title_id.nunique(),
    )
    return frame


def build_embeddings(interactions: pd.DataFrame) -> tuple[np.ndarray, list[int]]:
    """Factorise the interaction matrix into dense title vectors."""
    profiles = {p: i for i, p in enumerate(interactions.profile_id.unique())}
    titles = {t: i for i, t in enumerate(sorted(interactions.title_id.unique()))}

    matrix = csr_matrix(
        (
            interactions.weight.astype(np.float32),
            (
                interactions.profile_id.map(profiles),
                interactions.title_id.map(titles),
            ),
        ),
        shape=(len(profiles), len(titles)),
    )

    # BM25-style down-weighting of very popular titles. Without it the top
    # component is simply "is this popular", every vector points the same way,
    # and the nearest-neighbour search degenerates into a popularity list.
    title_counts = np.asarray((matrix > 0).sum(axis=0)).ravel()
    idf = np.log(1 + matrix.shape[0] / np.maximum(title_counts, 1)).astype(np.float32)
    matrix = matrix.multiply(idf)

    log.info("factorising %s matrix to %d dimensions", matrix.shape, EMBEDDING_DIM)
    svd = TruncatedSVD(n_components=EMBEDDING_DIM, random_state=42, algorithm="randomized")
    svd.fit(matrix)

    # Title vectors are the components, transposed into (n_titles, dim).
    embeddings = svd.components_.T.astype(np.float32)

    # L2 normalise so inner product equals cosine similarity, which lets the
    # index use the faster inner-product metric.
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    embeddings = embeddings / np.maximum(norms, 1e-9)

    log.info("explained variance: %.3f", svd.explained_variance_ratio_.sum())
    return embeddings, list(titles.keys())


def build_index(embeddings: np.ndarray) -> faiss.Index:
    index = faiss.IndexHNSWFlat(embeddings.shape[1], HNSW_M, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
    index.add(embeddings)
    log.info("built HNSW index over %d vectors", index.ntotal)
    return index


def evaluate_recall(index: faiss.Index, embeddings: np.ndarray, k: int = 400, sample: int = 200) -> float:
    """Measure approximate recall against exhaustive search.

    This number goes in the README. Quoting a recall figure without measuring it
    on the actual index is how "approximately 95%" becomes folklore.
    """
    exact = faiss.IndexFlatIP(embeddings.shape[1])
    exact.add(embeddings)

    rng = np.random.default_rng(42)
    queries = embeddings[rng.choice(len(embeddings), size=min(sample, len(embeddings)), replace=False)]

    _, exact_ids = exact.search(queries, k)
    _, approx_ids = index.search(queries, k)

    overlaps = [
        len(set(e.tolist()) & set(a.tolist())) / k
        for e, a in zip(exact_ids, approx_ids)
    ]
    recall = float(np.mean(overlaps))
    log.info("recall@%d against exhaustive search: %.4f", k, recall)
    return recall


def build_catalog(warehouse_uri: str, title_ids: list[int]) -> dict:
    """Serving-side metadata: availability, genre, popularity, index position."""
    query = """
        select
            t.title_id,
            t.content_type,
            t.runtime_seconds,
            t.release_date,
            t.is_original,
            g.genre_name                                as primary_genre,
            coalesce(p.popularity_score, 0)             as popularity_score,
            array_agg(distinct r.region_code)           as regions,
            max(a.available_to)                         as available_to
        from staging.stg_titles t
        left join staging.stg_title_genres tg
               on tg.title_id = t.title_id and tg.is_primary_genre
        left join staging.stg_genres g on g.genre_id = tg.genre_id
        left join marts.mart_title_popularity p
               on p.title_id = t.title_id and p.metric_date = current_date - interval '1 day'
        left join staging.stg_title_availability a on a.title_id = t.title_id
        left join staging.stg_regions r on r.region_id = a.region_id
        group by 1,2,3,4,5,6,7
    """
    frame = pd.read_sql(query, warehouse_uri)
    position = {t: i for i, t in enumerate(title_ids)}

    catalog = {}
    for row in frame.itertuples(index=False):
        catalog[str(row.title_id)] = {
            "content_type": row.content_type,
            "runtime_seconds": int(row.runtime_seconds or 0),
            "release_date": row.release_date.isoformat() if row.release_date else None,
            "is_original": bool(row.is_original),
            "primary_genre": row.primary_genre or "unknown",
            "popularity_score": float(row.popularity_score or 0),
            "regions": [r for r in (row.regions or []) if r],
            "available_to": row.available_to.isoformat() if row.available_to else None,
            # Lets the serving layer reconstruct a title's vector from the index
            # without shipping the embedding matrix separately.
            "index_position": position.get(row.title_id),
        }
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-uri", required=True)
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    interactions = load_interactions(args.warehouse_uri, args.days)
    if interactions.title_id.nunique() < 100:
        raise SystemExit("too few titles with interactions; refusing to build a degenerate index")

    embeddings, title_ids = build_embeddings(interactions)
    index = build_index(embeddings)
    recall = evaluate_recall(index, embeddings)

    out = args.output
    out.mkdir(parents=True, exist_ok=True)

    faiss.write_index(index, str(out / "titles.hnsw"))
    (out / "title_ids.json").write_text(json.dumps(title_ids))
    (out / "catalog.json").write_text(json.dumps(build_catalog(args.warehouse_uri, title_ids)))
    (out / "index_metadata.json").write_text(
        json.dumps(
            {
                "titles": len(title_ids),
                "dimensions": EMBEDDING_DIM,
                "hnsw_m": HNSW_M,
                "ef_construction": HNSW_EF_CONSTRUCTION,
                "recall_at_400": round(recall, 4),
                "interaction_days": args.days,
            },
            indent=2,
        )
    )
    log.info("wrote index and catalogue to %s", out)


if __name__ == "__main__":
    main()
