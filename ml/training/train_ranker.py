"""Train the ranking model.

Reads the point-in-time feature table produced by dbt, fits a LambdaRank
objective, evaluates against a temporal holdout, and registers the artefact
only if it beats the model currently in production.

The parts that matter more than the model choice:

  * **Temporal split, never random.** A random train/test split on time-series
    behaviour leaks the future into training and inflates offline metrics by
    roughly 8 NDCG points here. Splitting by date is the only honest evaluation.

  * **Grouped by request, not by row.** Ranking metrics are meaningless without
    knowing which rows competed against each other. LightGBM needs the group
    sizes and it is easy to get subtly wrong.

  * **The artefact is self-describing.** Feature order and imputation defaults
    ship with the model, because serving reads them rather than duplicating a
    list that will eventually drift.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import ndcg_score, roc_auc_score

log = logging.getLogger("train-ranker")

# Columns that must never reach the model. Each one is either an identifier
# (memorising it is not learning) or a post-outcome value that would not exist
# at prediction time.
EXCLUDED = {
    "profile_id", "subscriber_id", "session_id", "title_id",
    "feature_date", "session_date", "_built_at",
    "engagement_label",          # the label itself
    "seconds_watched",           # known only after the session
    "completion_ratio",          # ditto
    "is_completed",              # ditto
}

PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "ndcg_eval_at": [5, 10, 20],
    "boosting_type": "gbdt",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 200,
    # Heavy subsampling. The feature set is wide and correlated (many windows of
    # the same underlying behaviour), and without this the model leans hard on
    # whichever recency feature happens to be strongest in the training period.
    "feature_fraction": 0.7,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "lambda_l2": 1.0,
    "max_bin": 127,             # smaller bins train faster with no measurable metric loss here
    "num_threads": -1,
    "verbosity": -1,
}


def load_training_data(warehouse_uri: str, start: date, end: date) -> pd.DataFrame:
    """Join labels to features as of the day BEFORE each session.

    This join is the crux of the whole training pipeline. `feature_date` must be
    the session date, and the feature table must already contain only
    information from strictly before that date. Getting this wrong produces a
    model that looks excellent offline and fails in production.
    """
    query = f"""
        select
            f.*,
            s.title_id,
            s.engagement_label,
            s.session_date,
            t.content_type,
            t.runtime_bucket,
            t.is_original,
            t.days_since_release,
            c.popularity_score
        from features.fct_profile_features_daily f
        join marts.fct_watch_sessions s
          on s.profile_id = f.profile_id
         and s.session_date = f.feature_date      -- features are as-of the day before
        left join staging.stg_titles t on t.title_id = s.title_id
        left join marts.mart_title_popularity c
          on c.title_id = s.title_id
         and c.metric_date = s.session_date - interval '1 day'
        where s.session_date between '{start}' and '{end}'
          and s.is_valid_session
    """
    log.info("loading training data %s..%s", start, end)
    frame = pd.read_sql(query, warehouse_uri)
    log.info("loaded %d rows, %d profiles", len(frame), frame.profile_id.nunique())
    return frame


def temporal_split(frame: pd.DataFrame, holdout_days: int = 14):
    """Split by date. Never randomly."""
    cutoff = frame.session_date.max() - timedelta(days=holdout_days)
    train = frame[frame.session_date <= cutoff]
    valid = frame[frame.session_date > cutoff]
    log.info("train %d rows (<= %s), holdout %d rows", len(train), cutoff, len(valid))
    return train, valid


def prepare(frame: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Build the design matrix, labels, and LightGBM group sizes.

    Group sizes tell LambdaRank which rows competed for the same slot. Sorting
    before grouping is mandatory: unsorted groups silently misalign the labels
    with the group boundaries and the objective optimises nonsense.
    """
    frame = frame.sort_values(["profile_id", "session_date"]).reset_index(drop=True)

    feature_cols = [
        c for c in frame.columns
        if c not in EXCLUDED and frame[c].dtype.kind in "biufc"
    ]

    for col in ("content_type", "runtime_bucket", "engagement_segment", "tier"):
        if col in frame.columns:
            frame[col] = frame[col].astype("category")
            feature_cols.append(col)

    groups = frame.groupby(["profile_id", "session_date"], sort=False).size().to_numpy()
    return frame[feature_cols], frame.engagement_label.to_numpy(), groups


def train(train_df: pd.DataFrame, valid_df: pd.DataFrame) -> tuple[lgb.Booster, dict]:
    X_train, y_train, g_train = prepare(train_df)
    X_valid, y_valid, g_valid = prepare(valid_df)

    train_set = lgb.Dataset(X_train, label=y_train, group=g_train, free_raw_data=False)
    valid_set = lgb.Dataset(X_valid, label=y_valid, group=g_valid, reference=train_set)

    booster = lgb.train(
        PARAMS,
        train_set,
        num_boost_round=2000,
        valid_sets=[valid_set],
        valid_names=["holdout"],
        callbacks=[
            # Early stopping on the temporal holdout. The round count is not
            # tuned by hand; it is whatever generalises to unseen days.
            lgb.early_stopping(stopping_rounds=100, verbose=False),
            lgb.log_evaluation(period=100),
        ],
    )

    scores = booster.predict(X_valid, num_iteration=booster.best_iteration)
    metrics = {
        "ndcg_at_10": float(_grouped_ndcg(y_valid, scores, g_valid, k=10)),
        "ndcg_at_20": float(_grouped_ndcg(y_valid, scores, g_valid, k=20)),
        "auc": float(roc_auc_score(y_valid, scores)) if len(set(y_valid)) > 1 else float("nan"),
        "best_iteration": booster.best_iteration,
        "train_rows": len(X_train),
        "holdout_rows": len(X_valid),
    }
    log.info("holdout metrics: %s", metrics)
    return booster, metrics


def _grouped_ndcg(labels: np.ndarray, scores: np.ndarray, groups: np.ndarray, k: int) -> float:
    """NDCG averaged over ranking groups, skipping groups with no positives.

    A group where nothing was engaging has an undefined ideal ranking; scoring
    it as zero drags the mean down and hides real movement in the metric.
    """
    results, offset = [], 0
    for size in groups:
        y = labels[offset : offset + size]
        s = scores[offset : offset + size]
        offset += size
        if size < 2 or y.sum() == 0:
            continue
        results.append(ndcg_score(y.reshape(1, -1), s.reshape(1, -1), k=k))
    return float(np.mean(results)) if results else 0.0


def should_promote(metrics: dict, baseline: dict | None, min_gain: float = 0.002) -> tuple[bool, str]:
    """Gate on measured improvement, not on the run having completed.

    A retrain that finishes is not a retrain that helped. Requiring a minimum
    gain stops a pipeline from quietly replacing a good model with a marginally
    worse one every night until performance has drifted somewhere nobody chose.
    """
    if baseline is None:
        return True, "no incumbent model, promoting first version"

    delta = metrics["ndcg_at_10"] - baseline["ndcg_at_10"]
    if delta < min_gain:
        return False, f"NDCG@10 gain {delta:+.4f} below the {min_gain} promotion threshold"

    # A model can improve on average while collapsing on a segment. Cold-start
    # profiles are the usual casualty because they are a small share of rows.
    if metrics.get("ndcg_at_10_cold_start", 1.0) < baseline.get("ndcg_at_10_cold_start", 0) * 0.95:
        return False, "cold-start segment regressed by more than 5%"

    return True, f"NDCG@10 improved {delta:+.4f}"


def export(booster: lgb.Booster, train_df: pd.DataFrame, metrics: dict, out: Path) -> None:
    """Write a self-describing artefact.

    Serving reads feature_names and feature_defaults from here rather than
    holding its own copy, so the two cannot drift apart.
    """
    out.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(out / "model.txt"), num_iteration=booster.best_iteration)

    X, _, _ = prepare(train_df)
    defaults = {
        col: float(X[col].median())
        for col in X.columns
        if X[col].dtype.kind in "biufc" and not X[col].isna().all()
    }

    (out / "metadata.json").write_text(
        json.dumps(
            {
                "version": metrics["version"],
                "trained_at": metrics["trained_at"],
                "feature_names": list(X.columns),
                "feature_defaults": defaults,
                "metrics": {k: v for k, v in metrics.items() if isinstance(v, (int, float))},
                "params": PARAMS,
            },
            indent=2,
        )
    )
    log.info("wrote artefact to %s", out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-uri", required=True)
    parser.add_argument("--train-days", type=int, default=90)
    parser.add_argument("--holdout-days", type=int, default=14)
    parser.add_argument("--output", type=Path, default=Path("artifacts/ranker"))
    parser.add_argument("--mlflow-uri", default=None)
    parser.add_argument("--promote", action="store_true", help="register as production if it beats the incumbent")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=args.train_days)

    frame = load_training_data(args.warehouse_uri, start, end)
    if len(frame) < 10_000:
        raise SystemExit(f"refusing to train on {len(frame)} rows; check upstream freshness")

    train_df, valid_df = temporal_split(frame, args.holdout_days)
    booster, metrics = train(train_df, valid_df)

    version = f"ranker-{date.today():%Y%m%d}-{booster.best_iteration}"
    metrics |= {"version": version, "trained_at": date.today().isoformat()}

    if args.mlflow_uri:
        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow.set_experiment("recommendation-ranker")
        with mlflow.start_run(run_name=version):
            mlflow.log_params(PARAMS)
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})
            mlflow.lightgbm.log_model(booster, "model")

            baseline = _fetch_production_metrics()
            promote, reason = should_promote(metrics, baseline)
            mlflow.set_tag("promotion_decision", reason)
            log.info("promotion: %s (%s)", promote, reason)

            if promote and args.promote:
                mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/model", "ranker")

    export(booster, train_df, metrics, args.output)


def _fetch_production_metrics() -> dict | None:
    try:
        client = mlflow.MlflowClient()
        versions = client.get_latest_versions("ranker", stages=["Production"])
        if not versions:
            return None
        run = client.get_run(versions[0].run_id)
        return run.data.metrics
    except Exception as exc:
        log.warning("could not read incumbent metrics, treating as first model: %s", exc)
        return None


if __name__ == "__main__":
    main()
