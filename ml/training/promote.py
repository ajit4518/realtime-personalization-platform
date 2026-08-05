"""Promote a registered model version to a serving stage.

Called by the retraining DAG only after the evaluation gate has passed. Kept as
a separate entry point rather than folded into training so that a human can
promote or roll back by hand during an incident without rerunning a training
job.
"""

from __future__ import annotations

import argparse
import logging
import sys

import boto3
import mlflow

log = logging.getLogger("promote")


def promote(model_name: str, stage: str, version: str | None = None) -> str:
    client = mlflow.MlflowClient()

    if version is None:
        candidates = client.search_model_versions(f"name='{model_name}'")
        if not candidates:
            raise SystemExit(f"no registered versions of {model_name}")
        version = max(candidates, key=lambda v: int(v.version)).version

    current = client.get_latest_versions(model_name, stages=[stage])
    if current and current[0].version == version:
        log.info("version %s is already in %s, nothing to do", version, stage)
        return version

    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=stage,
        # The outgoing version moves to Archived rather than being deleted, so
        # a rollback is a stage transition rather than a retrain.
        archive_existing_versions=True,
    )
    log.info("promoted %s version %s to %s", model_name, version, stage)

    if current:
        log.info("previous version %s archived; roll back with --version %s",
                 current[0].version, current[0].version)

    return version


def sync_artifacts_to_s3(model_name: str, version: str, bucket: str) -> None:
    """Copy the promoted artefact to the path the serving pods read.

    Pods pull from `s3://<bucket>/ranker/current/` in an init container. Writing
    to a stable prefix means promoting a model does not require a Helm release
    or an image rebuild; the next pod rotation picks it up.
    """
    client = mlflow.MlflowClient()
    local = client.download_artifacts(
        client.get_model_version(model_name, version).run_id, "model"
    )

    s3 = boto3.client("s3")
    import os

    for root, _, files in os.walk(local):
        for name in files:
            path = os.path.join(root, name)
            key = f"ranker/current/{os.path.relpath(path, local).replace(os.sep, '/')}"
            s3.upload_file(path, bucket, key)
            log.info("uploaded s3://%s/%s", bucket, key)

    # A version marker so an operator can tell what is actually deployed
    # without cross-referencing MLflow.
    s3.put_object(
        Bucket=bucket,
        Key="ranker/current/VERSION",
        Body=f"{model_name}:{version}\n".encode(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-name", default="ranker")
    parser.add_argument("--stage", default="Production", choices=["Staging", "Production", "Archived"])
    parser.add_argument("--version", default=None, help="specific version; defaults to the latest")
    parser.add_argument("--model-bucket", default=None, help="sync artefacts here after promotion")
    parser.add_argument("--mlflow-uri", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.mlflow_uri:
        mlflow.set_tracking_uri(args.mlflow_uri)

    try:
        version = promote(args.model_name, args.stage, args.version)
        if args.model_bucket:
            sync_artifacts_to_s3(args.model_name, version, args.model_bucket)
    except Exception as exc:
        log.error("promotion failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
