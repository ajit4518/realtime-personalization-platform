"""Weekly ranker retraining, with a promotion gate.

The DAG deliberately does not promote a model just because training finished.
It trains, evaluates against a temporal holdout, compares to the incumbent, and
only then decides. A pipeline that promotes unconditionally will, over enough
weeks, walk the production model somewhere nobody chose.

    wait_for_features -> build_training_set -> train -> evaluate
                                                          |
                                            +-------------+-------------+
                                            v                           v
                                    promote_model                  skip_promotion
                                            |
                                    rebuild_index -> smoke_test -> notify
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

IMAGE = "{{ var.value.ecr_registry }}/ml-training:{{ var.value.ml_image_tag }}"

default_args = {
    "owner": "ml-platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
}

with DAG(
    dag_id="model_retraining",
    description="Weekly ranker retrain with an evaluation gate before promotion",
    default_args=default_args,
    schedule="0 5 * * 1",  # Monday, after the weekend's data has landed
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=6),
    tags=["ml", "weekly"],
) as dag:

    # Training reads the feature table, so it must not start before the
    # warehouse build that produces it has finished for the same logical date.
    wait_for_features = ExternalTaskSensor(
        task_id="wait_for_warehouse",
        external_dag_id="warehouse_elt",
        external_task_id="finish",
        # warehouse_elt runs at 02:15, this DAG at 05:00, so look back to the
        # same day's run rather than the same execution timestamp.
        execution_delta=pendulum.duration(hours=2, minutes=45),
        poke_interval=300,
        timeout=60 * 90,
        mode="reschedule",
    )

    # Training runs as its own pod so it can request memory the scheduler does
    # not have to reserve permanently. A 32 GB Airflow worker sitting idle six
    # days a week is a straightforward waste of money.
    train = KubernetesPodOperator(
        task_id="train_ranker",
        name="train-ranker",
        namespace="ml-jobs",
        image=IMAGE,
        cmds=["python", "-m", "training.train_ranker"],
        arguments=[
            "--warehouse-uri", "{{ conn.warehouse.get_uri() }}",
            "--train-days", "90",
            "--holdout-days", "14",
            "--output", "/artifacts/ranker",
            "--mlflow-uri", "{{ var.value.mlflow_uri }}",
        ],
        container_resources={
            "request_memory": "16Gi",
            "request_cpu": "4",
            "limit_memory": "24Gi",
            "limit_cpu": "8",
        },
        # Keep the pod around on failure so the logs and any partial artefact
        # survive for debugging. Deleted on success to avoid clutter.
        is_delete_operator_pod=True,
        on_finish_action="delete_succeeded_pod",
        get_logs=True,
        startup_timeout_seconds=600,
    )

    @task(task_id="evaluate")
    def evaluate(**context) -> dict:
        """Read the run's metrics and decide whether they justify a promotion."""
        import mlflow

        mlflow.set_tracking_uri(context["var"]["value"]["mlflow_uri"])
        client = mlflow.MlflowClient()

        experiment = client.get_experiment_by_name("recommendation-ranker")
        runs = client.search_runs(
            [experiment.experiment_id], order_by=["start_time DESC"], max_results=1
        )
        candidate = runs[0].data.metrics

        production = client.get_latest_versions("ranker", stages=["Production"])
        incumbent = client.get_run(production[0].run_id).data.metrics if production else None

        from training.train_ranker import should_promote

        promote, reason = should_promote(candidate, incumbent)
        return {
            "promote": promote,
            "reason": reason,
            "candidate_ndcg": candidate.get("ndcg_at_10"),
            "incumbent_ndcg": incumbent.get("ndcg_at_10") if incumbent else None,
        }

    evaluation = evaluate()

    def _branch(ti) -> str:
        decision = ti.xcom_pull(task_ids="evaluate")
        return "promote_model" if decision["promote"] else "skip_promotion"

    branch = BranchPythonOperator(task_id="promotion_gate", python_callable=_branch)

    promote_model = KubernetesPodOperator(
        task_id="promote_model",
        name="promote-model",
        namespace="ml-jobs",
        image=IMAGE,
        cmds=["python", "-m", "training.promote"],
        arguments=["--model-name", "ranker", "--stage", "Production"],
        is_delete_operator_pod=True,
    )

    rebuild_index = KubernetesPodOperator(
        task_id="rebuild_embedding_index",
        name="rebuild-index",
        namespace="ml-jobs",
        image=IMAGE,
        cmds=["python", "-m", "embeddings.build_index"],
        arguments=[
            "--warehouse-uri", "{{ conn.warehouse.get_uri() }}",
            "--output", "s3://{{ var.value.model_bucket }}/index/{{ ds }}/",
        ],
        container_resources={"request_memory": "8Gi", "request_cpu": "4"},
        is_delete_operator_pod=True,
    )

    @task(task_id="smoke_test_new_model")
    def smoke_test() -> dict:
        """Score a fixed set of profiles against the new model before it serves.

        Catches the failure that metrics cannot: an artefact that loads but
        produces degenerate output, for instance every score identical because a
        feature column went null upstream.
        """
        import statistics

        import requests

        canary = "http://recommendations-canary.serving.svc.cluster.local:8000"
        scores_seen: list[float] = []

        for profile_id in [101, 2048, 9931, 15007, 31142]:
            response = requests.get(
                f"{canary}/recommendations",
                params={"profile_id": profile_id, "region_code": "us-east", "limit": 20},
                timeout=5,
            )
            response.raise_for_status()
            payload = response.json()

            assert payload["titles"], f"empty recommendations for profile {profile_id}"
            scores = [t["score"] for t in payload["titles"]]
            assert scores == sorted(scores, reverse=True), "results not ordered by score"
            scores_seen.extend(scores)

        # A healthy ranker separates candidates. Near-zero variance means the
        # model is effectively returning a constant.
        spread = statistics.pstdev(scores_seen)
        assert spread > 0.01, f"score variance {spread:.5f} suggests a degenerate model"

        return {"profiles_tested": 5, "score_stdev": round(spread, 4)}

    skip_promotion = EmptyOperator(task_id="skip_promotion")
    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    wait_for_features >> train >> evaluation >> branch
    branch >> promote_model >> rebuild_index >> smoke_test() >> finish
    branch >> skip_promotion >> finish
