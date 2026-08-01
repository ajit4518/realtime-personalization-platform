"""Nightly warehouse build.

Structure of the DAG and why:

    wait_for_landing -> dbt snapshot -> dbt build (staging..marts)
                                            |
                                            +-> freshness + parity checks
                                            +-> publish offline features to Redis

Snapshots run before models, always. A snapshot captures state as of the moment
it runs; if a model that depends on subscription history runs first, it reads a
snapshot that is a day stale and the point-in-time features are silently wrong
for the most recent day.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DBT_DIR = "/opt/airflow/dbt"
DBT_RUN = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    # Alert on failure, but do not page for a retry that succeeded.
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="warehouse_elt",
    description="Nightly dbt build over CDC and event data",
    default_args=default_args,
    # 02:15 UTC: after the hourly S3 sink has flushed the 01:00 partition and
    # before the European business day starts reading dashboards.
    schedule="15 2 * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    # One build at a time. Two concurrent dbt runs against the same incremental
    # models will interleave merges and corrupt the high-water marks.
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=3),
    tags=["warehouse", "dbt", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Confirm the landing zone has stopped changing before reading it. Without
    # this, dbt occasionally reads a partition mid-write and produces a build
    # that is short by a few thousand events with no error anywhere.
    wait_for_landing = S3KeysUnchangedSensor(
        task_id="wait_for_event_landing",
        bucket_name="{{ var.value.lake_bucket }}",
        prefix="playback.events/dt={{ ds }}/",
        inactivity_period=300,
        min_objects=1,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",  # frees the worker slot while waiting
    )

    # Snapshots first. See the module docstring.
    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_RUN} snapshot --target prod",
    )

    with TaskGroup("build") as build:
        # `dbt build` interleaves tests with models, so a model whose test fails
        # blocks its children instead of letting bad data propagate downstream.
        staging = BashOperator(
            task_id="staging",
            bash_command=f"{DBT_RUN} build --target prod --select staging.*",
        )
        marts = BashOperator(
            task_id="marts",
            bash_command=f"{DBT_RUN} build --target prod --select marts.*+",
        )
        features = BashOperator(
            task_id="features",
            bash_command=f"{DBT_RUN} build --target prod --select features.*",
        )
        staging >> marts >> features

    with TaskGroup("quality") as quality:
        freshness = BashOperator(
            task_id="source_freshness",
            bash_command=f"{DBT_RUN} source freshness --target prod",
            # Freshness is informational at this point in the run; the build
            # already succeeded, and a stale reference table should not fail it.
            trigger_rule=TriggerRule.ALL_DONE,
        )
        parity = BashOperator(
            task_id="online_offline_parity",
            bash_command=f"{DBT_RUN} test --target prod --select tag:parity",
        )

    @task(task_id="publish_offline_features")
    def publish_offline_features(ds: str) -> dict:
        """Load the day's features into Redis as the serving fallback.

        Written in one pipelined pass with a TTL slightly longer than a day, so
        a failed run degrades to yesterday's features rather than to nothing.
        """
        import json

        import redis
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse")
        client = redis.from_url(
            "{{ var.value.redis_url }}", decode_responses=True, socket_timeout=5
        )

        rows = hook.get_records(
            """
            select profile_id, sessions_7d, sessions_30d, avg_completion_7d,
                   completion_rate_30d, days_since_last_session, tenure_days,
                   engagement_segment, tier, genre_affinity
            from features.fct_profile_features_daily
            where feature_date = %s
            """,
            parameters=(ds,),
        )

        written = 0
        pipe = client.pipeline(transaction=False)
        for i, row in enumerate(rows, 1):
            payload = {
                "sessions_7d": row[1],
                "sessions_30d": row[2],
                "avg_completion_7d": float(row[3] or 0),
                "completion_rate_30d": float(row[4] or 0),
                "days_since_last_session": row[5],
                "tenure_days": row[6],
                "engagement_segment": row[7],
                "tier": row[8],
                "genre_affinity": row[9] or {},
            }
            pipe.set(f"feat:daily:{row[0]}", json.dumps(payload), ex=60 * 60 * 30)
            if i % 5000 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)
            written = i
        pipe.execute()

        return {"profiles_published": written, "feature_date": ds}

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> wait_for_landing >> snapshot >> build >> quality >> publish_offline_features() >> finish
