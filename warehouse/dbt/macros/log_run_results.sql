{% macro log_run_results(results) %}
  {#
    Persists every model's execution outcome to an audit table on run end.

    Why this exists: dbt Cloud shows you the last run, and the console shows you
    the current one. Neither answers "which model has been getting slower for
    three weeks", which is the question you actually need when the nightly build
    starts overrunning its window. One small table makes that a SQL query.
  #}

  {% if execute and results | length > 0 %}

    {% set audit_table = target.schema ~ '_meta.dbt_run_results' %}

    {% set create_sql %}
      create table if not exists {{ audit_table }} (
        run_started_at    timestamp,
        invocation_id     varchar,
        target_name       varchar,
        node_id           varchar,
        node_type         varchar,
        materialization   varchar,
        status            varchar,
        execution_seconds double,
        rows_affected     bigint,
        error_message     varchar
      )
    {% endset %}
    {% do run_query(create_sql) %}

    {% set rows = [] %}
    {% for res in results %}
      {% set node = res.node %}
      {% set error = (res.message | replace("'", "''"))[:1000] if res.status == 'error' else none %}
      {% do rows.append(
          "('" ~ run_started_at ~ "','" ~ invocation_id ~ "','" ~ target.name ~ "','"
          ~ node.unique_id ~ "','" ~ node.resource_type ~ "','"
          ~ (node.config.materialized or 'n/a') ~ "','" ~ res.status ~ "',"
          ~ (res.execution_time | round(3)) ~ ","
          ~ (res.adapter_response.get('rows_affected', 0) or 0) ~ ","
          ~ ("'" ~ error ~ "'" if error else 'null')
          ~ ")"
      ) %}
    {% endfor %}

    {% do run_query("insert into " ~ audit_table ~ " values " ~ (rows | join(","))) %}
    {{ log("Logged " ~ results | length ~ " node results to " ~ audit_table, info=true) }}

  {% endif %}
{% endmacro %}
