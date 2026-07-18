# Flink job image. Built on the official Flink base so the runtime, the
# classpath and the entrypoint match what the Kubernetes operator expects.
FROM flink:1.18.1-scala_2.12-java11

ARG PYTHON_VERSION=3.10

# PyFlink needs a real Python plus the JVM bridge. Installing from the distro
# rather than building from source keeps the image build under a minute.
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
      python${PYTHON_VERSION} python3-pip python3-dev build-essential \
 && ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Connectors are pulled at build time, not at submit time: a job that downloads
# its own dependencies on startup is a job that fails during an incident when
# the artifact repository is unreachable.
ENV FLINK_HOME=/opt/flink
RUN mkdir -p ${FLINK_HOME}/lib \
 && cd ${FLINK_HOME}/lib \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/1.18.1/flink-statebackend-rocksdb-1.18.1.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY jobs/ /opt/streaming/jobs/

# Metrics reporter so the jobs show up in the same Prometheus the API uses.
RUN echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> ${FLINK_HOME}/conf/flink-conf.yaml \
 && echo "metrics.reporter.prom.port: 9249" >> ${FLINK_HOME}/conf/flink-conf.yaml \
 && cp ${FLINK_HOME}/opt/flink-metrics-prometheus-1.18.1.jar ${FLINK_HOME}/lib/

WORKDIR /opt/streaming

# Run as the unprivileged flink user that the base image already creates.
USER flink

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8081/ || exit 1
