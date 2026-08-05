# Training image. Runs as a Kubernetes pod from the retraining DAG.
#
# Separate from the serving image on purpose: training needs pandas, scikit-learn,
# scipy and MLflow, none of which belong in a latency-sensitive API container
# where they would add ~300 MB and a larger attack surface for no benefit.

FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gfortran libopenblas-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # BLAS threading is left to the job rather than the library. Without this,
    # OpenBLAS spawns a thread per core and fights LightGBM's own thread pool,
    # which makes training slower on larger pods than on smaller ones.
    OMP_NUM_THREADS=8 \
    OPENBLAS_NUM_THREADS=8

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 libopenblas0 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10002 ml \
 && useradd --uid 10002 --gid ml --create-home ml

COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
 && rm -rf /wheels requirements.txt

WORKDIR /opt/ml
COPY --chown=ml:ml training/ ./training/
COPY --chown=ml:ml embeddings/ ./embeddings/

# Artefacts are written here and uploaded to S3; the directory must be writable
# by the non-root user.
RUN mkdir -p /artifacts && chown ml:ml /artifacts

USER ml
ENV PYTHONPATH=/opt/ml

# No CMD. The DAG supplies the module and arguments per task, which keeps one
# image serving both training and index building.
