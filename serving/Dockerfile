# Multi-stage build. The final image carries the runtime and nothing else:
# no compiler, no build headers, no package index cache. That is roughly a
# 900 MB image reduced to 210 MB, which matters directly for pod startup time
# during a scale-out event.

# ── build stage ───────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# LightGBM and FAISS both need a compiler and OpenMP at build time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libgomp1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .

# Build wheels once here so the runtime stage installs without a toolchain.
RUN pip wheel --wheel-dir /wheels -r requirements.txt

# ── runtime stage ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# libgomp is the one build-time dependency that is also needed at runtime:
# both LightGBM and FAISS link against OpenMP.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 app \
 && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-index --find-links=/wheels -r requirements.txt \
 && rm -rf /wheels requirements.txt

WORKDIR /opt/service
COPY --chown=app:app app/ ./app/

# Non-root. A container that does not need root should never run as it, and
# most Kubernetes admission policies will reject one that does.
USER app

ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=45s --retries=3 \
    CMD curl -fsS http://localhost:8000/health/live || exit 1

# Uvicorn workers, not Gunicorn: the model is loaded per worker and each copy
# costs ~400 MB of resident memory, so horizontal scaling happens through pod
# replicas rather than through processes inside one pod.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--no-access-log", \
     "--timeout-keep-alive", "65"]
