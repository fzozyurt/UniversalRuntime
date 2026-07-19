# syntax=docker/dockerfile:1.7
ARG PYTHON_IMAGE=python:3.12-slim@sha256:d764629ce0ddd8c71fd371e9901efb324a95789d2315a47db7e4d27e78f1b0e9

FROM ${PYTHON_IMAGE} AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir --upgrade pip build \
    && python -m build --wheel --outdir /dist

FROM ${PYTHON_IMAGE} AS runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
ARG SOURCE=https://example.invalid/universal-runtime
ARG CREATED=unknown
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 \
    UR_CONTRACT_SCHEMA_PATH=/opt/universal-runtime/contracts/config/runtime-application.schema.json
WORKDIR /opt/universal-runtime
RUN addgroup --system --gid 65532 runtime \
    && adduser --system --uid 65532 --gid 65532 --no-create-home runtime \
    && mkdir -p /opt/universal-runtime /tmp/runtime \
    && chown -R 65532:65532 /opt/universal-runtime /tmp/runtime
COPY --from=build /dist/*.whl /tmp/
RUN wheel="$(find /tmp -maxdepth 1 -name 'universal_runtime-*.whl' -print -quit)" \
    && test -n "$wheel" \
    && python -m pip install --no-cache-dir "${wheel}[langgraph,fastapi,kafka]" \
    && rm -f /tmp/*.whl
COPY runtime.example.yaml /opt/universal-runtime/runtime.yaml
COPY contracts /opt/universal-runtime/contracts
COPY migrations /opt/universal-runtime/migrations
COPY alembic.ini /opt/universal-runtime/alembic.ini
COPY examples/phase1-agent/src /opt/universal-runtime/applications/phase1-agent
COPY deployment/docker/entrypoint.sh /usr/local/bin/runtime-entrypoint
COPY container/healthcheck.py /usr/local/bin/runtime-healthcheck.py
RUN chmod 0555 /usr/local/bin/runtime-entrypoint /usr/local/bin/runtime-healthcheck.py
LABEL org.opencontainers.image.title="UniversalRuntime" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="$SOURCE" \
      org.opencontainers.image.created="$CREATED"
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/runtime-entrypoint"]
