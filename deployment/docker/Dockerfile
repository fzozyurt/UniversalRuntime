FROM registry.access.redhat.com/ubi9/python-312:latest AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/universal-runtime
COPY --chown=1001:0 pyproject.toml README.md ./
COPY --chown=1001:0 src ./src
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir '.[langgraph,fastapi,kafka,a2a,observability]'

COPY --chown=1001:0 deployment/docker/entrypoint.sh /usr/local/bin/runtime-entrypoint
RUN chmod +x /usr/local/bin/runtime-entrypoint

USER 1001
EXPOSE 8080 9090
ENTRYPOINT ["runtime-entrypoint"]
