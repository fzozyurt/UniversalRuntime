FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/runtime
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --upgrade pip \
    && pip install '.[langgraph,fastapi,kafka,a2a,observability]'

COPY deployment/docker/entrypoint.sh /usr/local/bin/runtime-entrypoint
RUN chmod +x /usr/local/bin/runtime-entrypoint

USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["runtime-entrypoint"]
