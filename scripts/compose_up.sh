#!/usr/bin/env sh
set -eu
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD before starting Compose}"
docker compose -f deployment/compose/docker-compose.yml up --build -d
