#!/usr/bin/env sh
set -eu
docker compose -f deployment/compose/docker-compose.yml down
