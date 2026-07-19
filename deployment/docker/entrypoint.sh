#!/usr/bin/env sh
set -eu

MODE="${UR_MODE:-worker}"

if [ "${UR_OBSERVABILITY_ENABLED:-false}" = "true" ]; then
  exec opentelemetry-instrument runtime-launcher --mode "$MODE" "$@"
fi

exec runtime-launcher --mode "$MODE" "$@"
