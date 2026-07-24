#!/usr/bin/env sh
set -eu

MODE="${UR_MODE:-worker}"
case "$MODE" in
  validate|inspect|migrate|gateway|worker|api|standalone|all) ;;
  *) echo "unsupported UR_MODE: $MODE" >&2; exit 64 ;;
esac

set -- universal-runtime "$MODE" "$@"

if [ "${UR_OBSERVABILITY_ENABLED:-false}" = "true" ] && command -v opentelemetry-instrument >/dev/null 2>&1; then
  export OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES:-}"
  OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES}service.name=${UR_APPLICATION_ID:-universal-runtime}-${MODE}"
  _delim=","
  if [ -n "${POD_NAME:-}" ]; then
    OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES}${_delim}pod.name=${POD_NAME}"
  elif [ -n "${HOSTNAME:-}" ]; then
    OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES}${_delim}pod.name=${HOSTNAME}"
  fi
  if [ -n "${NODE_NAME:-}" ]; then
    OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES}${_delim}node.name=${NODE_NAME}"
  fi
  if [ -n "${ARTIFACT_VERSION:-}" ]; then
    OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES}${_delim}service.version=${ARTIFACT_VERSION}"
  fi
  export OTEL_RESOURCE_ATTRIBUTES
  set -- opentelemetry-instrument "$@"
fi

exec "$@"
