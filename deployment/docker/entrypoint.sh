#!/usr/bin/env sh
set -eu

MODE="${UR_MODE:-worker}"
case "$MODE" in
  validate|inspect|migrate|gateway|dispatcher|worker|projector|api|standalone) ;;
  *) echo "unsupported UR_MODE: $MODE" >&2; exit 64 ;;
esac

set -- universal-runtime "$MODE" "$@"

if [ "${UR_OBSERVABILITY_ENABLED:-false}" = "true" ] && command -v opentelemetry-instrument >/dev/null 2>&1; then
  set -- opentelemetry-instrument "$@"
fi

exec "$@"
