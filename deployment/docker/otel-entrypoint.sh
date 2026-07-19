#!/usr/bin/env bash
set -euo pipefail

# Optional zero-code launcher for images that include the OTel instrumentor.
# Exporter configuration is read by the OTel SDK; secrets are never printed.
MODE="${UR_MODE:-worker}"
case "$MODE" in
  validate|inspect|migrate|all|gateway|dispatcher|worker|projector|api|standalone) ;;
  *) printf '%s\n' "unsupported UR_MODE: $MODE" >&2; exit 64 ;;
esac

cmd=(universal-runtime "$MODE" "$@")
if [[ "${UR_OBSERVABILITY_ENABLED:-false}" == "true" ]]; then
  if ! command -v opentelemetry-instrument >/dev/null 2>&1; then
    printf '%s\n' "OTel enabled but opentelemetry-instrument is not installed" >&2
    exit 78
  fi
  exec opentelemetry-instrument "${cmd[@]}"
fi

exec "${cmd[@]}"
