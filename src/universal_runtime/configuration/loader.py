from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator

from universal_runtime.configuration.interpolation import interpolate_environment, redact_secrets


class RuntimeConfigLoader:
    def __init__(self, schema_path: Path) -> None:
        self._schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self._schema)

    def load(self, path: Path, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        resolved = interpolate_environment(document, environ)
        Draft202012Validator(self._schema).validate(resolved)
        return cast(dict[str, Any], resolved)

    @staticmethod
    def redact(config: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], redact_secrets(config))
