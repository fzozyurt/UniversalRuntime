from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True, slots=True)
class SchemaNames:
    prefix: str = "rt"

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.prefix):
            raise ValueError("schema prefix must be a lowercase SQL identifier")

    @property
    def core(self) -> str:
        return f"{self.prefix}_core"

    @property
    def execution(self) -> str:
        return f"{self.prefix}_exec"

    def framework_state(self, workspace_key: str, application_key: str, environment: str) -> str:
        return self._application_schema("s", workspace_key, application_key, environment)

    def application(self, workspace_key: str, application_key: str, environment: str) -> str:
        return self._application_schema("a", workspace_key, application_key, environment)

    def _application_schema(
        self, kind: str, workspace_key: str, application_key: str, environment: str
    ) -> str:
        values = (workspace_key, application_key, environment)
        if not all(_IDENTIFIER.fullmatch(value) for value in values):
            raise ValueError("schema keys must be lowercase SQL identifiers")
        return f"{self.prefix}_{kind}_{workspace_key}_{application_key}_{environment}"


DEFAULT_SCHEMAS = SchemaNames()
