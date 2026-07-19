from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ApplicationInspector(Protocol):
    def inspect(self, root: Path) -> Any: ...
