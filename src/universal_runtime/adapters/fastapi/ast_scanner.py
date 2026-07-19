from __future__ import annotations

import ast
from pathlib import Path

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure

_IGNORED = {".git", ".venv", "venv", "node_modules", "__pycache__", "dist", "build"}


def discover_fastapi_entrypoints(
    root: Path, *, max_files: int = 1000, max_bytes: int = 1_000_000
) -> tuple[str, ...]:
    root = root.resolve()
    candidates: list[str] = []
    files = 0
    for path in root.rglob("*.py"):
        if any(part in _IGNORED for part in path.parts):
            continue
        if not path.resolve().is_relative_to(root):
            continue
        files += 1
        if files > max_files:
            break
        if path.stat().st_size > max_bytes:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            raise RuntimeFailure(
                ErrorCode.ASGI_SYNTAX_ERROR,
                "application source contains invalid Python syntax",
                details={"path": str(path), "line": exc.lineno},
            ) from exc
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            function = node.value.func
            function_name = function.id if isinstance(function, ast.Name) else None
            if function_name not in {"FastAPI", "Starlette"}:
                continue
            module = _module_name(root, path)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    candidates.append(f"{module}:{target.id}")
    return tuple(dict.fromkeys(candidates))


def _module_name(root: Path, path: Path) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)
