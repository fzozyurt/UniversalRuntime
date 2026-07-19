from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = ROOT / "src/universal_runtime/domain"
FORBIDDEN_DOMAIN_PREFIXES = (
    "fastapi",
    "sqlalchemy",
    "langgraph",
    "langchain",
    "aiokafka",
    "grpc",
    "opentelemetry",
)
FORBIDDEN_NAMES = {"utils", "helpers", "misc"}
IGNORED_PARTS = {".git", ".venv", "node_modules", "build", "dist", "__pycache__"}


def imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def main() -> None:
    errors: list[str] = []
    for directory in ROOT.rglob("*"):
        if IGNORED_PARTS.intersection(directory.parts):
            continue
        if directory.is_dir() and directory.name in FORBIDDEN_NAMES:
            errors.append(f"forbidden generic directory: {directory.relative_to(ROOT)}")
    for path in DOMAIN.rglob("*.py"):
        for module in imports(path):
            if module.startswith(FORBIDDEN_DOMAIN_PREFIXES):
                errors.append(f"domain import violation: {path.relative_to(ROOT)} -> {module}")
    if errors:
        raise SystemExit("\n".join(errors))
    print("architecture: valid")


if __name__ == "__main__":
    main()
