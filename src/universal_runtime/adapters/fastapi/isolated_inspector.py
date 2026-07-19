from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from universal_runtime.adapters.fastapi.descriptor import AsgiApplicationDescriptor
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


def inspect_isolated(
    root: Path, *, timeout: float = 10.0, max_output: int = 64_000
) -> AsgiApplicationDescriptor:
    modules = sorted(path.relative_to(root).with_suffix("") for path in root.rglob("*.py"))
    candidates = tuple(
        f"{'.'.join(path.parts)}:{name}"
        for path in modules
        if path.name != "__init__"
        for name in ("app", "application", "api", "asgi", "create_app", "build_app")
    )
    if not candidates:
        raise RuntimeFailure(ErrorCode.CUSTOM_HTTP_UNAVAILABLE, "no isolated ASGI candidates found")
    code = (
        "import importlib,json,os,sys; sys.path.insert(0, os.environ['UR_APP_ROOT']); "
        "candidates=json.loads(os.environ['UR_CANDIDATES']); found=[]; "
        "\nfor p in candidates:\n try:\n  m,a=p.split(':',1); o=getattr(importlib.import_module(m),a); "
        "  o=o() if a in ('create_app','build_app') else o; found.append(p) if callable(o) else None\n except Exception: pass\n"
        "if len(found)!=1:\n raise SystemExit(2)\n"
        "print(json.dumps({'entrypoint':found[0],'framework':'asgi','object_kind':'application','routes':[],'has_lifespan':True,'docs_paths':[],'detection_method':'isolated_import','warnings':[]}))"
    )
    env = {
        key: os.environ[key]
        for key in ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP")
        if key in os.environ
    }
    env["UR_CANDIDATES"] = json.dumps(candidates)
    env["UR_APP_ROOT"] = str(root)
    try:
        result = subprocess.run(  # noqa: S603 - executable and arguments are fixed
            [sys.executable, "-c", code],
            cwd=str(root),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_TIMEOUT, "isolated ASGI inspection timed out"
        ) from exc
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_UNAVAILABLE,
            "isolated ASGI inspection failed",
            details={"returncode": result.returncode},
        )
    if len(result.stdout) > max_output:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_RESPONSE_TOO_LARGE, "inspection output exceeded limit"
        )
    try:
        return AsgiApplicationDescriptor(**json.loads(result.stdout))
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_UNAVAILABLE, "isolated inspector returned invalid JSON"
        ) from exc
