from __future__ import annotations

import os
import subprocess
import sys
import urllib.request


def main() -> int:
    """Run the deterministic acceptance checks against a running Compose gateway."""
    base_url = os.environ.get("UR_E2E_BASE_URL", "http://127.0.0.1:8080")
    if not base_url.startswith(("http://", "https://")):
        return 2
    for path in ("/ok", "/ready"):
        with urllib.request.urlopen(base_url + path, timeout=5) as response:  # noqa: S310
            if response.status != 200:
                return 1
    command = [
        sys.executable,
        "-c",
        "import sys; sys.path.insert(0, 'examples/phase1-agent/src'); "
        "from phase1_agent.graph import graph; "
        "assert graph.invoke({'messages': []})['tool_result'] == 'weather:Istanbul:sunny'",
    ]
    return subprocess.run(command, check=False).returncode  # noqa: S603


if __name__ == "__main__":
    raise SystemExit(main())
