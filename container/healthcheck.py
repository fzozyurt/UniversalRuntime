from __future__ import annotations

import os
import urllib.request


def main() -> int:
    port = os.environ.get("UR_GATEWAY_PORT", os.environ.get("UR_HTTP_PORT", "8080"))
    path = "/ready" if os.environ.get("UR_MODE") in {"gateway", "api", "standalone"} else "/ok"
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as response:
            return 0 if response.status == 200 else 1
    except (OSError, ValueError):
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
