from __future__ import annotations

import uvicorn

from universal_runtime.services.gateway.app import create_app


def main() -> int:
    uvicorn.run(create_app(), host="0.0.0.0", port=8080)  # noqa: S104
    return 0


app = create_app()


if __name__ == "__main__":
    raise SystemExit(main())
