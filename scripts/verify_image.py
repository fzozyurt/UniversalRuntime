from __future__ import annotations

import subprocess


def main() -> int:
    image = "universal-runtime:local"
    checks = [
        ["docker", "image", "inspect", image],
        ["docker", "run", "--rm", "--entrypoint", "universal-runtime", image, "info"],
    ]
    for command in checks:
        result = subprocess.run(command, check=False)  # noqa: S603
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
