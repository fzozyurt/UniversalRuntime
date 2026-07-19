from services.worker.main import main as _service_main


def main() -> int:
    return _service_main(run_forever=True)


__all__ = ["main"]
