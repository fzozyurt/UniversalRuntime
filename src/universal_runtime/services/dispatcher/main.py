from services.dispatcher.main import main as _service_main


def main(*, run_forever: bool = True) -> int:
    return _service_main(run_forever=run_forever)


__all__ = ["main"]
