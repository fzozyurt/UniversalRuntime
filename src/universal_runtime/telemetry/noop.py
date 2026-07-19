from __future__ import annotations

from typing import Any


class NoopSpan:
    def __enter__(self) -> NoopSpan:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_attribute(self, _key: str, _value: Any) -> None:
        return None

    def set_status(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def record_exception(self, _exception: BaseException, **_kwargs: Any) -> None:
        return None

    def add_event(self, _name: str, **_kwargs: Any) -> None:
        return None


class NoopTracer:
    def start_as_current_span(self, _name: str, **_kwargs: Any) -> NoopSpan:
        return NoopSpan()


class NoopMeter:
    def create_counter(self, *_args: Any, **_kwargs: Any) -> NoopInstrument:
        return NoopInstrument()

    def create_histogram(self, *_args: Any, **_kwargs: Any) -> NoopInstrument:
        return NoopInstrument()

    def create_up_down_counter(self, *_args: Any, **_kwargs: Any) -> NoopInstrument:
        return NoopInstrument()


class NoopInstrument:
    def add(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def record(self, *_args: Any, **_kwargs: Any) -> None:
        return None
