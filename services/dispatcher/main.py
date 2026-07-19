from __future__ import annotations

from universal_runtime.adapters.kafka import KafkaRunCommandQueue, TopicNames


def create_dispatch_queue() -> KafkaRunCommandQueue:
    return KafkaRunCommandQueue()


def main() -> int:
    TopicNames.from_config()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
