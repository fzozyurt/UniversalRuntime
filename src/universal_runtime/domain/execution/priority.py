from enum import IntEnum


class QueuePriority(IntEnum):
    BATCH = 10
    NORMAL = 50
    INTERACTIVE = 100
