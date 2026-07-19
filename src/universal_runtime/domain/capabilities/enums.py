from enum import StrEnum


class RuntimeProfile(StrEnum):
    LANGGRAPH = "langgraph"
    LANGCHAIN_AGENT = "langchain-agent"
    DEEPAGENTS = "deepagents"


class StreamMode(StrEnum):
    VALUES = "values"
    UPDATES = "updates"
    MESSAGES = "messages"
    MESSAGES_TUPLE = "messages-tuple"
    CUSTOM = "custom"
    EVENTS = "events"
    DEBUG = "debug"
    CHECKPOINTS = "checkpoints"
    TASKS = "tasks"


class SessionAffinity(StrEnum):
    NONE = "none"
    PREFERRED = "preferred"
    REQUIRED = "required"
