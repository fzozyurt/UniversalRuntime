from __future__ import annotations

import os
from typing import Any


def kafka_sasl_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    protocol = os.environ.get("UR_KAFKA_SECURITY_PROTOCOL", "").strip()
    if not protocol:
        return kwargs
    kwargs["security_protocol"] = protocol
    mechanism = os.environ.get("UR_KAFKA_SASL_MECHANISM", "").strip()
    if mechanism:
        kwargs["sasl_mechanism"] = mechanism
    username = os.environ.get("UR_KAFKA_SASL_PLAIN_USERNAME", "").strip()
    if username:
        kwargs["sasl_plain_username"] = username
    password = os.environ.get("UR_KAFKA_SASL_PLAIN_PASSWORD", "").strip()
    if password:
        kwargs["sasl_plain_password"] = password
    return kwargs
