from __future__ import annotations

import os
from collections.abc import Mapping


def resource_attributes(
    *,
    service_name: str,
    service_version: str,
    component: str = "runtime",
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    attributes = {
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment.name": os.getenv("UR_ENVIRONMENT", "local"),
        "runtime.component": component,
    }
    pod = os.getenv("K8S_POD_NAME") or os.getenv("POD_NAME")
    if pod:
        attributes["k8s.pod.name"] = pod
    if extra:
        attributes.update({str(key): str(value) for key, value in extra.items()})
    return attributes
