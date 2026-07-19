# OpenAPI Contract Notes

`universal-runtime-phase1.yaml` is the project-owned contract seed. During implementation:

1. compare every compatibility operation with the installed official `langgraph_sdk`,
2. add missing request/response fields without changing upstream behavior,
3. keep native management endpoints under `/api/v1`,
4. generate a compatibility report from tests rather than claiming unsupported endpoints.

Do not copy proprietary LangGraph server implementation code. Implement from public contracts, SDK behavior and clean-room tests.
