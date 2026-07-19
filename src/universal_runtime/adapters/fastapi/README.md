# FastAPI Adapter Checklist

- explicit entrypoint first,
- static AST discovery second,
- isolated import discovery third,
- no user import in Gateway,
- mount/proxy with correct `root_path`,
- propagate forwarded prefix/host/proto,
- propagate trace and internal execution identity,
- keep application migrations in the application schema,
- health/readiness endpoints must not collide with user routes.

## Detection and runtime boundaries

`detect_asgi_application()` accepts an explicit `module:attribute` entrypoint,
then performs a non-executing AST scan. When explicitly enabled, the final
inspection step runs candidate imports in a bounded subprocess. The Gateway
uses only the resulting JSON-safe descriptor and never imports application
modules.

The API process loads an entrypoint through `load_application()` and applies
request context and `root_path` middleware. Gateway custom HTTP traffic is
proxied under `/api/v1/applications/{application_id}/http/{path}` with bounded
request/response sizes and trusted forwarded headers.

Application migrations are executed by the application process under the
`application` advisory-lock category; checkpoint and platform schemas are not
used by this runner.
