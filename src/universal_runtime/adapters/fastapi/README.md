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
