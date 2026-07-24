# Kubernetes Deployment Contract

Phase 1 manifests contain:

- Gateway Deployment and Service for public HTTP and internal worker-control gRPC,
- Worker Deployment with application code and framework adapters,
- Application API Deployment and internal Service for custom FastAPI routes,
- platform migration Job,
- ConfigMap for non-secret resolved configuration,
- Secret references only; secret values never enter config revisions,
- ServiceAccount token automount disabled for Runtime and application pods,
- PodDisruptionBudget and NetworkPolicy,
- optional HPA/KEDA scaling.

There is no Dispatcher or Event Projector workload. Worker replicas share application Kafka consumer groups. Application migration ownership is elected through Gateway gRPC and PostgreSQL; the selected Worker executes Runtime-owned Alembic revisions before any replica consumes run topics.

Gateway has no permission to create pods. A future Operator or control plane may perform Kubernetes reconciliation.
