# Kubernetes Deployment Contract

Phase 1 manifests/Helm chart should generate:

- Gateway Deployment/Service
- Dispatcher Deployment
- Worker Deployment/Service for gRPC
- ConfigMap with non-secret resolved config
- Secret references only, never secret values in config revisions
- ServiceAccount with token automount disabled for application workers
- PodDisruptionBudget
- NetworkPolicy
- HPA optional; KEDA optional and not required
- migration Jobs for platform, framework-state and application migrations

Gateway has no permission to create pods. A future Operator/Control Plane performs Kubernetes reconciliation.
