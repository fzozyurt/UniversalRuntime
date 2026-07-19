# Runtime Protocol v1

Compile with Buf or protoc. Import paths assume `contracts/proto` is the proto root.

The standard gRPC health protocol is used separately:

```text
grpc.health.v1.Health/Check
grpc.health.v1.Health/Watch
```

Compatibility rules:

- additive fields only,
- removed fields must be reserved,
- arbitrary JSON payload uses `google.protobuf.Value`,
- object-only config/context/metadata uses `google.protobuf.Struct`,
- run event sequence is monotonically increasing per run.
