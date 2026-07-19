# UniversalRuntime containers

`Dockerfile` builds the non-user-code platform image. `Dockerfile.agent-base`
is the base for an application image that owns graph and FastAPI imports.

The Python, PostgreSQL and Kafka images used by the checked-in Compose topology
are tag-plus-digest pinned. No secret is accepted as a build argument.
