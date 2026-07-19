# Dispatcher Service

Responsibilities:

- consume priority run topics,
- weighted fair scheduling and aging,
- acquire thread/run lease,
- select worker by application/revision/capability/capacity,
- send lease over gRPC,
- handle heartbeat, retry and dead letter.
