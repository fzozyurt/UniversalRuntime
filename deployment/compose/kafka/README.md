# Kafka bootstrap

The broker is configured with automatic topic creation disabled. Topic creation
belongs to the Kafka composition root and must use the configured `UR_TOPIC_PREFIX`
and `UR_KAFKA_ENVIRONMENT`; the default topic contract is documented in
`contracts/kafka/TOPIC_CONTRACT.md`.
