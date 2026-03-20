# Streaming Proof

Streaming Proof is a robust testing framework designed to verify ordering and delivery guarantees in distributed streaming systems like Apache Kafka, Apache Pulsar, and other messaging platforms. It offers a streamlined approach to validate the correctness of streaming systems through automated testing and monitoring. With Streaming Proof, you can easily verify that your streaming system meets its promised delivery guarantees. For a deeper understanding of the framework, please check the [design document](doc/The_framework_for_streaming_correctness_verification.md).

**Key Benefits**:
- Simple yet powerful correctness verification at scale
- Comprehensive test management through REST APIs
- Minimal external dependencies
- Flexible deployment options (Docker, Kubernetes)
- Zone-aware deployment capabilities

Here is the example to show the proof status:

```json
{
  "proof" : {
    "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
    "name" : "Ursa: at_least_once + ordering",
    "driver" : "ursa",
    "features" : [ "at_least_once", "ordering" ],
    "description" : null,
    "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
    "partitions" : 10,
    "producers" : 4,
    "consumers" : 4,
    "msgRate" : 1000,
    "keys" : 40,
    "checkPointInterval" : 5,
    "timeout" : 180,
    "duration" : 28800,
    "startTime" : 1738794638049
  },
  "summary" : {
    "verified" : 1765040,
    "errors" : 0,
    "outOfOrders" : 0,
    "missed" : 0,
    "duplicates" : 716,
    "timeouts" : 0
  },
  "checkpoints" : {...}
}
```

- **verified**: Total number of messages successfully verified
- **errors**: Number of failures during message publication
- **outOfOrders**: Number of messages received out of sequence
- **missed**: Number of messages not received for verification
- **duplicates**: Number of duplicate messages detected
- **timeouts**: Number of verification attempts that exceeded the configured timeout period

The checkpoints section provides detailed information about the verification status of each message key, allowing you to track the progress and health of your streaming system at a granular level.

**Supported Streaming Systems**:
- Apache Kafka, or any Kafka API compatible systems
- Apache Pulsar (Failover, Key_Shared, Shared subscriptions)

**Supported verifications**:
- At least once
- Ordering
- Shared subscription (Pulsar) — at-least-once delivery with duplicate counting across round-robin consumers

## Build
### Building from Source

- Java 21 or later
- Maven 3.6 or later

```bash
mvn clean install -DskipTests
```

### Building Docker Image

- Java 21 or later
- Maven 3.6 or later

```bash
mvn clean install -DskipTests -Pdocker
```
## Release

The workflow [Publish image to Docker hub](https://github.com/streamnative/streaming-proof/actions/workflows/publish.yaml) is triggered automatically after a PR merged to `main` branch, and the image is `streamnative/streaming-proof:latest`.

If you want to publish a custom tag image, you can run the above workflow manually with your branch name and tag name.

## Quick Start with Docker Compose

Follow these simple steps to get Streaming Proof up and running quickly with Docker Compose:

1. Start the services with docker-compose:

```bash
cd deploy/docker-compose/kafka
docker-compose up -d
```
This will start:

- Proof Coordinator service (port 8080)
- Proof Worker service (ports 8088)
- Kafka (port 9092)
- ZooKeeper (port 2181)

2. Verify the deployment:

```bash
docker-compose ps
```
3. Init configurations:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers" : {
    "worker.1" : "http://worker:8088"
  },
  "drivers" : {
    "ursa" : {
      "driverType" : "kafka",
      "driverConfigs" : {
        "acks" : "1",
        "bootstrap.servers" : "kafka:9092",
        "session.timeout.ms" : "45000"
      }
    }
  }
}'
```
4. Create first proof:

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name" : "Ursa: at_least_once + ordering",
  "driver" : "ursa",
  "features" : [ "at_least_once", "ordering" ],
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180
}'
```

### Pulsar Shared Subscription Verification

To verify at-least-once delivery with Pulsar Shared subscriptions, configure the `pulsar` field with `subscriptionType: "Shared"`:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers": {
    "worker.1": "http://worker:8088"
  },
  "drivers": {
    "pulsar_driver": {
      "driverType": "pulsar",
      "driverConfigs": {
        "pulsar.service.url": "pulsar://localhost:6650",
        "pulsar.admin.url": "http://localhost:8080"
      }
    }
  }
}'
```

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name": "Pulsar Shared subscription test",
  "driver": "pulsar_driver",
  "features": ["at_least_once"],
  "partitions": 1,
  "producers": 2,
  "consumers": 4,
  "msgRate": 500,
  "keys": 10,
  "checkPointInterval": 5,
  "timeout": 180,
  "pulsar": {
    "consumerConfig": {
      "subscriptionType": "Shared"
    }
  }
}'
```

With Shared subscriptions, messages are round-robin distributed across consumers — per-key ordering is not guaranteed. The framework uses **high-watermark-based verification** instead of last-sequence comparison: it computes the highest contiguous sequence per key across all consumers, and verifies that every produced message has been consumed. Out-of-order delivery is expected and not flagged.

5. Stop all the components:

```bash
docker-compose down
```

## Deployment

## Deploy with helm chart

```bash
cd streaming-proof/deploy/helm
helm install streaming-proof ./streaming-proof -n streaming-proof 
```

## Create proof

1. Create workers and drivers configuration:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers" : {
    "worker.1" : "http://worker.1:8088",
    "worker.2" : "http://worker.2:8088",
    "worker.3" : "http://worker.3:8088"
  },
  "drivers" : {
    "ursa" : {
      "driverType" : "kafka",
      "driverConfigs" : {
        "security.protocol" : "SASL_PLAINTEXT",
        "acks" : "1",
        "sasl.mechanism" : "PLAIN",
        "sasl.jaas.config" : "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='token:<your-token>';",
        "client.id" : "zone_id={zone.id}",
        "bootstrap.servers" : "kafka.0:9092,kafka.1:9092,kafka.12:9092",
        "session.timeout.ms" : "45000"
      }
    }
  }
}'
```

2. Create and start a proof:

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name" : "Ursa: at_least_once + ordering",
  "driver" : "ursa",
  "features" : [ "at_least_once", "ordering" ],
  "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180
}'
```

3. Get the list of proofs:

```bash
curl http://localhost:8080/proofs
```

output:

```json
[ {
  "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
  "name" : "Ursa: at_least_once + ordering",
  "driver" : "ursa",
  "features" : [ "at_least_once", "ordering" ],
  "description" : null,
  "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180,
  "duration" : 28800,
  "startTime" : 1738794638049
} ]
```

4. Get the proof status:

```bash
curl http://localhost:8080/proofs/137ba4d8-50ad-4c96-8c16-101b54083771
```

output:

```json
{
  "proof" : {
    "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
    "name" : "Ursa: at_least_once + ordering",
    "driver" : "ursa",
    "features" : [ "at_least_once", "ordering" ],
    "description" : null,
    "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
    "partitions" : 10,
    "producers" : 4,
    "consumers" : 4,
    "msgRate" : 1000,
    "keys" : 40,
    "checkPointInterval" : 5,
    "timeout" : 180,
    "duration" : 28800,
    "startTime" : 1738794638049
  },
  "summary" : {
    "verified" : 1765040,
    "errors" : 0,
    "outOfOrders" : 0,
    "missed" : 0,
    "duplicates" : 716,
    "timeouts" : 0
  },
  "checkpoints" : {...}
}
```

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

