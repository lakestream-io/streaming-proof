# Streaming Proof

Streaming Proof is a robust testing framework designed to verify ordering and delivery guarantees in distributed streaming systems like Apache Kafka, Apache Pulsar, and other messaging platforms. It offers a streamlined approach to validate the correctness of streaming systems through automated testing and monitoring.

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

And the checkpoints section will also show you the details of each message key verification.

**Supported Streaming Systems**:
- Apache Kafka, or any Kafka API compatible systems

**Supported verifications**:
- At least once
- Ordering

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

## Deployment

### Deploy with Docker Compose

1. Start docker compose:

```bash
cd docker
docker-compose up -d
```
This will start:

- Proof Coordinator service (port 8080)
- Proof Worker service (ports 80889)
- Kafka (port 9092)
- ZooKeeper (port 2181)

2. Verify the deployment:

```bash
docker-compose ps
```

### Deploy with Kubernetes

1. Create a service to expose the headless service of workers:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: streaming-proof-worker-headless
  namespace: <namespace>
spec:
  clusterIP: None  # Headless service
  selector:
    app: streaming-proof
  ports:
    - name: http
      port: 8088
      targetPort: 8088
```

```bash
kubectl apply -f <service.yaml>
```

2. Create a sts for coordinator:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: streaming-proof-coordinator
  namespace: <namespace>
spec:
  replicas: 1
  selector:
    matchLabels:
      app: streaming-proof
      component: streaming-proof-coordinator
  template:
    metadata:
      labels:
        app: streaming-proof
        component: streaming-proof-coordinator
    spec:
      containers:
      - args:
        - |
          apt-get update && apt-get install curl jq -y
          export TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600");
          export ZONE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone-id);
          export HEAP_OPTS="-Xms4G -Xmx4G -Dzone.id=${ZONE_ID}"
          bin/coordinator
        command:
        - sh
        - -c
        image: streamnative/streaming-proof:latest
        imagePullPolicy: Always
        name: streaming-proof-coordinator
        ports:
        - containerPort: 8080
          protocol: TCP
        resources:
          limits:
            cpu: "2"
            memory: 8Gi
          requests:
            cpu: "2"
            memory: 8Gi
        terminationMessagePath: /dev/termination-log
        terminationMessagePolicy: File
      dnsPolicy: ClusterFirst
      restartPolicy: Always
      schedulerName: default-scheduler
      securityContext: {}
```

```bash
kubectl apply -f <coordinator.yaml>
```

3. Create a sts for workers:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: streaming-proof-worker
  namespace: <namespace>
spec:
  replicas: 3
  selector:
    matchLabels:
      app: streaming-proof
      component: streaming-proof-worker
  serviceName: streaming-proof-worker-headless
  template:
    metadata:
      labels:
        app: streaming-proof
        component: streaming-proof-worker
    spec:
      affinity:
        podAffinity: {}
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: streaming-proof
                  component: streaming-proof-worker
              topologyKey: failure-domain.beta.kubernetes.io/zone
    spec:
      containers:
      - args:
        - |
          apt-get update && apt-get install curl jq -y
          export TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600");
          export ZONE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone-id);
          export HEAP_OPTS="-Xms4G -Xmx4G -Dzone.id=${ZONE_ID} -Dnetworkaddress.cache.ttl=10 -Dnetworkaddress.cache.negative.ttl=0 -Dio.netty.resolver.dns.cacheMaxTimeToLive=10 -Dio.netty.resolver.dns.cacheMinTimeToLive=10"
          bin/worker
        command:
        - sh
        - -c
        image: streamnative/streaming-proof:latest
        imagePullPolicy: Always
        name: streaming-proof-worker
        ports:
        - containerPort: 8088
          protocol: TCP
        resources:
          limits:
            cpu: "2"
            memory: 8Gi
          requests:
            cpu: "2"
            memory: 8Gi
        terminationMessagePath: /dev/termination-log
        terminationMessagePolicy: File
      dnsPolicy: ClusterFirst
      restartPolicy: Always
      schedulerName: default-scheduler
      securityContext: {}
```

```bash
kubectl apply -f <worker.yaml>
```

4. Verify the deployment:

```
kubectl get pods | grep "streaming-proof"
```

## Create first proof

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

