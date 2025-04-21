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
## Release

The workflow [Publish image to Docker hub](https://github.com/streamnative/streaming-proof/actions/workflows/publish.yaml) is triggered automatically after a PR merged to `main` branch, and the image is `streamnative/streaming-proof:latest`.

If you want to publish a custom tag image, you can run the above workflow manually with your branch name and tag name.

## Try with docker-compose

1. Start docker compose:

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

5. Stop all the components:

```bash
docker-compose down
```

## Deployment

### Deploy with Kubernetes

1. Create configmap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: streaming-proof-configs
  namespace: streaming-proof
data:
  serverless.json: |
    {
      "workers": {
        "worker.1": "http://streaming-proof-worker-0.streaming-proof-worker-headless.streaming-proof.svc.cluster.local:8088",
        "worker.2": "http://streaming-proof-worker-1.streaming-proof-worker-headless.streaming-proof.svc.cluster.local:8088",
        "worker.3": "http://streaming-proof-worker-2.streaming-proof-worker-headless.streaming-proof.svc.cluster.local:8088"
      },
      "drivers": {
      }

2. Create a NodePool for streaming-proof which disable auto-scaling and uses non-spot instance:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: non-spot
spec:
  disruption:
    budgets:
    - nodes: "1"
    consolidateAfter: 1h
    consolidationPolicy: WhenEmptyOrUnderutilized
  template:
    spec:
      taints:
      - key: nodepool
        value: non-spot
        effect: NoSchedule
      expireAfter: Never
      nodeClassRef:
        group: karpenter.k8s.aws
        kind: EC2NodeClass
        name: default
      requirements:
      - key: kubernetes.io/os
        operator: In
        values:
        - linux
      - key: kubernetes.io/arch
        operator: In
        values:
        - amd64
      - key: karpenter.k8s.aws/instance-hypervisor
        operator: In
        values:
        - nitro
      - key: karpenter.sh/capacity-type
        operator: In
        values:
        - on-demand
      - key: karpenter.k8s.aws/instance-category
        operator: In
        values:
        - c
        - m
        - r
      - key: karpenter.k8s.aws/instance-generation
        operator: Gt
        values:
        - "5"
```

3. Create a service to expose the headless service of workers:

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

4. Create a sts for coordinator:

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
      volumes:
      - name: config-volume
        configMap:
          name: streaming-proof-configs
      nodeSelector:
        karpenter.sh/capacity-type: on-demand
      tolerations:
      - key: nodepool
        value: non-spot
        effect: NoSchedule
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
        volumeMounts:
        - mountPath: /mnt/streaming-proof/configs
          name: config-volume
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

5. Create a sts for workers:

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
      nodeSelector:
        karpenter.sh/capacity-type: on-demand
      tolerations:
      - key: nodepool
        value: non-spot
        effect: NoSchedule
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

6. Verify the deployment:

```
kubectl get pods | grep "streaming-proof"
```

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

