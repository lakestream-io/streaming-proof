# chaos-experiments

Recurring [Chaos Mesh](https://chaos-mesh.org/) experiments for streaming-system
clusters. This chart renders `Schedule` resources only; it does not install
Chaos Mesh.

All experiments are disabled by default. Targets are selected with two
configurable Kubernetes label keys:

- `target.clusterLabel`, defaulting to `app.kubernetes.io/instance`
- `target.roleLabel`, defaulting to `app.kubernetes.io/component`

## Fault types

| Values key | Chaos Mesh action | Default | Blast radius |
|---|---|---:|---|
| `podKill` | `PodChaos` / `pod-kill` | Disabled | One pod |
| `podFailure` | `PodChaos` / `pod-failure` | Disabled | One pod |
| `networkDelay` | `NetworkChaos` / `delay` | Disabled | One pod |
| `networkPartition` | `NetworkChaos` / `partition` | Disabled | One pod |

## Install Chaos Mesh

Chaos Mesh must run in the cluster containing the target pods.

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace
```

## Configure targets

Create a separate values file rather than committing environment-specific
cluster information:

```yaml
target:
  namespace: messaging
  clusterLabel: app.kubernetes.io/instance
  roleLabel: app.kubernetes.io/component

podKill:
  enabled: true
  clusters:
    - name: example-cluster
      enabled: true
      roles:
        broker: "0 */6 * * *"
```

This example selects pods in the `messaging` namespace with both
`app.kubernetes.io/instance=example-cluster` and
`app.kubernetes.io/component=broker`.

## Render and apply

```bash
helm template chaos ./deploy/helm/chaos-experiments \
  --namespace chaos-testing \
  -f /path/to/chaos-values.yaml

helm upgrade --install chaos ./deploy/helm/chaos-experiments \
  --namespace chaos-testing --create-namespace \
  -f /path/to/chaos-values.yaml
```

Review the rendered selectors and schedules before applying them. For network
partitions, an empty `networkPartition.externalTargets` fully isolates the
selected pod; provide explicit CIDRs to limit the partition.

## Verify

```bash
kubectl get schedules.chaos-mesh.org -n messaging
kubectl get podchaos,networkchaos -n messaging
```
