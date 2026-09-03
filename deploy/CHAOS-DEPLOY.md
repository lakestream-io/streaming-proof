# Chaos Testing Deployment Guide

This guide installs Chaos Mesh and the optional recurring fault-injection
schedules in the Kubernetes cluster that runs the target streaming-system pods.

## 1. Install Chaos Mesh

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update chaos-mesh

helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --version 2.8.3 \
  -f deploy/helm/chaos-mesh-values.yaml
```

The supplied values configure the Chaos daemon for containerd and keep the
Dashboard private. Adapt the runtime settings if your nodes use a different
container runtime.

Verify the installation:

```bash
helm status chaos-mesh -n chaos-mesh
kubectl get pods -n chaos-mesh
```

To access the Dashboard locally:

```bash
kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333
```

Then open <http://localhost:2333>.

## 2. Configure experiments

All checked-in experiments are disabled and contain no cluster configuration.
Create a local values file using the example in
`deploy/helm/chaos-experiments/README.md`. Ensure the configured selector
labels match the labels on the target pods.

Render and inspect the manifests before installation:

```bash
helm template chaos ./deploy/helm/chaos-experiments \
  --namespace chaos-testing \
  -f /path/to/chaos-values.yaml
```

Install after reviewing the target namespace, selectors, cron expressions, and
blast radius:

```bash
helm upgrade --install chaos ./deploy/helm/chaos-experiments \
  --namespace chaos-testing --create-namespace \
  -f /path/to/chaos-values.yaml
```

## 3. Verify

```bash
kubectl get schedules.chaos-mesh.org -n <target-namespace>
kubectl get podchaos,networkchaos -n <target-namespace>
```

## Uninstall

```bash
helm uninstall chaos -n chaos-testing
helm uninstall chaos-mesh -n chaos-mesh
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Chaos Mesh pods cannot reach containers | Verify `chaosDaemon.runtime` and `chaosDaemon.socketPath` for the node runtime |
| Schedules exist but affect no pods | Verify `target.namespace`, `target.clusterLabel`, and `target.roleLabel` against the real pod labels |
| A schedule overlaps an earlier run | Use `concurrencyPolicy: Forbid` or increase the cron interval |
