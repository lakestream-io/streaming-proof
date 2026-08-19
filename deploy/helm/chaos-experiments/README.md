# chaos-experiments

Recurring Chaos Mesh experiments for managed Pulsar clusters. Every Schedule
targets pods with the `cloud.streamnative.io/pulsar-instance` and
`cloud.streamnative.io/role` labels.

This chart renders Chaos Mesh `Schedule` resources only. It does not install
Chaos Mesh.

## Fault types

| Values key | Chaos Mesh action | Default | Blast radius |
|---|---|---:|---|
| `podKill` | `PodChaos` / `pod-kill` | Enabled | One pod |
| `podFailure` | `PodChaos` / `pod-failure` | Disabled | One pod |
| `networkDelay` | `NetworkChaos` / `delay` | Disabled | One pod |
| `networkPartition` | `NetworkChaos` / `partition` | Disabled | One pod |

Pod-kill gives the target pod 60 seconds to terminate gracefully and keeps the
Chaos object active for 90 seconds. The duration-based faults use
`concurrencyPolicy: Forbid` so the same Schedule cannot overlap itself.

The disabled fault schedules use minute offsets `:22`, `:34`, and `:46`. These
fit between the existing pod-kill slots for the checked-in staging instances.
Review the complete schedule whenever cron expressions or durations change.

## Install Chaos Mesh

Chaos Mesh must run on the data-plane cluster where the Pulsar pods run.

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock
```

## Render and apply

With a direct data-plane kubeconfig:

```bash
helm upgrade --install chaos ./deploy/helm/chaos-experiments \
  --kube-context <data-plane-context> \
  -n o-2h056
```

Through `apiserver-admin`:

```bash
helm template chaos ./deploy/helm/chaos-experiments \
  --namespace o-2h056 \
  | apiserver-admin pools connect aws-use1-staging-q3p9f \
      -n o-2h056 \
      --context gke_sncloud-staging_us-west1-a_api \
      --org o-2h056 \
      -q \
      -- kubectl apply -f -
```

## Enable duration-based faults

All disruptive additions are disabled by default. Enable them in
`values.yaml`, or render a one-off configuration with Helm overrides:

```bash
helm template chaos ./deploy/helm/chaos-experiments \
  --namespace o-2h056 \
  --set podFailure.enabled=true \
  --set networkDelay.enabled=true \
  --set networkPartition.enabled=true
```

`networkPartition.externalTargets: []` fully isolates the selected pod. Set
explicit CIDRs when the test should sever only a remote peer or network.

## Verify

```bash
kubectl get schedules.chaos-mesh.org -n o-2h056
kubectl get podchaos,networkchaos -n o-2h056
```

Every Schedule retains up to `defaults.historyLimit` completed child objects.
Objects created before pod-kill gained a duration remain active and require a
one-time manual cleanup.
