# Chaos Testing Deployment Guide

Steps to deploy Chaos Mesh and recurring fault-injection experiments on a **data plane**
cluster (the pool member where the Pulsar pods run, e.g.
`aws-use1-staging-q3p9f`). Run every command from a terminal with a direct
kubectl context to the data plane (the same context you use for `helm upgrade`).

> Chaos Mesh must be installed on the **data plane** (the pool member running the
> Pulsar pods), not the control plane.

---

## Step 1: Install Chaos Mesh

```bash
cd <repo-root>   # streaming-proof repository root

helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update chaos-mesh

helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --version 2.8.3 \
  -f deploy/helm/chaos-mesh-values.yaml
```

`deploy/helm/chaos-mesh-values.yaml` handles two critical settings:

- **containerd runtime** — EKS nodes use containerd, but the chart defaults to
  docker. Without this override the chaos-daemon cannot reach containers and
  pod-kill silently fails.
- **dashboard exposed via an internet-facing NLB**, with `securityMode: false`
  (no token login).

> Installation pulls images from `ghcr.io` (the daemon image is ~197MB per node),
> so the first run can take tens of seconds. **Do not Ctrl+C.** Interrupting
> leaves the release in a `failed` state and requires
> `helm uninstall chaos-mesh -n chaos-mesh` before reinstalling.

Verify all components are Running (controller-manager ×3, one daemon per node,
dashboard, dns-server):

```bash
helm status chaos-mesh -n chaos-mesh | head -4
kubectl get pods -n chaos-mesh
```

---

## Step 2: Access the Dashboard

### Option A: Public NLB (wait 3–5 min after creation for target health checks)

```bash
kubectl get svc chaos-dashboard -n chaos-mesh \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{"\n"}'
# Open http://<hostname>:2333 in a browser
```

### Option B: port-forward (works immediately, bypasses the NLB)

```bash
kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333
# Open http://localhost:2333 in a browser
```

> With `securityMode: false` no token is required.
> ⚠️ The dashboard is internet-reachable. Since login is disabled, restrict
> access by source IP via `dashboard.service.loadBalancerSourceRanges` in
> `deploy/helm/chaos-mesh-values.yaml`, then re-run the Step 1 `helm upgrade`.

---

## Step 3: Deploy the experiments

> Depends on the CRDs from Step 1 — install Chaos Mesh first.

```bash
helm upgrade --install chaos ./deploy/helm/chaos-experiments -n o-2h056
```

Verify the Schedules were created:

```bash
kubectl get schedules -n o-2h056
```

⚠️ **Pod-kill starts on its cron as soon as the chart is applied.** Set the
relevant instance to `enabled: false` before applying when injection must not
start immediately. Pod-failure and network faults are disabled by default and
must be enabled explicitly.

See `deploy/helm/chaos-experiments/README.md` for the pod-kill, pod-failure,
network-delay, and network-partition designs. Every checked-in fault uses
`mode: one`, and the schedules are staggered to avoid overlapping faults within
the same instance.

---

## Uninstall

```bash
# Remove the experiments
helm uninstall chaos -n o-2h056

# Remove Chaos Mesh itself (CRDs are kept by default, harmless)
helm uninstall chaos-mesh -n chaos-mesh
```

---

## Troubleshooting

| Symptom | Cause / Fix |
|---------|-------------|
| `helm install` seems stuck | Usually pulling `ghcr.io` images; wait, don't interrupt |
| `helm status` = `failed` | Previous run was interrupted; `helm uninstall` then reinstall |
| NLB hostname not reachable | Target health checks take 3–5 min; verify the dashboard via port-forward |
| pod-kill has no effect | Confirm `chaosDaemon.runtime: containerd` (see chaos-mesh-values.yaml) |
| Schedules exist but kill nothing | Selector uses `pulsar-instance` + `role`; confirm the labels match real pods |
