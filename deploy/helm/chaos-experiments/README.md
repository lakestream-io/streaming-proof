# chaos-experiments

Recurring Chaos Mesh **pod-kill** experiments for a managed Pulsar cluster. Each
experiment kills ONE pod of a role (pulsar-broker / bookkeeper / oxia), matched
by the `pulsar-instance` + `role` labels, on a staggered schedule so the kills
never overlap.

This chart only renders Chaos Mesh `Schedule` resources. It does not install
Chaos Mesh itself.

## 1. Install Chaos Mesh (once, on the DATA PLANE cluster)

The Pulsar pods run in the data plane (the pool member), so Chaos Mesh must be
installed there — not on the control plane.

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace
```

## 2. Point at your instance

`values.yaml` targets the current staging instance (`namespace: o-2h056`,
`pulsarInstance: zike-lakestream-test`). Change these for another instance.
Disable any component that doesn't exist (e.g. an Ursa cluster has no `bookie`).

## 3. Apply against the data plane

Direct kubeconfig:

```bash
helm install chaos ./deploy/helm/chaos-experiments --kube-context <data-plane-context>
```

Through `apiserver-admin` (render + pipe):

```bash
helm template chaos ./deploy/helm/chaos-experiments \
  | apiserver-admin pools connect aws-use1-staging-q3p9f \
      -n o-2h056 --context gke_sncloud-staging_us-west1-a_api --org o-2h056 -q \
      -- kubectl apply -f -
```

Pods are matched by labels `cloud.streamnative.io/pulsar-instance` +
`cloud.streamnative.io/role`. Common role values: `pulsar-broker`,
`bookkeeper`, `oxia`.

## Default schedules (staggered)

| Instance               | Role          | Cron               | Fires at        |
|------------------------|---------------|--------------------|-----------------|
| `zike-lakestream-test` | pulsar-broker | `*/15 * * * *`     | :00 :15 :30 :45 |
| `zike-lakestream-test` | oxia          | `10-59/15 * * * *` | :10 :25 :40 :55 |
