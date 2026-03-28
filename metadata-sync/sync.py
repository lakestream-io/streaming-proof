#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import copy
import json
import logging
import os
from typing import Any

import yaml
from kubernetes import client, config
from kubernetes.client import ApiException


LOGGER = logging.getLogger("streaming-proof-metadata-sync")


def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"missing required environment variable: {name}")
    return value


def load_kube_config() -> None:
    try:
        config.load_incluster_config()
        LOGGER.info("Loaded in-cluster Kubernetes configuration")
    except config.ConfigException:
        config.load_kube_config()
        LOGGER.info("Loaded local Kubernetes configuration")


def parse_driver_cluster_mappings(raw_value: str) -> dict[str, str]:
    data = json.loads(raw_value)
    if not isinstance(data, dict):
        raise ValueError("DRIVER_CLUSTER_MAPPINGS must be a JSON object")

    mappings: dict[str, str] = {}
    for driver_name, mapping in data.items():
        if isinstance(mapping, str):
            mappings[driver_name] = mapping
            continue
        if isinstance(mapping, dict):
            cluster_name = mapping.get("clusterName")
            if isinstance(cluster_name, str) and cluster_name:
                mappings[driver_name] = cluster_name
                continue
        raise ValueError(
            f"driver mapping for {driver_name} must be a string or object with clusterName"
        )

    return mappings


def compact(value: Any) -> Any:
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            compacted = compact(child)
            if compacted is None:
                continue
            if compacted == {} or compacted == []:
                continue
            result[key] = compacted
        return result

    if isinstance(value, list):
        result = [compact(child) for child in value]
        return [child for child in result if child is not None and child != {} and child != []]

    return value


def read_custom_object(
    custom_api: client.CustomObjectsApi,
    group: str,
    version: str,
    namespace: str,
    plural: str,
    name: str,
) -> dict[str, Any] | None:
    try:
        return custom_api.get_namespaced_custom_object(group, version, namespace, plural, name)
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


def read_oxia_cluster(
    custom_api: client.CustomObjectsApi,
    namespace: str,
    name: str,
) -> dict[str, Any] | None:
    candidates = [
        ("k8s.streamnative.io", "v1alpha1"),
        ("config.streamnative.io", "v1alpha1"),
        ("core.oxia.io", "v1"),
    ]
    for group, version in candidates:
        obj = read_custom_object(custom_api, group, version, namespace, "oxiaclusters", name)
        if obj is not None:
            return obj
    return None


def extract_resource_block(resource_spec: dict[str, Any] | None) -> dict[str, Any] | None:
    if not resource_spec:
        return None
    return compact(
        {
            "requests": resource_spec.get("requests"),
            "limits": resource_spec.get("limits"),
        }
    )


def extract_storage_request(claim_spec: dict[str, Any] | None) -> dict[str, Any] | None:
    if not claim_spec:
        return None
    return compact(
        {
            "storageClassName": claim_spec.get("storageClassName"),
            "accessModes": claim_spec.get("accessModes"),
            "requests": (claim_spec.get("resources") or {}).get("requests"),
        }
    )


def build_broker_resources(broker: dict[str, Any] | None) -> dict[str, Any] | None:
    if not broker:
        return None
    spec = broker.get("spec") or {}
    pod = spec.get("pod") or {}
    return compact(
        {
            "replicas": spec.get("replicas"),
            "resources": extract_resource_block(pod.get("resources")),
        }
    )


def build_bookkeeper_resources(bookkeeper: dict[str, Any] | None) -> dict[str, Any] | None:
    if not bookkeeper:
        return None
    spec = bookkeeper.get("spec") or {}
    pod = spec.get("pod") or {}
    storage = spec.get("storage") or {}
    return compact(
        {
            "replicas": spec.get("replicas"),
            "resources": extract_resource_block(pod.get("resources")),
            "journal": extract_storage_request((storage.get("journal") or {}).get("volumeClaimTemplate")),
            "ledger": extract_storage_request((storage.get("ledger") or {}).get("volumeClaimTemplate")),
        }
    )


def build_zookeeper_resources(zookeeper: dict[str, Any] | None) -> dict[str, Any] | None:
    if not zookeeper:
        return None
    spec = zookeeper.get("spec") or {}
    pod = spec.get("pod") or {}
    persistence = spec.get("persistence") or {}
    return compact(
        {
            "replicas": spec.get("replicas"),
            "resources": extract_resource_block(pod.get("resources")),
            "data": extract_storage_request(persistence.get("data")),
            "dataLog": extract_storage_request(persistence.get("dataLog")),
        }
    )


def build_oxia_resources(oxia: dict[str, Any] | None) -> dict[str, Any] | None:
    if not oxia:
        return None
    spec = oxia.get("spec") or {}
    server = spec.get("server") or {}
    return compact(
        {
            "replicas": (server.get("pod") or {}).get("replicas") or server.get("replicas"),
            "resources": extract_resource_block(server.get("resources")),
            "coordinatorResources": extract_resource_block((spec.get("coordinator") or {}).get("resources")),
            "storage": extract_storage_request(server.get("volumeClaimSpec")),
        }
    )


def build_driver_metadata(
    custom_api: client.CustomObjectsApi,
    source_namespace: str,
    cluster_name: str,
) -> dict[str, Any]:
    broker = read_custom_object(
        custom_api,
        "pulsar.streamnative.io",
        "v1alpha1",
        source_namespace,
        "pulsarbrokers",
        cluster_name,
    )
    bookkeeper = read_custom_object(
        custom_api,
        "bookkeeper.streamnative.io",
        "v1alpha1",
        source_namespace,
        "bookkeeperclusters",
        cluster_name,
    )
    zookeeper = read_custom_object(
        custom_api,
        "zookeeper.streamnative.io",
        "v1alpha1",
        source_namespace,
        "zookeeperclusters",
        cluster_name,
    )
    oxia = read_oxia_cluster(custom_api, source_namespace, cluster_name)

    metadata = compact(
        {
            "clusterResources": {
                "broker": build_broker_resources(broker),
                "bookkeeper": build_bookkeeper_resources(bookkeeper),
                "zookeeper": build_zookeeper_resources(zookeeper),
                "oxia": build_oxia_resources(oxia),
            },
            "pulsarConfig": ((broker or {}).get("spec") or {}).get("config", {}).get("custom"),
        }
    )

    return metadata


def update_driver_metadata(
    config_doc: dict[str, Any],
    driver_cluster_mappings: dict[str, str],
    custom_api: client.CustomObjectsApi,
    source_namespace: str,
) -> bool:
    drivers = config_doc.get("drivers")
    if not isinstance(drivers, dict):
        raise ValueError("configs file must contain a top-level drivers map")

    changed = False
    for driver_name, cluster_name in driver_cluster_mappings.items():
        driver_config = drivers.get(driver_name)
        if not isinstance(driver_config, dict):
            LOGGER.warning("Skipping missing driver %s", driver_name)
            continue

        metadata = build_driver_metadata(custom_api, source_namespace, cluster_name)
        desired_metadata = copy.deepcopy(metadata)
        current_metadata = driver_config.get("metadata")
        if current_metadata == desired_metadata:
            LOGGER.info("Driver %s metadata already up to date", driver_name)
            continue

        driver_config["metadata"] = desired_metadata
        changed = True
        LOGGER.info("Updated metadata for driver %s from cluster %s", driver_name, cluster_name)

    return changed


def main() -> None:
    configure_logging()
    load_kube_config()

    target_namespace = get_env("STREAMING_PROOF_NAMESPACE")
    target_configmap_name = get_env("STREAMING_PROOF_CONFIGMAP_NAME", "streaming-proof-configs")
    target_config_key = get_env("STREAMING_PROOF_CONFIG_KEY", "configs.yaml")
    source_namespace = get_env("METADATA_SOURCE_NAMESPACE")
    driver_cluster_mappings = parse_driver_cluster_mappings(get_env("DRIVER_CLUSTER_MAPPINGS"))
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    core_api = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    config_map = core_api.read_namespaced_config_map(target_configmap_name, target_namespace)
    config_blob = (config_map.data or {}).get(target_config_key)
    if not config_blob:
        raise ValueError(
            f"config map {target_configmap_name} does not contain key {target_config_key}"
        )

    config_doc = yaml.safe_load(config_blob) or {}
    changed = update_driver_metadata(
        config_doc,
        driver_cluster_mappings,
        custom_api,
        source_namespace,
    )

    if not changed:
        LOGGER.info("No metadata changes detected")
        return

    rendered_config = yaml.safe_dump(config_doc, sort_keys=False)
    patch_body = {"data": {target_config_key: rendered_config}}

    if dry_run:
        LOGGER.info("Dry run enabled, skipping ConfigMap patch")
        LOGGER.info("Rendered config:\n%s", rendered_config)
        return

    core_api.patch_namespaced_config_map(target_configmap_name, target_namespace, patch_body)
    LOGGER.info(
        "Patched ConfigMap %s/%s key %s",
        target_namespace,
        target_configmap_name,
        target_config_key,
    )


if __name__ == "__main__":
    main()
