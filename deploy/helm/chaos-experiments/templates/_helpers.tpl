{{/* Build a DNS-safe Schedule name for newly added fault types. */}}
{{- define "chaos.fullname" -}}
{{- printf "%s-%s-%s-%s" (index . 0) (index . 1) (index . 2) (index . 3) | lower | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Select one Pulsar cluster role inside the target namespace. */}}
{{- define "chaos.podSelector" -}}
namespaces:
  - {{ .namespace | quote }}
labelSelectors:
  cloud.streamnative.io/cluster: {{ .cluster | quote }}
  cloud.streamnative.io/role: {{ .role | quote }}
{{- end -}}
