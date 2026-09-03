{{/* Build a DNS-safe Schedule name for newly added fault types. */}}
{{- define "chaos.fullname" -}}
{{- printf "%s-%s-%s-%s" (index . 0) (index . 1) (index . 2) (index . 3) | lower | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Select one cluster component inside the target namespace. */}}
{{- define "chaos.podSelector" -}}
namespaces:
  - {{ .namespace | quote }}
labelSelectors:
  {{ .clusterLabel | quote }}: {{ .cluster | quote }}
  {{ .roleLabel | quote }}: {{ .role | quote }}
{{- end -}}
