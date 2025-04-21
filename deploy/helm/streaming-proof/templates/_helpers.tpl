{{/*
Create chart name and version as used by the chart label
*/}}
{{- define "streaming-proof.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "streaming-proof.labels" -}}
helm.sh/chart: {{ include "streaming-proof.chart" . }}
app: streaming-proof
{{- end }}