{{/*
Expand the name of the chart.
*/}}
{{- define "qdic-assets.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "qdic-assets.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Chart label.
*/}}
{{- define "qdic-assets.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "qdic-assets.labels" -}}
helm.sh/chart: {{ include "qdic-assets.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: {{ include "qdic-assets.name" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | default .Chart.Version | quote }}
{{- end -}}

{{/*
Selector labels (must match Deployment selector).
*/}}
{{- define "qdic-assets.selectorLabels" -}}
app.kubernetes.io/name: {{ include "qdic-assets.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
