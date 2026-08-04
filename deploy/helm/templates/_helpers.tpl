{{/*
Name helpers. Kubernetes truncates at 63 characters for DNS compatibility, so
every generated name is clipped and stripped of a trailing hyphen.
*/}}

{{- define "recommendations.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "recommendations.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "recommendations.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full label set, applied to every object. Uses the standard Kubernetes
recommended labels so kubectl, dashboards, and cost tooling can all group
resources without bespoke conventions.
*/}}
{{- define "recommendations.labels" -}}
helm.sh/chart: {{ include "recommendations.chart" . }}
{{ include "recommendations.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: personalization-platform
{{- end }}

{{/*
Selector labels are a strict subset and must never include anything that
changes between releases. A Deployment's selector is immutable, so putting the
version in here would make every upgrade fail.
*/}}
{{- define "recommendations.selectorLabels" -}}
app.kubernetes.io/name: {{ include "recommendations.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "recommendations.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "recommendations.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
