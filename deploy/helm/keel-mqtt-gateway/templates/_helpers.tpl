{{/*
Chart name and fully qualified app name.
*/}}
{{- define "keel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "keel.fullname" -}}
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

{{- define "keel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "keel.labels" -}}
helm.sh/chart: {{ include "keel.chart" . }}
{{ include "keel.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "keel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "keel.core.fullname" -}}
{{- printf "%s-core" (include "keel.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "keel.edge.fullname" -}}
{{- printf "%s-edge" (include "keel.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "keel.core.selectorLabels" -}}
{{ include "keel.selectorLabels" . }}
app.kubernetes.io/component: core
{{- end }}

{{- define "keel.edge.selectorLabels" -}}
{{ include "keel.selectorLabels" . }}
app.kubernetes.io/component: edge
{{- end }}

{{- define "keel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "keel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "keel.image" -}}
{{- $root := .root -}}
{{- $override := .override | default dict -}}
{{- $repo := $override.repository | default $root.Values.image.repository -}}
{{- $tag := $override.tag | default $root.Values.image.tag | default $root.Chart.AppVersion -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}

{{- define "keel.imagePullPolicy" -}}
{{- $root := .root -}}
{{- $override := .override | default dict -}}
{{- $override.pullPolicy | default $root.Values.image.pullPolicy -}}
{{- end }}

{{/*
generateSecretName returns the name of the chart-managed Secret created
for a given external dependency's inline password (see
templates/secrets.yaml) — used consistently by both that template and by
every Deployment/StatefulSet that needs to reference it as a fallback
when no existingSecret is configured.
*/}}
{{- define "keel.generatedSecretName" -}}
{{- printf "%s-credentials" (include "keel.fullname" .) }}
{{- end }}

{{/*
envSecretRef renders a valueFrom.secretKeyRef env entry for one of the
external dependencies (postgresql, redpanda, kafka-hono). Pass a dict:
  root: the top-level context (.)
  existingSecret: .Values.<dep>.external.existingSecret (or "")
  existingSecretKey: .Values.<dep>.external.existingSecretPasswordKey
  generatedKey: the key this value is stored under in the chart-managed
                Secret (see templates/secrets.yaml) when no existingSecret
                is set
Never renders a plaintext password inline — always a secretKeyRef, to
either the user's own existingSecret or the one this chart generates.
*/}}
{{- define "keel.envSecretRef" -}}
{{- if .existingSecret -}}
secretKeyRef:
  name: {{ .existingSecret }}
  key: {{ .existingSecretKey }}
{{- else -}}
secretKeyRef:
  name: {{ include "keel.generatedSecretName" .root }}
  key: {{ .generatedKey }}
{{- end -}}
{{- end }}
