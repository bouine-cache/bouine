{{- define "bouine.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "bouine.fullname" -}}
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

{{- define "bouine.labels" -}}
helm.sh/chart: {{ include "bouine.name" . }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "bouine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "bouine.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bouine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
bouine.goMemLimit returns the GOMEMLIMIT env var value.
If .Values.goMemLimit is set, it is used as-is (manual override).
Otherwise, the value is auto-computed as 75% of
.Values.resources.limits.memory. Supports the common Kubernetes
binary quantity suffixes (Gi, Mi, Ki, Ti). Plain numbers are
treated as bytes. Unrecognized suffixes are passed through unchanged.
*/}}
{{- define "bouine.goMemLimit" -}}
{{- if .Values.goMemLimit -}}
{{- .Values.goMemLimit -}}
{{- else -}}
{{- $mem := .Values.resources.limits.memory | toString -}}
{{- if hasSuffix "Ti" $mem -}}
{{- printf "%.0fTiB" (mulf (float64 (trimSuffix "Ti" $mem)) 0.75) -}}
{{- else if hasSuffix "Gi" $mem -}}
{{- printf "%.0fGiB" (mulf (float64 (trimSuffix "Gi" $mem)) 0.75) -}}
{{- else if hasSuffix "Mi" $mem -}}
{{- printf "%.0fMiB" (mulf (float64 (trimSuffix "Mi" $mem)) 0.75) -}}
{{- else if hasSuffix "Ki" $mem -}}
{{- printf "%.0fKiB" (mulf (float64 (trimSuffix "Ki" $mem)) 0.75) -}}
{{- else -}}
{{- printf "%.0f" (mulf (float64 $mem) 0.75) -}}
{{- end -}}
{{- end -}}
{{- end }}
