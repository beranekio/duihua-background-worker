{{- define "duihua-background-worker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "duihua-background-worker.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "duihua-background-worker.labels" -}}
helm.sh/chart: {{ include "duihua-background-worker.chart" . }}
{{ include "duihua-background-worker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "duihua-background-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "duihua-background-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "duihua-background-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "duihua-background-worker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.driver" -}}
{{- $autoscaling := .Values.autoscaling | default dict -}}
{{- $autoscaling.driver | default "store-metrics" -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.minReplicas" -}}
{{- $replicas := get .Values.autoscaling "replicas" | default dict -}}
{{- if hasKey $replicas "min" -}}
{{- $replicas.min -}}
{{- else -}}
1
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.maxReplicas" -}}
{{- $replicas := get .Values.autoscaling "replicas" | default dict -}}
{{- if hasKey $replicas "max" -}}
{{- $replicas.max -}}
{{- else -}}
4
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.metricsUrl" -}}
{{- $autoscaling := .Values.autoscaling | default dict -}}
{{- if $autoscaling.metricsUrl -}}
{{- $autoscaling.metricsUrl -}}
{{- else -}}
{{- fail "autoscaling.driver=store-metrics requires autoscaling.metricsUrl (or set autoscaling.driver=redis-streams)" -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.jobsPerReplica" -}}
{{- $autoscaling := .Values.autoscaling | default dict -}}
{{- $driver := include "duihua-background-worker.autoscaling.driver" . -}}
{{- if eq $driver "redis-streams" -}}
{{- if hasKey $autoscaling "lagCount" -}}
{{- $autoscaling.lagCount -}}
{{- else if hasKey $autoscaling "jobsPerReplica" -}}
{{- $autoscaling.jobsPerReplica -}}
{{- else -}}
5
{{- end -}}
{{- else -}}
{{- if hasKey $autoscaling "jobsPerReplica" -}}
{{- $autoscaling.jobsPerReplica -}}
{{- else if hasKey $autoscaling "lagCount" -}}
{{- $autoscaling.lagCount -}}
{{- else -}}
5
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.activationTargetValue" -}}
{{- $autoscaling := .Values.autoscaling | default dict -}}
{{- $driver := include "duihua-background-worker.autoscaling.driver" . -}}
{{- if eq $driver "redis-streams" -}}
{{- if hasKey $autoscaling "activationLagCount" -}}
{{- $autoscaling.activationLagCount -}}
{{- else if hasKey $autoscaling "activationTargetValue" -}}
{{- $autoscaling.activationTargetValue -}}
{{- else -}}
0
{{- end -}}
{{- else -}}
{{- if hasKey $autoscaling "activationTargetValue" -}}
{{- $autoscaling.activationTargetValue -}}
{{- else if hasKey $autoscaling "activationLagCount" -}}
{{- $autoscaling.activationLagCount -}}
{{- else -}}
0
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.activationLagCount" -}}
{{- include "duihua-background-worker.autoscaling.activationTargetValue" . -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.scaledownPeriod" -}}
{{- $autoscaling := .Values.autoscaling | default dict -}}
{{- if hasKey $autoscaling "scaledownPeriod" -}}
{{- $autoscaling.scaledownPeriod -}}
{{- else -}}
300
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.autoscaling.enabled" -}}
{{- if not .Values.enabled -}}
{{- else if not .Values.autoscaling.enabled -}}
{{- else -}}
true
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.valkey.address" -}}
{{- $valkey := .Values.autoscaling.valkey | default dict -}}
{{- if $valkey.address -}}
{{- $valkey.address -}}
{{- else -}}
{{- fail "autoscaling.driver=redis-streams requires autoscaling.valkey.address" -}}
{{- end -}}
{{- end -}}

{{- define "duihua-background-worker.valkey.streamKey" -}}
{{- $valkey := .Values.autoscaling.valkey | default dict -}}
{{- $valkey.streamKey | default "responses-api-store:background" -}}
{{- end -}}

{{- define "duihua-background-worker.validate.config" -}}
{{- if .Values.enabled -}}
{{- if not .Values.responsesApiStore.endpoint -}}
{{- fail "enabled=true requires responsesApiStore.endpoint" -}}
{{- end -}}
{{- if eq (include "duihua-background-worker.autoscaling.enabled" .) "true" -}}
{{- $driver := include "duihua-background-worker.autoscaling.driver" . -}}
{{- if eq $driver "store-metrics" -}}
{{- $_ := include "duihua-background-worker.autoscaling.metricsUrl" . -}}
{{- else if eq $driver "redis-streams" -}}
{{- $_ := include "duihua-background-worker.valkey.address" . -}}
{{- else -}}
{{- fail (printf "autoscaling.driver must be store-metrics or redis-streams, got %q" $driver) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}