{{- define "gpuTelemetry.databaseURL" -}}
{{- if and (not .Values.database.postgresEnabled) (empty .Values.database.url) -}}
{{- fail "database.url is required when database.postgresEnabled=false" -}}
{{- end -}}
{{- default (printf "postgres://postgres@%s-postgres:5432/telemetry?sslmode=disable" .Release.Name) .Values.database.url -}}
{{- end -}}
