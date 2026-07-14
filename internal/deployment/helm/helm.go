package helm

import (
	"context"
	"fmt"
	"strings"

	"cpip/internal/deployment/services"
)

// Chart represents the generated Helm Chart file contents.
type Chart struct {
	Name    string            `json:"name"`
	Version string            `json:"version"`
	Files   map[string]string `json:"files"` // Path relative to chart root -> content
}

// Generator generates standard Helm v3 charts programmatically.
type Generator struct{}

// NewGenerator constructs a Helm Generator.
func NewGenerator() *Generator {
	return &Generator{}
}

// Generate generates the Chart.yaml, values.yaml, and templates directory contents.
func (g *Generator) Generate(_ context.Context, chartName, chartVersion string, svcs []services.Service) (Chart, error) {
	files := make(map[string]string)

	// 1. Chart.yaml
	files["Chart.yaml"] = fmt.Sprintf(`apiVersion: v2
name: %s
description: Reusable Helm Chart for Collaborative Programming Infrastructure Platform (CPIP)
type: application
version: %s
appVersion: "1.0.0"
`, chartName, chartVersion)

	// 2. values.yaml
	var valSb strings.Builder
	valSb.WriteString("global:\n  environment: development\n\nservices:\n")
	for _, s := range svcs {
		valSb.WriteString(fmt.Sprintf("  %s:\n", s.Name))
		valSb.WriteString(fmt.Sprintf("    image: %s\n", s.Image))
		valSb.WriteString(fmt.Sprintf("    tag: %s\n", s.Version))
		valSb.WriteString(fmt.Sprintf("    replicas: %d\n", s.Replicas))
		valSb.WriteString("    resources:\n")
		valSb.WriteString(fmt.Sprintf("      requests:\n        cpu: %q\n        memory: %q\n", s.Resources.CPURequest, s.Resources.MemoryRequest))
		valSb.WriteString(fmt.Sprintf("      limits:\n        cpu: %q\n        memory: %q\n", s.Resources.CPULimit, s.Resources.MemoryLimit))
		if len(s.Ports) > 0 {
			valSb.WriteString("    ports:\n")
			for _, p := range s.Ports {
				valSb.WriteString(fmt.Sprintf("      - name: %s\n        containerPort: %d\n        servicePort: %d\n        protocol: %s\n", p.Name, p.ContainerPort, p.ServicePort, p.Protocol))
			}
		}
	}
	files["values.yaml"] = valSb.String()

	// 3. templates/_helpers.tpl
	files["templates/_helpers.tpl"] = fmt.Sprintf(`{{/*
Expand the name of the chart.
*/}}
{{- define "%s.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "%s.chart" -}}
{{- printf "%%s-%%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}
`, chartName, chartName)

	// 4. templates/deployment.yaml (Generic template iterating over values.services)
	files["templates/deployment.yaml"] = `{{- range $svcName, $svcValues := .Values.services }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $svcName }}
  labels:
    app: {{ $svcName }}
spec:
  replicas: {{ $svcValues.replicas | default 1 }}
  selector:
    matchLabels:
      app: {{ $svcName }}
  template:
    metadata:
      labels:
        app: {{ $svcName }}
    spec:
      containers:
      - name: {{ $svcName }}
        image: "{{ $svcValues.image }}:{{ $svcValues.tag }}"
        ports:
        {{- range $svcValues.ports }}
        - name: {{ .name }}
          containerPort: {{ .containerPort }}
          protocol: {{ .protocol }}
        {{- end }}
        resources:
          {{- toYaml $svcValues.resources | nindent 10 }}
---
{{- end }}
`

	// 5. templates/service.yaml
	files["templates/service.yaml"] = `{{- range $svcName, $svcValues := .Values.services }}
{{- if $svcValues.ports }}
apiVersion: v1
kind: Service
metadata:
  name: svc-{{ $svcName }}
  labels:
    app: {{ $svcName }}
spec:
  type: ClusterIP
  selector:
    app: {{ $svcName }}
  ports:
  {{- range $svcValues.ports }}
  - name: {{ .name }}
    port: {{ .servicePort }}
    targetPort: {{ .containerPort }}
    protocol: {{ .protocol }}
  {{- end }}
---
{{- end }}
{{- end }}
`

	return Chart{
		Name:    chartName,
		Version: chartVersion,
		Files:   files,
	}, nil
}
