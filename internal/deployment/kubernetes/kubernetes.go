package kubernetes

import (
	"context"
	"fmt"
	"strings"

	"cpip/internal/deployment/probes"
	"cpip/internal/deployment/services"
)

// Provider implements deployment.Provider interface for Kubernetes.
type Provider struct {
	name string
}

// NewProvider constructs a Kubernetes Provider.
func NewProvider() *Provider {
	return &Provider{name: "kubernetes"}
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return p.name
}

// Generate creates a multi-document YAML string containing Namespace, ResourceQuota,
// LimitRange, NetworkPolicy, PDB, PVCs, Deployments, Services, and Ingresses.
func (p *Provider) Generate(_ context.Context, namespace, profile string, svcs []services.Service) (string, error) {
	var sb strings.Builder

	// 1. Namespace
	sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels:
    environment: %s
---
`, namespace, profile))

	// 2. ResourceQuota
	sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: ResourceQuota
metadata:
  name: cpip-quota
  namespace: %s
spec:
  hard:
    requests.cpu: "10"
    requests.memory: 20Gi
    limits.cpu: "20"
    limits.memory: 40Gi
    persistentvolumeclaims: "10"
---
`, namespace))

	// 3. LimitRange
	sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: LimitRange
metadata:
  name: cpip-limits
  namespace: %s
spec:
  limits:
  - default:
      cpu: 500m
      memory: 512Mi
    defaultRequest:
      cpu: 100m
      memory: 256Mi
    type: Container
---
`, namespace))

	// 4. Default Deny-All NetworkPolicy (highly secure default)
	sb.WriteString(fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: %s
spec:
  podSelector: {}
  policyTypes:
  - Ingress
---
`, namespace))

	// Iterate through each service and generate manifests
	for _, s := range svcs {
		// 5. ServiceAccount
		sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa-%s
  namespace: %s
---
`, s.Name, namespace))

		// 6. NetworkPolicy (Allow ingress to ports)
		if len(s.Ports) > 0 {
			var portsYaml []string
			for _, port := range s.Ports {
				portsYaml = append(portsYaml, fmt.Sprintf("    - port: %d\n      protocol: %s", port.ContainerPort, port.Protocol))
			}
			sb.WriteString(fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-%s
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: %s
  ingress:
  - ports:
%s
  policyTypes:
  - Ingress
---
`, s.Name, namespace, s.Name, strings.Join(portsYaml, "\n")))
		}

		// 7. PodDisruptionBudget (only if replicas > 1 for high availability)
		if s.Replicas > 1 {
			sb.WriteString(fmt.Sprintf(`apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: pdb-%s
  namespace: %s
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: %s
---
`, s.Name, namespace, s.Name))
		}

		// 8. PersistentVolumeClaims (if volume type is PVC)
		for _, vol := range s.Volumes {
			if vol.Type == services.VolumePVC {
				sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: %s
---
`, vol.Name, namespace, vol.Size))
			}
		}

		// 9. Deployment
		sb.WriteString(fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      serviceAccountName: sa-%s
      containers:
      - name: %s
        image: %s:%s
`, s.Name, namespace, s.Name, s.Replicas, s.Name, s.Name, s.Name, s.Name, s.Image, s.Version))

		// Resources (CPU and Memory)
		if s.Resources.CPURequest != "" || s.Resources.CPULimit != "" || s.Resources.MemoryRequest != "" || s.Resources.MemoryLimit != "" {
			sb.WriteString("        resources:\n")
			if s.Resources.CPURequest != "" || s.Resources.MemoryRequest != "" {
				sb.WriteString("          requests:\n")
				if s.Resources.CPURequest != "" {
					sb.WriteString(fmt.Sprintf("            cpu: %s\n", s.Resources.CPURequest))
				}
				if s.Resources.MemoryRequest != "" {
					sb.WriteString(fmt.Sprintf("            memory: %s\n", s.Resources.MemoryRequest))
				}
			}
			if s.Resources.CPULimit != "" || s.Resources.MemoryLimit != "" || s.Resources.GPUCount > 0 {
				sb.WriteString("          limits:\n")
				if s.Resources.CPULimit != "" {
					sb.WriteString(fmt.Sprintf("            cpu: %s\n", s.Resources.CPULimit))
				}
				if s.Resources.MemoryLimit != "" {
					sb.WriteString(fmt.Sprintf("            memory: %s\n", s.Resources.MemoryLimit))
				}
				if s.Resources.GPUCount > 0 {
					// Future GPU limit configuration
					sb.WriteString(fmt.Sprintf("            nvidia.com/gpu: %d\n", s.Resources.GPUCount))
				}
			}
		}

		// Ports
		if len(s.Ports) > 0 {
			sb.WriteString("        ports:\n")
			for _, port := range s.Ports {
				sb.WriteString(fmt.Sprintf("        - name: %s\n          containerPort: %d\n          protocol: %s\n", port.Name, port.ContainerPort, port.Protocol))
			}
		}

		// Environment Variables
		if len(s.Env) > 0 || len(s.Secrets) > 0 {
			sb.WriteString("        env:\n")
			for k, v := range s.Env {
				sb.WriteString(fmt.Sprintf("        - name: %s\n          value: %q\n", k, v))
			}
			for k, ref := range s.Secrets {
				// Map env variable to Kubernetes Secret key-ref
				secretName := fmt.Sprintf("%s-secrets", s.Name)
				sb.WriteString(fmt.Sprintf("        - name: %s\n          valueFrom:\n            secretKeyRef:\n              name: %s\n              key: %s\n", k, secretName, ref))
			}
		}

		// Probes (Liveness/Readiness/Startup)
		if s.Health.Startup != nil {
			sb.WriteString("        startupProbe:\n")
			writeProbeYaml(&sb, s.Health.Startup)
		}
		if s.Health.Readiness != nil {
			sb.WriteString("        readinessProbe:\n")
			writeProbeYaml(&sb, s.Health.Readiness)
		}
		if s.Health.Liveness != nil {
			sb.WriteString("        livenessProbe:\n")
			writeProbeYaml(&sb, s.Health.Liveness)
		}

		// Volumes and VolumeMounts
		if len(s.Volumes) > 0 {
			sb.WriteString("        volumeMounts:\n")
			for _, vol := range s.Volumes {
				sb.WriteString(fmt.Sprintf("        - name: %s\n          mountPath: %s\n", vol.Name, vol.MountPath))
				if vol.SubPath != "" {
					sb.WriteString(fmt.Sprintf("          subPath: %s\n", vol.SubPath))
				}
				if vol.ReadOnly {
					sb.WriteString("          readOnly: true\n")
				}
			}
			sb.WriteString("      volumes:\n")
			for _, vol := range s.Volumes {
				sb.WriteString(fmt.Sprintf("      - name: %s\n", vol.Name))
				if vol.Type == services.VolumePVC {
					sb.WriteString(fmt.Sprintf("        persistentVolumeClaim:\n          claimName: %s\n", vol.Name))
				} else if vol.Type == services.VolumeHostPath {
					sb.WriteString(fmt.Sprintf("        hostPath:\n          path: %s\n", vol.HostPath))
				} else if vol.Type == services.VolumeEmptyDir {
					sb.WriteString("        emptyDir: {}\n")
				} else if vol.Type == services.VolumeSecretMap {
					sb.WriteString(fmt.Sprintf("        secret:\n          secretName: %s\n", vol.Name))
				} else {
					sb.WriteString("        emptyDir: {}\n")
				}
			}
		}

		sb.WriteString("---\n")

		// 10. Service (ClusterIP/NodePort)
		if len(s.Ports) > 0 {
			sb.WriteString(fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: svc-%s
  namespace: %s
  labels:
    app: %s
spec:
  type: ClusterIP
  selector:
    app: %s
  ports:
`, s.Name, namespace, s.Name, s.Name))
			for _, port := range s.Ports {
				sb.WriteString(fmt.Sprintf("  - name: %s\n    port: %d\n    targetPort: %d\n    protocol: %s\n", port.Name, port.ServicePort, port.ContainerPort, port.Protocol))
			}
			sb.WriteString("---\n")
		}

		// 11. Ingress (only for API / Gateway types that require external routing)
		if s.Type == services.TypeAPI || s.Type == services.TypeGateway {
			sb.WriteString(fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ingress-%s
  namespace: %s
  annotations:
    kubernetes.io/ingress.class: nginx
spec:
  rules:
  - host: cpip.%s.local
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: svc-%s
            port:
              number: %d
---
`, s.Name, namespace, profile, s.Name, s.Ports[0].ServicePort))
		}
	}

	return sb.String(), nil
}

func writeProbeYaml(sb *strings.Builder, pc *probes.ProbeConfig) {
	if pc.Action.Type == probes.ActionHTTP {
		sb.WriteString(fmt.Sprintf("          httpGet:\n            path: %s\n            port: %d\n", pc.Action.Path, pc.Action.Port))
	} else if pc.Action.Type == probes.ActionTCP {
		sb.WriteString(fmt.Sprintf("          tcpSocket:\n            port: %d\n", pc.Action.Port))
	} else if pc.Action.Type == probes.ActionExec && len(pc.Action.Command) > 0 {
		sb.WriteString("          exec:\n            command:\n")
		for _, cmd := range pc.Action.Command {
			sb.WriteString(fmt.Sprintf("            - %s\n", cmd))
		}
	}
	sb.WriteString(fmt.Sprintf("          initialDelaySeconds: %d\n", pc.InitialDelaySeconds))
	sb.WriteString(fmt.Sprintf("          periodSeconds: %d\n", pc.PeriodSeconds))
	sb.WriteString(fmt.Sprintf("          timeoutSeconds: %d\n", pc.TimeoutSeconds))
	sb.WriteString(fmt.Sprintf("          successThreshold: %d\n", pc.SuccessThreshold))
	sb.WriteString(fmt.Sprintf("          failureThreshold: %d\n", pc.FailureThreshold))
}
