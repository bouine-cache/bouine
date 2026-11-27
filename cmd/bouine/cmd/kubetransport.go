package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// kubeProxyMinVersion is the first bouine version whose admin server
// strips the API-server services/proxy path prefix. kube:// remotes are
// refused against older daemons via an explicit GET /version check.
const kubeProxyMinVersion = "0.7.0"

// kubeTransport implements bouineapi.RoundTripper over the Kubernetes
// API-server services/proxy subresource:
// /api/v1/namespaces/<ns>/services/<svc>:<port>/proxy/<path>.
type kubeTransport struct {
	client   *rest.RESTClient
	basePath string
}

// parseKubeServer parses kube://<namespace>/<service>:<port>.
func parseKubeServer(server string) (ns, svc string, port int, err error) {
	rest0, ok := strings.CutPrefix(server, "kube://")
	if !ok {
		return "", "", 0, fmt.Errorf("not a kube:// server %q", server)
	}
	nsPart, svcPart, ok := strings.Cut(rest0, "/")
	if !ok || nsPart == "" || svcPart == "" {
		return "", "", 0, fmt.Errorf("kube server must be kube://<namespace>/<service>:<port>, got %q", server)
	}
	var portStr string
	svc, portStr, ok = strings.Cut(svcPart, ":")
	if !ok {
		return "", "", 0, fmt.Errorf("kube service must include a port, got %q", svcPart)
	}
	var p int
	if _, err := fmt.Sscanf(portStr, "%d", &p); err != nil || p <= 0 || p > 65535 {
		return "", "", 0, fmt.Errorf("invalid port %q", portStr)
	}
	return nsPart, svc, p, nil
}

// newKubeTransport builds the transport for kube://<ns>/<svc>:<port>
// using the default kubeconfig loading rules.
func newKubeTransport(server string) (*kubeTransport, error) {
	ns, svc, port, err := parseKubeServer(server)
	if err != nil {
		return nil, err
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	rc, err := rest.RESTClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("build API-server client: %w", err)
	}
	return &kubeTransport{
		basePath: fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%d/proxy", ns, svc, port),
		client:   rc,
	}, nil
}

// Do implements bouineapi.RoundTripper.
func (t *kubeTransport) Do(ctx context.Context, method, path string, body []byte, bearerToken string) (int, []byte, error) {
	var req *rest.Request
	switch method {
	case "GET":
		req = t.client.Get()
	case "POST":
		req = t.client.Post()
	default:
		return 0, nil, fmt.Errorf("unsupported method %q over kube proxy", method)
	}
	req = req.AbsPath(t.basePath + path)
	req.SetHeader("Accept", "application/json")
	if method == "POST" {
		req.SetHeader("Content-Type", "application/json")
		if body != nil {
			req.Body(bytes.NewReader(body))
		}
	}
	if bearerToken != "" {
		req.SetHeader("Authorization", "Bearer "+bearerToken)
	}
	res := req.Do(ctx)
	raw, err := res.Raw()
	if err != nil {
		if result := res.Error(); result != nil {
			return 0, nil, fmt.Errorf("kube proxy %s %s: %w", method, path, result)
		}
		return 0, nil, fmt.Errorf("kube proxy %s %s: %w", method, path, err)
	}
	var status int
	res.StatusCode(&status)
	return status, raw, nil
}

var _ bouineapi.RoundTripper = (*kubeTransport)(nil)
