package cmd

// engine.go holds the core engine type, shared state structs (runState,
// invalidationOps), and initialisation helpers (resolveAdminToken,
// initRings, initCluster, initCloudflare, buildDataPlane). Lifecycle
// orchestration lives in runner.go; subsystem construction helpers live
// in builder.go.

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"time"

	bouinecf "github.com/thylong/bouine/internal/cloudflare"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/runtime/shutdown"
	"github.com/thylong/bouine/internal/server"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
)

type engine struct {
	cfg        *config.Config
	configPath string
	startTime  time.Time
	logger     observability.Logger
	metrics    *observability.Metrics
}

func newEngine(cfg *config.Config, configPath string, logger *slog.Logger) *engine {
	return &engine{
		cfg:        cfg,
		configPath: configPath,
		startTime:  time.Now(),
		logger:     observability.NewSampledLogger(logger, observability.DefaultKeySampleRate),
		metrics:    observability.NewMetrics(),
	}
}

// runState bundles subsystem references created during engine startup.
// Passed to startAdmin and buildDashboard instead of 10+ positional args.
type runState struct {
	store        storage.Store
	pools        map[string]*origin.Pool
	dpMetrics    *observability.DataPlaneMetrics
	rings        *observability.Rings
	headerRing   *observability.OriginHeaderRing
	snapshotPath string
	token        string

	clusterNode    *cluster.Cluster
	peerFetcher    *cluster.PeerFetcher
	broadcaster    *cluster.Broadcaster
	peersFn        func() []api.PeerInfo
	antiEntropy    *cluster.AntiEntropy
	clusterMetrics *cluster.Metrics

	cfProp    *cfPropagator
	cfCancel  context.CancelFunc
	seq       *shutdown.Sequencer
	listeners []*server.Listener
}

// invalidationOps provides shared purge/ban/refresh closures used by
// both the admin API and the dashboard, eliminating duplication.
type invalidationOps struct {
	PurgeFn   func(ctx context.Context, url string) error
	BanFn     func(ctx context.Context, hostRegex, pathRegex string) (int, error)
	RefreshFn func(ctx context.Context, url string) error
}

func (e *engine) resolveAdminToken() string {
	// Env var takes precedence so operators can inject a token via Vault
	// without baking it into the config ConfigMap.
	if token := os.Getenv("BOUINE_ADMIN_TOKEN"); token != "" {
		return token
	}

	token := e.cfg.Admin.Token
	if token == "" {
		tok := make([]byte, 16)
		_, _ = rand.Read(tok)
		token = hex.EncodeToString(tok)
		e.logger.Warn("admin token not configured — using auto-generated token",
			"token", token,
			"hint", "set admin.token in config or BOUINE_ADMIN_TOKEN env var to silence this warning")
	}
	return token
}

func (e *engine) initRings() (*observability.Rings, string) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "bouine"
	}
	rings := observability.NewRings(hostname)
	snapshotPath := ""
	if e.cfg.Storage.WarmDir != "" {
		snapshotPath = e.cfg.Storage.WarmDir + "/metrics.snap"
	}
	if snapshotPath != "" {
		if err := rings.Load(snapshotPath); err != nil {
			e.logger.Warn("rings snapshot load failed", "error", err)
		}
	}
	return rings, snapshotPath
}

func (e *engine) initCluster(
	ctx context.Context,
	store storage.Store,
	rings *observability.Rings,
) (*cluster.Cluster, *cluster.PeerFetcher, *cluster.Broadcaster, func() []api.PeerInfo, *cluster.AntiEntropy, *cluster.Metrics) {
	if !e.cfg.Cluster.Enabled || e.cfg.Listen.Cluster == "" {
		return nil, nil, nil, nil, nil, nil
	}

	clusterNode, err := e.buildCluster(ctx)
	if err != nil {
		e.logger.Error("cluster init failed", "error", err)
		return nil, nil, nil, nil, nil, nil
	}

	var clusterTLS *tls.Config
	if e.cfg.Cluster.TLS.CABundle != "" {
		clusterTLS, err = buildClusterTLSConfig(e.cfg.Cluster.TLS)
		if err != nil {
			e.logger.Error("cluster TLS failed", "error", err)
			return nil, nil, nil, nil, nil, nil
		}
	}

	clusterMetrics := cluster.RegisterMetrics(e.metrics.Registry)
	clusterMetrics.SetMode(e.cfg.Cluster.Mode)
	if rings != nil && rings.Replication != nil {
		clusterMetrics.OnReplicationBytes = func(direction string, bytes float64) {
			rings.Replication.RecordReplication(direction, int(bytes))
		}
	}
	clusterNode.SetMetrics(clusterMetrics)

	peerFetcher := cluster.NewPeerFetcher(clusterTLS, e.metrics.Registry)
	broadcaster := cluster.NewBroadcaster(clusterNode, nil, "")

	clusterNode.SetInvalidator(cluster.Invalidator{
		PurgeFn: func(ctx context.Context, evt api.PurgeEvent) error {
			return store.Delete(ctx, evt.Key)
		},
		BanFn: func(ctx context.Context, evt api.BanEvent) error {
			_, err := store.Ban(ctx, evt.Predicate)
			return err
		},
	})

	var ae *cluster.AntiEntropy
	if e.cfg.Cluster.Mode == config.ClusterModeFull {
		clusterNode.SetReplicator(cluster.Replicator{
			StoreObject: func(ctx context.Context, obj *api.Object) error {
				return store.Put(ctx, obj.Key, obj)
			},
		})
		keyLister, ok := any(store).(storage.KeyLister)
		if !ok {
			e.logger.Warn("anti-entropy disabled: store does not implement KeyLister",
				"mode", e.cfg.Cluster.Mode)
		} else {
			ae = cluster.NewAntiEntropy(cluster.AntiEntropyConfig{
				Interval:      e.cfg.Cluster.AntiEntropyInterval,
				BackfillLimit: e.cfg.Cluster.BackfillLimit,
				Logger:        e.logger,
			}, e.cfg.Cluster.NodeName, keyLister, peerFetcher, store, clusterNode.Members, clusterMetrics)
		}
	}

	if e.cfg.Cluster.HopLimit > 0 && e.cfg.Cluster.Mode != config.ClusterModeStrong {
		e.logger.Warn("cluster.hop_limit has no effect in non-strong mode",
			"mode", e.cfg.Cluster.Mode, "hop_limit", e.cfg.Cluster.HopLimit)
	}
	if e.cfg.Cluster.Mode == config.ClusterModeFull {
		e.logger.Warn("cluster.mode 'full': memory scales linearly with cluster size")
	}

	return clusterNode, peerFetcher, broadcaster, clusterNode.Members, ae, clusterMetrics
}

func (e *engine) initCloudflare(dpMetrics *observability.DataPlaneMetrics, closeCtx context.Context) *cfPropagator {
	cfAPIToken := e.cfg.Cloudflare.APIToken
	if cfAPIToken == "" {
		cfAPIToken = os.Getenv("CF_API_TOKEN")
	}
	var cfInvalidator bouinecf.Invalidator
	if e.cfg.Cloudflare.ZoneID != "" && cfAPIToken != "" {
		cfClient, cfErr := bouinecf.New(bouinecf.Config{
			ZoneID:   e.cfg.Cloudflare.ZoneID,
			APIToken: cfAPIToken,
			Timeout:  e.cfg.Cloudflare.Timeout,
		})
		if cfErr != nil {
			e.logger.Warn("cloudflare init failed — propagation disabled", "error", cfErr)
		} else {
			cfInvalidator = cfClient
			e.logger.Info("cloudflare invalidation enabled",
				"zone", cfClient.ZoneID(),
				"async", e.cfg.Cloudflare.IsAsync(),
				"propagate", e.cfg.Cloudflare.Propagate)
		}
	}
	return buildCFPropagator(cfInvalidator, e.cfg.Cloudflare, dpMetrics, e.logger, closeCtx)
}

// buildDataPlane assembles the data-plane handler chain.
func (e *engine) buildDataPlane(rs *runState) http.Handler {
	return e.buildHandler(rs)
}
