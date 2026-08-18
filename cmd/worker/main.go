package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-ops-worker/internal/config"
	"github.com/cyxc1124/osspilot-ops-worker/internal/lifecycle"
	"github.com/cyxc1124/osspilot-ops-worker/internal/logx"
	"github.com/cyxc1124/osspilot-ops-worker/internal/queue"
	"github.com/cyxc1124/osspilot-ops-worker/internal/rgw"
	"github.com/cyxc1124/osspilot-ops-worker/internal/settings"
)

func main() {
	logx.Setup("osspilot-ops-worker")
	cfg := config.Load()
	if cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL is required for the lifecycle worker")
		os.Exit(1)
	}
	if cfg.RedisURL == "" {
		slog.Error("REDIS_URL is required for the lifecycle worker")
		os.Exit(1)
	}
	redisOpt, err := asynq.ParseRedisURI(cfg.RedisURL)
	if err != nil {
		slog.Error("REDIS_URL", "err", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	jobs := &jobs{
		settings: settings.NewStore(pool),
		rules:    lifecycle.NewStore(pool),
		fb: settings.Fallbacks{
			S3Endpoint: cfg.S3Endpoint, RGWAccessKey: cfg.RGWAccessKey, RGWSecretKey: cfg.RGWSecretKey,
		},
		tenantAPI: cfg.TenantAPIURL,
		secret:    cfg.ProjectionSecret,
	}

	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskLifecycleRule, jobs.RunRule)
	mux.HandleFunc(queue.TaskLifecycle, jobs.Run)

	go serveHealthz(cfg.HTTPAddr)
	slog.Info("lifecycle worker listen", "concurrency", cfg.AsynqConcurrency)
	srv := asynq.NewServer(redisOpt, asynq.Config{Concurrency: cfg.AsynqConcurrency})
	if err := srv.Run(withTaskLog(mux)); err != nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}
}

type jobs struct {
	settings  *settings.Store
	rules     *lifecycle.Store
	fb        settings.Fallbacks
	tenantAPI string
	secret    string
}

func (j *jobs) RunRule(ctx context.Context, t *asynq.Task) error {
	var p struct {
		RuleID int64 `json:"rule_id"`
	}
	if err := json.Unmarshal(t.Payload(), &p); err != nil || p.RuleID < 1 {
		return fmt.Errorf("invalid lifecycle rule payload")
	}
	enabled, endpoint, ak, sk, err := loadS3(ctx, j.settings, j.fb)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	if !enabled {
		slog.Info("lifecycle cleanup skipped (disabled)")
		return nil
	}
	cli := rgw.New(endpoint, ak, sk)
	if cli == nil {
		slog.Warn("S3/RGW is not configured")
		return nil
	}
	rule, err := j.rules.GetByID(ctx, p.RuleID)
	if err != nil {
		return err
	}
	if rule == nil || !rule.Enabled {
		slog.Info("lifecycle rule skipped", "id", p.RuleID)
		return nil
	}
	return (&lifecycle.Runner{
		Rules: j.rules, S3: cli,
		After: func(ctx context.Context, bucket string) {
			enqueueInventory(ctx, j.tenantAPI, j.secret, bucket)
		},
	}).RunRule(ctx, *rule)
}

func (j *jobs) Run(ctx context.Context, _ *asynq.Task) error {
	slog.Info("lifecycle run start")
	enabled, endpoint, ak, sk, err := loadS3(ctx, j.settings, j.fb)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	if !enabled {
		slog.Info("lifecycle cleanup skipped (disabled)")
		return nil
	}
	cli := rgw.New(endpoint, ak, sk)
	if cli == nil {
		slog.Warn("S3/RGW is not configured")
		return nil
	}
	if err := (&lifecycle.Runner{
		Rules: j.rules, S3: cli,
		After: func(ctx context.Context, bucket string) {
			enqueueInventory(ctx, j.tenantAPI, j.secret, bucket)
		},
	}).Run(ctx); err != nil {
		return err
	}
	return nil
}

func withTaskLog(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		slog.Info("task start", "type", t.Type())
		if err := next.ProcessTask(ctx, t); err != nil {
			slog.Error("task fail", "type", t.Type(), "err", err)
			return err
		}
		return nil
	})
}

func loadS3(ctx context.Context, store *settings.Store, fb settings.Fallbacks) (enabled bool, endpoint, ak, sk string, err error) {
	rows, err := store.Load(ctx)
	if err != nil {
		return false, "", "", "", err
	}
	enabled = true
	switch strings.ToLower(strings.TrimSpace(rows["lifecycle_cleanup_enabled"].Value)) {
	case "0", "false", "no", "off":
		enabled = false
	}
	pick := func(key, fallback string) string {
		if v := strings.TrimSpace(rows[key].Value); v != "" {
			return v
		}
		return fallback
	}
	return enabled, pick("s3_endpoint", fb.S3Endpoint), pick("rgw_access_key", fb.RGWAccessKey), pick("rgw_secret_key", fb.RGWSecretKey), nil
}

func enqueueInventory(ctx context.Context, base, secret, bucket string) {
	if strings.TrimSpace(base) == "" || strings.TrimSpace(secret) == "" {
		slog.Warn("lifecycle inventory skipped", "bucket", bucket, "reason", "TENANT_API_URL/PROJECTION_SECRET unset")
		return
	}
	u := strings.TrimRight(base, "/") + "/internal/buckets/" + url.PathEscape(bucket) + "/inventory"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader("{}"))
	if err != nil {
		slog.Warn("lifecycle inventory", "bucket", bucket, "err", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		slog.Warn("lifecycle inventory", "bucket", bucket, "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		slog.Warn("lifecycle inventory", "bucket", bucket, "status", resp.StatusCode)
	}
}

func serveHealthz(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	slog.Info("healthz listen", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("healthz", "err", err)
	}
}
