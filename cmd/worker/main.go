package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-ops-worker/internal/config"
	"github.com/cyxc1124/osspilot-ops-worker/internal/lifecycle"
	"github.com/cyxc1124/osspilot-ops-worker/internal/rgw"
	"github.com/cyxc1124/osspilot-ops-worker/internal/settings"
)

func main() {
	cfg := config.Load()
	if cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL is required for the lifecycle worker")
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	settingsStore := settings.NewStore(pool)
	rules := lifecycle.NewStore(pool)
	interval := cfg.LifecycleInterval
	fb := settings.Fallbacks{
		S3Endpoint: cfg.S3Endpoint, RGWAccessKey: cfg.RGWAccessKey, RGWSecretKey: cfg.RGWSecretKey,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runOnce := func() {
		enabled, endpoint, ak, sk, err := loadS3(ctx, settingsStore, fb)
		if err != nil {
			slog.Error("load settings", "err", err)
			return
		}
		if !enabled {
			slog.Info("lifecycle cleanup skipped (disabled)")
			return
		}
		cli := rgw.New(endpoint, ak, sk)
		if cli == nil {
			slog.Warn("S3/RGW is not configured")
			return
		}
		if err := (&lifecycle.Runner{Rules: rules, S3: cli}).Run(ctx); err != nil {
			slog.Error("lifecycle run", "err", err)
		}
	}

	runOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	slog.Info("lifecycle worker listen", "interval", interval.String())
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
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
