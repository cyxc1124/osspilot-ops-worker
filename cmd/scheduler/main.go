package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-ops-worker/internal/config"
	"github.com/cyxc1124/osspilot-ops-worker/internal/lifecycle"
	"github.com/cyxc1124/osspilot-ops-worker/internal/logx"
	"github.com/cyxc1124/osspilot-ops-worker/internal/queue"
	"github.com/cyxc1124/osspilot-ops-worker/internal/sched"
)

func main() {
	logx.Setup("osspilot-ops-scheduler")
	cfg := config.Load()
	if cfg.RedisURL == "" || cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL and REDIS_URL are required for the scheduler")
		os.Exit(1)
	}
	redisOpt, err := asynq.ParseRedisURI(cfg.RedisURL)
	if err != nil {
		slog.Error("REDIS_URL", "err", err)
		os.Exit(1)
	}
	group, err := sched.New(cfg.RedisURL, "ops")
	if err != nil {
		slog.Error("scheduler group", "err", err)
		os.Exit(1)
	}
	defer group.Close()
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	client := asynq.NewClient(redisOpt)
	defer client.Close()
	rules := lifecycle.NewStore(pool)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go serveHealthz(cfg.HTTPAddr)
	slog.Info("scheduler listen", "me", group.Me(), "interval", cfg.LifecycleInterval.String())

	tick := time.NewTicker(sched.BeatEvery)
	defer tick.Stop()
	lastMembers := ""
	run := func() {
		if err := group.Beat(ctx); err != nil {
			slog.Warn("heartbeat", "err", err)
			return
		}
		members, err := group.Members(ctx)
		if err != nil {
			slog.Warn("members", "err", err)
			return
		}
		fp := sched.MembersFingerprint(members)
		if fp != lastMembers {
			slog.Info("scheduler members", "members", members)
			lastMembers = fp
		}
		items, err := rules.ListEnabled(ctx)
		if err != nil {
			slog.Warn("list rules", "err", err)
			return
		}
		slot, ttl := sched.Slot(time.Now().UTC(), cfg.LifecycleInterval)
		for _, rule := range items {
			if !group.Mine(rule.ID, members) {
				continue
			}
			id := sched.IDString(rule.ID)
			ok, err := group.Claim(ctx, slot, queue.TaskLifecycleRule, id, ttl)
			if err != nil {
				slog.Warn("claim", "rule", rule.ID, "err", err)
				continue
			}
			if !ok {
				continue
			}
			payload, err := json.Marshal(map[string]int64{"rule_id": rule.ID})
			if err != nil {
				group.DropClaim(ctx, slot, queue.TaskLifecycleRule, id)
				continue
			}
			if _, err := client.EnqueueContext(ctx, asynq.NewTask(queue.TaskLifecycleRule, payload),
				asynq.TaskID(sched.ClaimKey(slot, queue.TaskLifecycleRule, id)), asynq.MaxRetry(3), asynq.Timeout(2*time.Hour)); err != nil {
				if !errors.Is(err, asynq.ErrTaskIDConflict) {
					group.DropClaim(ctx, slot, queue.TaskLifecycleRule, id)
					slog.Warn("enqueue", "rule", rule.ID, "err", err)
				}
			}
		}
	}
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			run()
		}
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
