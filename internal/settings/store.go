package settings

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type Fallbacks struct {
	S3Endpoint        string
	RGWAccessKey      string
	RGWSecretKey      string
	DownloadCDNURL    string
	PreviewCDNURL     string
	ObjectHTTPDomain  string
	ObjectHTTPSDomain string
	OfficeURL         string
	CephMgmtAPIURL    string
}

type row struct {
	Value     string
	UpdatedAt time.Time
}

func (s *Store) Load(ctx context.Context) (map[string]row, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, value, updated_at FROM system_settings`)
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()
	out := map[string]row{}
	for rows.Next() {
		var k string
		var r row
		if err := rows.Scan(&k, &r.Value, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out[k] = r
	}
	return out, rows.Err()
}

func (s *Store) Upsert(ctx context.Context, key, value string, secret bool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO system_settings (key, value, is_secret, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, is_secret = EXCLUDED.is_secret, updated_at = now()`,
		key, value, secret)
	if err != nil {
		return fmt.Errorf("upsert setting %s: %w", key, err)
	}
	return nil
}

func pick(rows map[string]row, key, fallback string) string {
	if r, ok := rows[key]; ok {
		if v := strings.TrimSpace(r.Value); v != "" {
			return v
		}
	}
	return fallback
}

func pickPtr(rows map[string]row, key, fallback string) *string {
	v := pick(rows, key, fallback)
	if v == "" {
		return nil
	}
	return &v
}

func pickInt(rows map[string]row, key string, fallback int) int {
	r, ok := rows[key]
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(r.Value))
	if err != nil {
		return fallback
	}
	return n
}

func pickInt64(rows map[string]row, key string, fallback int64) int64 {
	r, ok := rows[key]
	if !ok {
		return fallback
	}
	n, err := strconv.ParseInt(strings.TrimSpace(r.Value), 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func pickBool(rows map[string]row, key string, fallback bool) bool {
	r, ok := rows[key]
	if !ok {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(r.Value)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
}

func latest(rows map[string]row) *time.Time {
	var t *time.Time
	for _, r := range rows {
		u := r.UpdatedAt
		if t == nil || u.After(*t) {
			cp := u
			t = &cp
		}
	}
	return t
}
