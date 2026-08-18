package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	RedisURL          string
	S3Endpoint        string
	RGWAccessKey      string
	RGWSecretKey      string
	TenantAPIURL      string
	ProjectionSecret  string
	LifecycleInterval time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		RedisURL:          os.Getenv("REDIS_URL"),
		S3Endpoint:        os.Getenv("S3_ENDPOINT"),
		RGWAccessKey:      os.Getenv("RGW_ACCESS_KEY"),
		RGWSecretKey:      os.Getenv("RGW_SECRET_KEY"),
		TenantAPIURL:      os.Getenv("TENANT_API_URL"),
		ProjectionSecret:  os.Getenv("PROJECTION_SECRET"),
		LifecycleInterval: getenvDuration("LIFECYCLE_INTERVAL", time.Hour),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < time.Minute {
		return fallback
	}
	return d
}
