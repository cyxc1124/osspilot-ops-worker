package lifecycle

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/cyxc1124/osspilot-ops-worker/internal/rgw"
)

const maxDeletesPerAction = 2000

type Runner struct {
	Rules *Store
	S3    *rgw.Client
	After func(ctx context.Context, bucket string)
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Rules == nil || r.S3 == nil {
		return nil
	}
	rules, err := r.Rules.ListEnabled(ctx)
	if err != nil {
		return err
	}
	var objects, trash, versions, multipart int
	for _, rule := range rules {
		o, t, v, m, _ := r.runRule(ctx, rule)
		objects += o
		trash += t
		versions += v
		multipart += m
	}
	slog.Info("lifecycle run done", "rules", len(rules), "objects", objects, "trash", trash, "versions", versions, "multipart", multipart)
	return nil
}

func (r *Runner) RunRule(ctx context.Context, rule Rule) error {
	objects, trash, versions, multipart, err := r.runRule(ctx, rule)
	slog.Info("lifecycle rule done", "id", rule.ID, "bucket", rule.BucketName, "objects", objects, "trash", trash, "versions", versions, "multipart", multipart)
	return err
}

func (r *Runner) runRule(ctx context.Context, rule Rule) (objects, trash, versions, multipart int, err error) {
	touched := 0
	mark := func(n int) {
		if n > 0 {
			touched += n
		}
	}
	if rule.DeleteAfterDays != nil && *rule.DeleteAfterDays > 0 {
		n, e := r.deletePrefix(ctx, rule.BucketName, rule.Prefix, *rule.DeleteAfterDays, liveOnly)
		if e != nil {
			slog.Warn("lifecycle delete", "bucket", rule.BucketName, "err", e)
			err = e
		}
		objects += n
		mark(n)
	}
	if rule.CleanupTrashAfterDays != nil && *rule.CleanupTrashAfterDays > 0 {
		n, e := r.deletePrefix(ctx, rule.BucketName, joinPrefix(".trash/", rule.Prefix), *rule.CleanupTrashAfterDays, anyKey)
		if e != nil {
			slog.Warn("lifecycle trash", "bucket", rule.BucketName, "err", e)
			err = e
		}
		trash += n
		mark(n)
	}
	if rule.CleanupVersionsAfterDays != nil && *rule.CleanupVersionsAfterDays > 0 {
		n, e := r.deletePrefix(ctx, rule.BucketName, joinPrefix(".versions/", rule.Prefix), *rule.CleanupVersionsAfterDays, anyKey)
		if e != nil {
			slog.Warn("lifecycle versions", "bucket", rule.BucketName, "err", e)
			err = e
		}
		versions += n
		mark(n)
	}
	if rule.CleanupMultipartAfterDays != nil && *rule.CleanupMultipartAfterDays > 0 {
		n, e := r.abortMultipart(ctx, rule.BucketName, rule.Prefix, *rule.CleanupMultipartAfterDays)
		if e != nil {
			slog.Warn("lifecycle multipart", "bucket", rule.BucketName, "err", e)
			err = e
		}
		multipart += n
	}
	if touched > 0 && r.After != nil {
		r.After(ctx, rule.BucketName)
	}
	return
}

type keyFilter func(key string) bool

func liveOnly(key string) bool {
	return !strings.HasPrefix(key, ".trash/") && !strings.HasPrefix(key, ".versions/")
}

func anyKey(string) bool { return true }

func joinPrefix(base, prefix string) string {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix == "" {
		return base
	}
	return base + prefix
}

func (r *Runner) deletePrefix(ctx context.Context, bucket, prefix string, days int, keep keyFilter) (int, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	token := ""
	deleted := 0
	for deleted < maxDeletesPerAction {
		items, next, truncated, err := r.S3.ListObjects(ctx, bucket, prefix, token, 1000)
		if err != nil {
			return deleted, err
		}
		for _, obj := range items {
			if deleted >= maxDeletesPerAction {
				break
			}
			if !keep(obj.Key) || obj.LastModified == nil || !obj.LastModified.Before(cutoff) {
				continue
			}
			if err := r.S3.DeleteObject(ctx, bucket, obj.Key); err != nil {
				slog.Warn("lifecycle delete object", "bucket", bucket, "key", obj.Key, "err", err)
				continue
			}
			deleted++
		}
		if !truncated || next == "" || deleted >= maxDeletesPerAction {
			break
		}
		token = next
	}
	return deleted, nil
}

func (r *Runner) abortMultipart(ctx context.Context, bucket, prefix string, days int) (int, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	keyMarker, uploadMarker := "", ""
	aborted := 0
	for aborted < maxDeletesPerAction {
		items, nextKey, nextUpload, truncated, err := r.S3.ListMultipart(ctx, bucket, prefix, keyMarker, uploadMarker)
		if err != nil {
			return aborted, err
		}
		for _, u := range items {
			if aborted >= maxDeletesPerAction {
				break
			}
			if u.Initiated == nil || !u.Initiated.Before(cutoff) {
				continue
			}
			if err := r.S3.AbortMultipart(ctx, bucket, u.Key, u.UploadID); err != nil {
				slog.Warn("lifecycle abort multipart", "bucket", bucket, "key", u.Key, "err", err)
				continue
			}
			aborted++
		}
		if !truncated || aborted >= maxDeletesPerAction {
			break
		}
		keyMarker, uploadMarker = nextKey, nextUpload
	}
	return aborted, nil
}
