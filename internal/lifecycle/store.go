package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Rule struct {
	ID                        int64
	BucketID                  int64
	BucketName                string
	TenantID                  int64
	Prefix                    string
	Enabled                   bool
	DeleteAfterDays           *int
	CleanupTrashAfterDays     *int
	CleanupVersionsAfterDays  *int
	CleanupMultipartAfterDays *int
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const cols = `r.id, r.bucket_id, b.bucket_name, COALESCE(g.account_id, 0), r.prefix, r.enabled,
	r.delete_after_days, r.cleanup_trash_after_days, r.cleanup_versions_after_days, r.cleanup_multipart_after_days,
	r.created_at, r.updated_at`

const fromJoin = `FROM lifecycle_rules r
	JOIN platform_buckets b ON b.id = r.bucket_id
	LEFT JOIN LATERAL (
		SELECT account_id FROM account_bucket_grants WHERE bucket_id = r.bucket_id ORDER BY account_id LIMIT 1
	) g ON true`

func scan(row interface{ Scan(dest ...any) error }) (Rule, error) {
	var u Rule
	err := row.Scan(
		&u.ID, &u.BucketID, &u.BucketName, &u.TenantID, &u.Prefix, &u.Enabled,
		&u.DeleteAfterDays, &u.CleanupTrashAfterDays, &u.CleanupVersionsAfterDays, &u.CleanupMultipartAfterDays,
		&u.CreatedAt, &u.UpdatedAt,
	)
	return u, err
}

func (s *Store) List(ctx context.Context, bucketID, tenantID *int64) ([]Rule, error) {
	q := `SELECT ` + cols + ` ` + fromJoin
	args := []any{}
	n := 1
	if bucketID != nil {
		q += fmt.Sprintf(` WHERE r.bucket_id = $%d`, n)
		args = append(args, *bucketID)
		n++
	}
	if tenantID != nil {
		if n == 1 {
			q += ` WHERE `
		} else {
			q += ` AND `
		}
		q += fmt.Sprintf(`EXISTS (
			SELECT 1 FROM account_bucket_grants g2 WHERE g2.bucket_id = r.bucket_id AND g2.account_id = $%d)`, n)
		args = append(args, *tenantID)
	}
	q += ` ORDER BY r.id`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list lifecycle: %w", err)
	}
	defer rows.Close()
	var out []Rule
	for rows.Next() {
		u, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Store) ListEnabled(ctx context.Context) ([]Rule, error) {
	enabled := true
	// reuse List with no filters then keep enabled — cheaper than another join shape
	items, err := s.List(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	out := make([]Rule, 0, len(items))
	for _, r := range items {
		if r.Enabled == enabled {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *Store) GetByID(ctx context.Context, id int64) (*Rule, error) {
	u, err := scan(s.pool.QueryRow(ctx, `SELECT `+cols+` `+fromJoin+` WHERE r.id = $1`, id))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get lifecycle: %w", err)
	}
	return &u, nil
}

func (s *Store) Insert(ctx context.Context, bucketID int64, prefix string, enabled bool, deleteDays, trashDays, versionDays, multipartDays *int, at time.Time) (*Rule, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO lifecycle_rules (
			bucket_id, prefix, enabled, delete_after_days, cleanup_trash_after_days,
			cleanup_versions_after_days, cleanup_multipart_after_days, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8) RETURNING id`,
		bucketID, prefix, enabled, deleteDays, trashDays, versionDays, multipartDays, at).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("insert lifecycle: %w", err)
	}
	return s.GetByID(ctx, id)
}

func (s *Store) Update(ctx context.Context, u *Rule) (*Rule, error) {
	_, err := s.pool.Exec(ctx, `
		UPDATE lifecycle_rules SET
			prefix = $2, enabled = $3, delete_after_days = $4, cleanup_trash_after_days = $5,
			cleanup_versions_after_days = $6, cleanup_multipart_after_days = $7, updated_at = $8
		WHERE id = $1`,
		u.ID, u.Prefix, u.Enabled, u.DeleteAfterDays, u.CleanupTrashAfterDays,
		u.CleanupVersionsAfterDays, u.CleanupMultipartAfterDays, u.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("update lifecycle: %w", err)
	}
	return s.GetByID(ctx, u.ID)
}

func (s *Store) Delete(ctx context.Context, id int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM lifecycle_rules WHERE id = $1`, id)
	if err != nil {
		return false, fmt.Errorf("delete lifecycle: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
