package shortner
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// ErrNotFound is returned when a short code doesn't exist in the DB.
var ErrNotFound = errors.New("short code not found")

// URL mirrors the urls table row.
type URL struct {
	ID        int64      `db:"id"`
	ShortCode string     `db:"short_code"`
	LongURL   string     `db:"long_url"`
	CreatedAt time.Time  `db:"created_at"`
	ExpiresAt *time.Time `db:"expires_at"`
}

// Repository handles all MySQL queries for the shortener.
// No business logic here — just raw DB operations.
type Repository struct {
	db *sqlx.DB
}

// NewRepository creates a new Repository.
func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

// Save inserts a new short URL into the urls table.
// Returns error on duplicate short_code (caller should retry with a new code).
func (r *Repository) Save(ctx context.Context, shortCode, longURL string) error {
	query := `
		INSERT INTO urls (short_code, long_url, created_at)
		VALUES (?, ?, NOW())
	`
	_, err := r.db.ExecContext(ctx, query, shortCode, longURL)
	if err != nil {
		return fmt.Errorf("repository: save failed: %w", err)
	}
	return nil
}

// GetByShortCode fetches the full URL row for a given short code.
// Returns ErrNotFound if the code doesn't exist or has expired.
func (r *Repository) GetByShortCode(ctx context.Context, shortCode string) (*URL, error) {
	var u URL
	query := `
		SELECT id, short_code, long_url, created_at, expires_at
		FROM urls
		WHERE short_code = ?
		  AND (expires_at IS NULL OR expires_at > NOW())
		LIMIT 1
	`
	err := r.db.GetContext(ctx, &u, query, shortCode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("repository: get failed: %w", err)
	}
	return &u, nil
}

// Exists checks if a short code is already taken.
// Used for collision detection during code generation.
func (r *Repository) Exists(ctx context.Context, shortCode string) (bool, error) {
	var count int
	query := `SELECT COUNT(1) FROM urls WHERE short_code = ?`
	err := r.db.GetContext(ctx, &count, query, shortCode)
	if err != nil {
		return false, fmt.Errorf("repository: exists check failed: %w", err)
	}
	return count > 0, nil
}