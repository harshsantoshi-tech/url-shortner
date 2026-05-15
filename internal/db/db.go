package db

import (
	"fmt"
	"time"

	"github.com/harshsantoshi-tech/url-shortner/config"
	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql" // MySQL driver — blank import registers it
)

// Connect opens a MySQL connection pool using the provided config.
// It pings the DB to verify connectivity before returning.
// Call this once in main() and pass *sqlx.DB everywhere via DI.
func Connect(cfg *config.Config) (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("db: open failed: %w", err)
	}

	// Connection pool settings — mirrors what you used at Magicpin
	db.SetMaxOpenConns(cfg.DBMaxOpenConns)
	db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	db.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	// Verify the connection is alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db: ping failed (is MySQL running?): %w", err)
	}

	return db, nil
}

// MustConnect calls Connect and panics on error.
// Use only in main() where a DB failure is unrecoverable.
func MustConnect(cfg *config.Config) *sqlx.DB {
	db, err := Connect(cfg)
	if err != nil {
		panic(fmt.Sprintf("db: fatal connection error: %v", err))
	}
	return db
}

// URL represents a row in the urls table.
type URL struct {
	ID        int64      `db:"id"`
	ShortCode string     `db:"short_code"`
	LongURL   string     `db:"long_url"`
	UserID    *int64     `db:"user_id"`    // nullable
	CreatedAt time.Time  `db:"created_at"`
	ExpiresAt *time.Time `db:"expires_at"` // nullable — nil means no expiry
}

// ClickEvent represents a row in the click_events table.
type ClickEvent struct {
	ID        int64     `db:"id"`
	ShortCode string    `db:"short_code"`
	Referrer  *string   `db:"referrer"`   // nullable
	ClickedAt time.Time `db:"clicked_at"`
}