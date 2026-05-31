package db

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestOpen is an integration test: it connects to a live MariaDB, verifies the
// pool is usable, and confirms the migrated tables are present. It is skipped
// when DB_DSN is unset so `make test` stays green without a database.
//
// Run it against the compose stack from the host (note 127.0.0.1, not the
// in-network "mariadb" hostname):
//
//	DB_DSN="root:secret@tcp(127.0.0.1:3306)/supplychain" go test ./internal/db/ -v
func TestOpen(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip("DB_DSN not set; skipping MariaDB integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	// Connection is alive.
	var got int
	if err := pool.QueryRowContext(ctx, "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if got != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", got)
	}

	// Migrations ran: the two tables exist.
	for _, table := range []string{"orders", "outbox"} {
		var name string
		err := pool.QueryRowContext(ctx,
			"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
			table,
		).Scan(&name)
		if err != nil {
			t.Fatalf("table %q not found (did migrations run?): %v", table, err)
		}
	}
}
