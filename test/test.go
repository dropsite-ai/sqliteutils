package test

import (
	"context"
	"testing"

	"github.com/dropsite-ai/sqliteutils"
	"github.com/dropsite-ai/sqliteutils/pool"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Pool initializes an in-memory SQLite pool using dbpool.InitPool,
// This function should be called at the beginning of each sqlite test.
func Pool(ctx context.Context, t *testing.T, migration string, poolSize int) error {
	t.Helper()

	// Define the in-memory DSN for testing
	uri := "file::memory:?mode=memory&cache=shared"

	// Initialize the pool using dbpool.InitPool with the in-memory URI
	err := pool.InitPool(uri, poolSize)
	if err != nil {
		return sqliteutils.FailedToInitPoolError(err, uri)
	}

	if migration == "" {
		return nil
	}

	pool, err := pool.GetPool()
	if err != nil {
		return sqliteutils.FailedToGetPoolError(err)
	}

	conn, err := pool.Take(ctx)
	if err != nil {
		return sqliteutils.FailedToTakeConnectionFromPoolError(err)
	}
	defer pool.Put(conn)

	if err := sqlitex.ExecScript(conn, migration); err != nil {
		return sqliteutils.FailedToExecScriptError(err, migration)
	}

	return nil
}
