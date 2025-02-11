package pool

import (
	"sync"

	"github.com/dropsite-ai/sqliteutils"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var (
	poolUri  string
	pool     *sqlitex.Pool
	poolLock sync.Mutex
)

// InitPool initializes the global pool with the given directory.
// It should be called once during application startup.
func InitPool(uri string, poolSize int) error {
	poolLock.Lock()
	defer poolLock.Unlock()
	return initPoolUnlocked(uri, poolSize)
}

// ClosePool safely closes the global pool.
// It should be called during application shutdown.
func ClosePool() error {
	poolLock.Lock()
	defer poolLock.Unlock()
	return closePoolUnlocked()
}

// GetPool returns the initialized global pool.
// Returns an error if the pool is not initialized.
func GetPool() (*sqlitex.Pool, error) {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pool == nil {
		return nil, sqliteutils.ErrPoolNotInitialized
	}
	return pool, nil
}

// GetPoolUri returns the path to the system database.
func GetPoolUri() string {
	poolLock.Lock()
	defer poolLock.Unlock()
	return poolUri
}

// ResetPool safely closes the current pool and re-initializes it with the existing poolUri.
// This can be useful for reloading configurations.
func ResetPool(poolSize int) error {
	poolLock.Lock()
	defer poolLock.Unlock()

	if err := closePoolUnlocked(); err != nil {
		return err
	}

	return initPoolUnlocked(poolUri, poolSize)
}

// SetPool allows injecting an existing *sqlitex.Pool into the dbpool.
// This is primarily intended for testing purposes.
// It closes any existing pool before setting the new one.
func SetPool(newPool *sqlitex.Pool) error {
	poolLock.Lock()
	defer poolLock.Unlock()

	if pool != nil {
		if err := pool.Close(); err != nil {
			return sqliteutils.FailedToClosePoolError(err)
		}
	}

	pool = newPool
	poolUri = ""

	return nil
}

// initPoolUnlocked initializes the pool without locking.
// Assumes that the caller holds the poolLock.
func initPoolUnlocked(uri string, poolSize int) error {
	if pool != nil {
		return nil // Pool already initialized
	}

	poolUri = uri

	var err error
	pool, err = sqlitex.NewPool(uri, sqlitex.PoolOptions{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL | sqlite.OpenURI,
		PoolSize: poolSize,
		PrepareConn: func(conn *sqlite.Conn) error {
			// Enable foreign keys for this connection
			if err = sqlitex.Execute(conn, "PRAGMA foreign_keys = ON;", nil); err != nil {
				return sqliteutils.FailedToEnableForeignKeysError(err)
			}
			// Enable trusted schema
			if err = sqlitex.Execute(conn, "PRAGMA trusted_schema=1;", nil); err != nil {
				return sqliteutils.FailedToEnableForeignKeysError(err)
			}
			// Create reverse UDF
			return conn.CreateFunction("reverse", &sqlite.FunctionImpl{
				NArgs:         1,
				Deterministic: true,
				Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
					input := args[0].Text()
					runes := []rune(input)
					for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
						runes[i], runes[j] = runes[j], runes[i]
					}
					return sqlite.TextValue(string(runes)), nil
				},
			})
		},
	})
	if err != nil {
		return sqliteutils.FailedToInitPoolError(err, poolUri)
	}

	return nil
}

// closePoolUnlocked closes the pool without locking.
// Assumes that the caller holds the poolLock.
func closePoolUnlocked() error {
	if pool == nil {
		return sqliteutils.ErrPoolNotInitialized
	}

	err := pool.Close()
	if err != nil {
		return sqliteutils.FailedToClosePoolError(err)
	}
	pool = nil
	poolUri = ""
	return nil
}
