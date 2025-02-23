# sqliteutils

Pooling, querying, backup, and testing utilities for [zombiezen/go-sqlite](/zombiezen/go-sqlite).  

## Installation

### Go Package

```bash
go get github.com/dropsite-ai/sqliteutils
```

### Homebrew (macOS or Compatible)

```bash
brew tap dropsite-ai/homebrew-tap
brew install sqliteutils
```

### Download Binaries

Grab the latest pre-built binaries from the [GitHub Releases](https://github.com/dropsite-ai/sqliteutils/releases). Extract them, then run the `sqliteutils` executable directly.

### Build from Source

1. **Clone the repository**:
   ```bash
   git clone https://github.com/dropsite-ai/sqliteutils.git
   cd sqliteutils
   ```
2. **Build using Go**:
   ```bash
   go build -o sqliteutils cmd/main.go
   ```

## Usage

### Command-Line Usage

```bash
  -dbpath string
    	Path to the SQLite database file (default "sqlite.db")
  -poolsize int
    	Number of connections in the pool (default 4)
  -query string
    	SQL query to execute (default "SELECT sqlite_version();")
```

### Programmatic Usage

Below are some examples demonstrating how to use each package directly in your Go code.

#### Using the Pool Package

The `pool` package handles initializing, retrieving, and closing the SQLite connection pool. This is the foundation for all other operations.

```go
package main

import (
	"fmt"
	"github.com/dropsite-ai/sqliteutils/pool"
)

func main() {
	// Initialize the connection pool.
	if err := pool.InitPool("sqlite.db", 4); err != nil {
		fmt.Println("Error initializing pool:", err)
		return
	}
	defer pool.ClosePool()

	// Retrieve and print the current pool URI.
	fmt.Println("Connected to database:", pool.GetPoolUri())
}
```

#### Executing SQL Queries with the Exec Package

The `exec` package makes executing and processing SQL queries simple—whether single statements, multiple statements, or transactions.

```go
package main

import (
	"context"
	"fmt"
	"github.com/dropsite-ai/sqliteutils/exec"
	"github.com/dropsite-ai/sqliteutils/pool"
)

func main() {
	// Initialize the pool.
	if err := pool.InitPool("sqlite.db", 4); err != nil {
		fmt.Println("Pool initialization error:", err)
		return
	}
	defer pool.ClosePool()

	// Execute a simple query.
	ctx := context.Background()
	query := "SELECT sqlite_version();"
	err := exec.Exec(ctx, query, nil, func(i int, row map[string]interface{}) {
		fmt.Printf("Row %d: %+v\n", i, row)
	})
	if err != nil {
		fmt.Println("Query execution error:", err)
	}
}
```

#### Performing Database Backups with the Backup Package

Use the `backup` package to create a backup of your database. It handles opening both source and destination databases and performs the backup with error handling.

```go
package main

import (
	"fmt"
	"github.com/dropsite-ai/sqliteutils/backup"
)

func main() {
	// Backup from source.db to backup.db.
	if err := backup.BackupDatabase("source.db", "backup.db"); err != nil {
		fmt.Println("Backup error:", err)
		return
	}
	fmt.Println("Database backup successful.")
}
```

#### Testing with the Test Package

For testing, the `test` package provides a helper to initialize an in-memory SQLite pool with your schema migrations.

```go
package mytest

import (
	"context"
	"testing"

	"github.com/dropsite-ai/sqliteutils/test"
)

func TestDatabaseSetup(t *testing.T) {
	ctx := context.Background()
	migration := `
		CREATE TABLE example (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
	`
	// Initialize the in-memory test pool.
	if err := test.Pool(ctx, t, migration, 1); err != nil {
		t.Fatalf("Failed to initialize test pool: %v", err)
	}
	// Additional test code can follow here...
}
```

## Test

```bash
make test
```

## Release

```bash
make release
```