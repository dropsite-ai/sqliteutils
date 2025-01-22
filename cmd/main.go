package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dropsite-ai/sqliteutils/exec"
	"github.com/dropsite-ai/sqliteutils/pool"
)

func main() {
	// Define and parse flags
	dbPath := flag.String("db-path", "sqlite.db", "Path to the SQLite database file")
	poolSize := flag.Int("pool-size", 4, "Number of connections in the pool")
	query := flag.String("query", "", "SQL query to execute")
	flag.Parse()

	// Validate inputs
	if *query == "" {
		fmt.Println("Error: --query flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize the database pool
	err := pool.InitPool(*dbPath, *poolSize)
	if err != nil {
		fmt.Printf("Failed to initialize database pool: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err = pool.ClosePool(); err != nil {
			fmt.Printf("Failed to close database pool: %v\n", err)
		}
	}()

	// Execute the query
	ctx := context.Background()
	err = exec.Exec(ctx, *query, nil, func(index int, row map[string]interface{}) {
		fmt.Printf("Result %d: %+v\n", index, row)
	})
	if err != nil {
		fmt.Printf("Failed to execute query: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Query executed successfully")
}
