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
	dbPath := flag.String("dbpath", "sqlite.db", "Path to the SQLite database file")
	poolSize := flag.Int("poolsize", 4, "Number of connections in the pool")
	query := flag.String("query", "SELECT sqlite_version();", "SQL query to execute")
	flag.Parse()

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

	// Prepare the queries and parameters as slices
	queries := []string{*query}
	params := []map[string]interface{}{nil} // No parameters for the default query

	// Execute the query
	ctx := context.Background()
	err = exec.Exec(ctx, queries, params, func(index int, row map[string]interface{}) {
		fmt.Printf("Result %d: %+v\n", index+1, row)
	})
	if err != nil {
		fmt.Printf("Failed to execute query: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Query executed successfully")
}
