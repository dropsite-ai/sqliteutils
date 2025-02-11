package pool_test

import (
	"context"
	"testing"

	"github.com/dropsite-ai/sqliteutils/pool"
)

func TestReverseUDF(t *testing.T) {
	ctx := context.Background()
	// Use an in-memory database so tests are fast and isolated.
	uri := "file::memory:?mode=memory&cache=shared"
	if err := pool.InitPool(uri, 1); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	defer func() {
		if err := pool.ClosePool(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	// Get a connection from the pool.
	p, err := pool.GetPool()
	if err != nil {
		t.Fatalf("failed to get pool: %v", err)
	}
	conn, err := p.Take(ctx)
	if err != nil {
		t.Fatalf("failed to take connection: %v", err)
	}
	defer p.Put(conn)

	// Prepare a statement that uses the reverse UDF.
	stmt, err := conn.Prepare("SELECT reverse(?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Finalize()

	// Define some test cases.
	testCases := []struct {
		input    string
		expected string
	}{
		{"hello", "olleh"},
		{"", ""},
		{"GoLang", "gnaLoG"},
		{"🙂🙃", "🙃🙂"},
	}

	for _, tc := range testCases {
		// Reset the statement between runs.
		if err := stmt.Reset(); err != nil {
			t.Fatalf("failed to reset statement: %v", err)
		}
		// Bind the input value.
		stmt.BindText(1, tc.input)
		// Execute the statement.
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatalf("failed to step statement: %v", err)
		}
		if !hasRow {
			t.Fatalf("expected a row for input %q", tc.input)
		}
		// Read and check the result.
		result := stmt.ColumnText(0)
		if result != tc.expected {
			t.Errorf("reverse(%q) = %q; want %q", tc.input, result, tc.expected)
		}
		// Make sure no extra rows are returned.
		hasRow, err = stmt.Step()
		if err != nil {
			t.Fatalf("failed to step to next row: %v", err)
		}
		if hasRow {
			t.Errorf("expected only one row for input %q", tc.input)
		}
	}
}
