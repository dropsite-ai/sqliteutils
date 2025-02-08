package exec_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dropsite-ai/sqliteutils/exec"
	"github.com/dropsite-ai/sqliteutils/pool"
	"github.com/dropsite-ai/sqliteutils/test"
)

func TestStreamInsertAndReadBlob(t *testing.T) {
	ctx := context.Background()
	// Create a table with a blob column and one extra text column.
	const migration = `
		CREATE TABLE test_blob (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB,
			extra TEXT
		);
	`
	if err := test.Pool(ctx, t, migration, 1); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	defer func() {
		if err := pool.ClosePool(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	// Prepare the blob content and extra column value.
	content := "Hello, Blob!"
	size := int64(len(content))
	reader := strings.NewReader(content)
	extra := map[string]interface{}{
		"extra": "metadata",
	}

	// Insert the blob into the table.
	rowID, err := exec.StreamInsertBlob(ctx, "test_blob", "data", size, reader, extra)
	if err != nil {
		t.Fatalf("StreamInsertBlob failed: %v", err)
	}
	if rowID <= 0 {
		t.Fatalf("expected valid rowID, got %d", rowID)
	}

	// Read back the entire blob.
	var buf bytes.Buffer
	n, err := exec.StreamReadBlob(ctx, "test_blob", "data", rowID, 0, -1, &buf)
	if err != nil {
		t.Fatalf("StreamReadBlob failed: %v", err)
	}
	if n != size {
		t.Errorf("expected to read %d bytes, got %d", size, n)
	}
	if buf.String() != content {
		t.Errorf("blob content mismatch: expected %q, got %q", content, buf.String())
	}
}

func TestStreamInsertBlob_SizeMismatch(t *testing.T) {
	ctx := context.Background()
	// Create a table with only a blob column.
	const migration = `
		CREATE TABLE test_blob (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB
		);
	`
	if err := test.Pool(ctx, t, migration, 1); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	defer func() {
		if err := pool.ClosePool(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	// Provide a reader whose byte count is less than the declared size.
	content := "short"
	declaredSize := int64(len(content) + 5) // Intentionally incorrect
	reader := strings.NewReader(content)

	_, err := exec.StreamInsertBlob(ctx, "test_blob", "data", declaredSize, reader, nil)
	if err == nil {
		t.Fatal("expected error due to size mismatch, but got none")
	}
}

func TestStreamReadBlob_OffsetAndLength(t *testing.T) {
	ctx := context.Background()
	// Create a simple table with a blob column.
	const migration = `
		CREATE TABLE test_blob (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB
		);
	`
	if err := test.Pool(ctx, t, migration, 1); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	defer func() {
		if err := pool.ClosePool(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	// Insert blob data.
	content := "Hello, Blob!"
	size := int64(len(content))
	reader := strings.NewReader(content)
	rowID, err := exec.StreamInsertBlob(ctx, "test_blob", "data", size, reader, nil)
	if err != nil {
		t.Fatalf("StreamInsertBlob failed: %v", err)
	}

	// Read a subset of the blob: from offset 7, read 4 bytes (expecting "Blob").
	var buf bytes.Buffer
	n, err := exec.StreamReadBlob(ctx, "test_blob", "data", rowID, 7, 4, &buf)
	if err != nil {
		t.Fatalf("StreamReadBlob failed: %v", err)
	}
	if n != 4 {
		t.Errorf("expected to read 4 bytes, got %d", n)
	}
	expected := "Blob"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestStreamReadBlob_InvalidRow(t *testing.T) {
	ctx := context.Background()
	// Create a table with a blob column.
	const migration = `
		CREATE TABLE test_blob (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB
		);
	`
	if err := test.Pool(ctx, t, migration, 1); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	defer func() {
		if err := pool.ClosePool(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	// Attempt to read from a row that does not exist.
	var buf bytes.Buffer
	_, err := exec.StreamReadBlob(ctx, "test_blob", "data", 999, 0, -1, &buf)
	if err == nil {
		t.Fatal("expected error when reading blob from non-existent row, but got none")
	}
}
