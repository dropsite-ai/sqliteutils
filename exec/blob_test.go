package exec_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dropsite-ai/sqliteutils/exec"
	"github.com/dropsite-ai/sqliteutils/pool"
	"github.com/dropsite-ai/sqliteutils/test"
)

// TestCreateAndReadBlob creates a blob row with an extra column,
// writes the entire content as a single chunk, and then reads it back.
func TestCreateAndReadBlob(t *testing.T) {
	ctx := context.Background()
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

	content := "Hello, Blob!"
	size := int64(len(content))
	extraCols := map[string]interface{}{
		"extra": "metadata",
	}

	// Create a blob row with a declared size.
	rowID, err := exec.CreateBlob(ctx, "test_blob", "data", size, extraCols)
	if err != nil {
		t.Fatalf("CreateBlob failed: %v", err)
	}
	if rowID <= 0 {
		t.Fatalf("expected valid rowID, got %d", rowID)
	}

	// Write the entire content in one chunk (offset 0).
	if err = exec.WriteBlobChunk(ctx, "test_blob", "data", rowID, 0, []byte(content)); err != nil {
		t.Fatalf("WriteBlobChunk failed: %v", err)
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

// TestIncompleteWrite creates a blob with a declared size larger than the data written.
// When reading back, the unwritten portion should contain zero bytes.
func TestIncompleteWrite(t *testing.T) {
	ctx := context.Background()
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

	content := "short"
	declaredSize := int64(len(content) + 5) // declare more than provided
	rowID, err := exec.CreateBlob(ctx, "test_blob", "data", declaredSize, nil)
	if err != nil {
		t.Fatalf("CreateBlob failed: %v", err)
	}

	// Write only the available content.
	if err = exec.WriteBlobChunk(ctx, "test_blob", "data", rowID, 0, []byte(content)); err != nil {
		t.Fatalf("WriteBlobChunk failed: %v", err)
	}

	// Read back the entire blob.
	var buf bytes.Buffer
	n, err := exec.StreamReadBlob(ctx, "test_blob", "data", rowID, 0, -1, &buf)
	if err != nil {
		t.Fatalf("StreamReadBlob failed: %v", err)
	}
	if n != declaredSize {
		t.Errorf("expected to read %d bytes, got %d", declaredSize, n)
	}
	data := buf.Bytes()
	// The first part should match the written content.
	if string(data[:len(content)]) != content {
		t.Errorf("expected first %d bytes to be %q, got %q", len(content), content, string(data[:len(content)]))
	}
	// The remainder should be zero bytes.
	zeros := make([]byte, declaredSize-int64(len(content)))
	if !bytes.Equal(data[len(content):], zeros) {
		t.Errorf("expected remaining bytes to be zero, got %v", data[len(content):])
	}
}

// TestStreamReadBlob_OffsetAndLength writes a blob and then reads a subset of its data.
func TestStreamReadBlob_OffsetAndLength(t *testing.T) {
	ctx := context.Background()
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

	content := "Hello, Blob!"
	size := int64(len(content))
	rowID, err := exec.CreateBlob(ctx, "test_blob", "data", size, nil)
	if err != nil {
		t.Fatalf("CreateBlob failed: %v", err)
	}
	if err = exec.WriteBlobChunk(ctx, "test_blob", "data", rowID, 0, []byte(content)); err != nil {
		t.Fatalf("WriteBlobChunk failed: %v", err)
	}

	// Read a subset of the blob: starting at offset 7, read 4 bytes (expecting "Blob").
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

// TestStreamReadBlob_InvalidRow attempts to read from a non-existent row.
func TestStreamReadBlob_InvalidRow(t *testing.T) {
	ctx := context.Background()
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

	var buf bytes.Buffer
	// Attempt to read from a non-existent row (e.g. rowID 999).
	_, err := exec.StreamReadBlob(ctx, "test_blob", "data", 999, 0, -1, &buf)
	if err == nil {
		t.Fatal("expected error when reading blob from non-existent row, but got none")
	}
}
