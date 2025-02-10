package exec

import (
	"context"
	"fmt"
	"io"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/dropsite-ai/sqliteutils/pool"
)

// CreateBlob inserts a new row into the specified table/column using zeroblob(size)
// and any extra columns you want to set. It returns the new rowID.
func CreateBlob(
	ctx context.Context,
	table string,
	column string,
	size int64,
	extraCols map[string]interface{},
) (int64, error) {
	p, err := pool.GetPool()
	if err != nil {
		return 0, err
	}
	conn, err := p.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer p.Put(conn)

	// Build INSERT statement.
	// e.g. INSERT INTO mytable (col, other) VALUES (zeroblob(:blob_size), :other)
	colNames := []string{column}
	colParams := []string{"zeroblob(:blob_size)"}
	paramMap := map[string]interface{}{
		":blob_size": size,
	}
	for k, v := range extraCols {
		colNames = append(colNames, k)
		colParams = append(colParams, fmt.Sprintf(":%s", k))
		paramMap[":"+k] = v
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(colParams, ", "),
	)
	if err = executeNoRows(conn, insertSQL, paramMap); err != nil {
		return 0, fmt.Errorf("failed to insert zeroblob row: %w", err)
	}

	// Get the rowID of the newly inserted row.
	var rowID int64
	err = sqlitex.Execute(conn, "SELECT last_insert_rowid() as id;", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			rowID = stmt.ColumnInt64(0)
			return nil
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get rowID: %w", err)
	}

	return rowID, nil
}

// WriteBlobChunk writes a single chunk to the blob identified by table, column and rowID.
// 'offset' is where this chunk should be written.
func WriteBlobChunk(
	ctx context.Context,
	table string,
	column string,
	rowID int64,
	offset int64,
	data []byte,
) error {
	p, err := pool.GetPool()
	if err != nil {
		return err
	}
	conn, err := p.Take(ctx)
	if err != nil {
		return err
	}
	defer p.Put(conn)

	blob, err := conn.OpenBlob("", table, column, rowID, true)
	if err != nil {
		return fmt.Errorf("open blob handle failed: %w", err)
	}
	defer blob.Close()

	// Seek to the correct offset before writing.
	if _, err = blob.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("blob seek error: %w", err)
	}
	n, err := blob.Write(data)
	if err != nil {
		return fmt.Errorf("blob write error: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("blob write error: wrote %d bytes, expected %d", n, len(data))
	}
	return nil
}

// StreamReadBlob reads data from the blob starting at the given offset.
// If length < 0 it reads to EOF.
func StreamReadBlob(
	ctx context.Context,
	table string,
	column string,
	rowID int64,
	offset int64,
	length int64, // < 0 means read entire blob from offset
	w io.Writer,
) (int64, error) {
	p, err := pool.GetPool()
	if err != nil {
		return 0, err
	}
	conn, err := p.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer p.Put(conn)

	blob, err := conn.OpenBlob("", table, column, rowID, false)
	if err != nil {
		return 0, fmt.Errorf("open blob handle failed: %w", err)
	}
	defer blob.Close()

	if offset > 0 {
		if _, err := blob.Seek(offset, io.SeekStart); err != nil {
			return 0, fmt.Errorf("seek failed: %w", err)
		}
	}
	if length < 0 {
		return io.Copy(w, blob)
	}
	return io.Copy(w, io.LimitReader(blob, length))
}

// executeNoRows is a helper that prepares and executes a statement that does not return any rows.
func executeNoRows(conn *sqlite.Conn, query string, params map[string]interface{}) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	bindParams(stmt, params)

	for {
		hasRow, stepErr := stmt.Step()
		if stepErr != nil {
			return stepErr
		}
		if !hasRow {
			break
		}
	}
	return nil
}
