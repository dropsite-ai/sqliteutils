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

// StreamInsertBlob inserts a new row with zeroblob(size) into the given table/column,
// plus any extra columns you provide in extraCols. Then it streams data from 'r' directly
// into that blob, all within a single transaction. If anything fails, the transaction
// is rolled back.
//
// The function returns the newly inserted row ID.
func StreamInsertBlob(
	ctx context.Context,
	table string,
	column string,
	size int64, // must match the total number of bytes in 'r'
	r io.Reader, // source of blob data
	extraCols map[string]interface{},
) (int64, error) {
	// 1) Acquire a connection from the pool
	p, err := pool.GetPool()
	if err != nil {
		return 0, err
	}
	conn, err := p.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer p.Put(conn)

	// 2) Begin transaction
	if err = beginTx(conn); err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = rollbackTx(conn)
		}
	}()

	// 3) Build and execute INSERT statement with zeroblob(:size) plus extra cols
	//    e.g. INSERT INTO table (column, colA, colB) VALUES (zeroblob(:size), :colA, :colB)
	colNames := []string{column}
	colParams := []string{"zeroblob(:blob_size)"}
	paramMap := map[string]interface{}{
		":blob_size": size,
	}
	// If we have extraCols, append them to the INSERT
	for k := range extraCols {
		colNames = append(colNames, k)
		colParams = append(colParams, fmt.Sprintf(":%s", k))
		paramMap[":"+k] = extraCols[k]
	}
	insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		table,
		strings.Join(colNames, ", "),
		strings.Join(colParams, ", "),
	)

	if err = executeNoRows(conn, insertSQL, paramMap); err != nil {
		return 0, fmt.Errorf("failed to insert zeroblob row: %w", err)
	}

	// 4) Get last_insert_rowid()
	var rowID int64
	err = sqlitex.Execute(conn, `SELECT last_insert_rowid() as id;`, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			rowID = stmt.ColumnInt64(0)
			return nil
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get rowID: %w", err)
	}

	// 5) Open the blob handle for writing
	blob, err := conn.OpenBlob("", table, column, rowID, true)
	if err != nil {
		return 0, fmt.Errorf("open blob handle failed: %w", err)
	}
	defer blob.Close()

	// 6) Copy from r -> blob
	written, copyErr := io.Copy(blob, r)
	if copyErr != nil {
		return 0, fmt.Errorf("failed to copy into blob: %w", copyErr)
	}
	if written != size {
		return 0, fmt.Errorf("only wrote %d bytes, expected %d", written, size)
	}

	// Explicitly close the blob handle before committing.
	if err := blob.Close(); err != nil {
		return 0, fmt.Errorf("failed to close blob handle: %w", err)
	}

	// 7) Commit
	if err := commitTx(conn); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	committed = true

	return rowID, nil
}

// StreamReadBlob opens the specified blob (table, column, rowID) in read-only mode,
// then streams from the blob to 'w'. If length < 0, the entire blob is read.
// If offset or length exceed the blob size, you’ll get fewer bytes.
//
// Returns the number of bytes copied.
//
// You can also do a manual transaction if you prefer, but typically read-only
// access is safe even outside a transaction in WAL mode.
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

	// Open the blob read-only
	blob, err := conn.OpenBlob("", table, column, rowID, false)
	if err != nil {
		return 0, fmt.Errorf("open blob handle failed: %w", err)
	}
	defer blob.Close()

	// Seek if offset > 0
	if offset > 0 {
		_, err := blob.Seek(offset, io.SeekStart)
		if err != nil {
			return 0, fmt.Errorf("seek failed: %w", err)
		}
	}

	// If length < 0 => read entire blob to EOF
	if length < 0 {
		return io.Copy(w, blob)
	}

	// Otherwise, wrap blob in an io.LimitReader for length
	return io.Copy(w, io.LimitReader(blob, length))
}

// ---------------------
// Internal helpers
// ---------------------

// beginTx starts a transaction on the given connection.
func beginTx(conn *sqlite.Conn) error {
	return execStmt(conn, "BEGIN TRANSACTION;")
}

// commitTx commits the current transaction.
func commitTx(conn *sqlite.Conn) error {
	return execStmt(conn, "COMMIT;")
}

// rollbackTx rolls back the current transaction.
func rollbackTx(conn *sqlite.Conn) error {
	return execStmt(conn, "ROLLBACK;")
}

// executeNoRows is a small helper to prepare+step a statement with params
// that doesn't produce row results. If you need row iteration, call sqlitex.Execute.
func executeNoRows(conn *sqlite.Conn, query string, params map[string]interface{}) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	// Bind parameters
	bindParams(stmt, params)

	// Step until done
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

// execStmt simply executes a raw statement with no params or result iteration.
func execStmt(conn *sqlite.Conn, sql string) error {
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
	}
	return nil
}
