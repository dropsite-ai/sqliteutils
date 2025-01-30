package exec

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/dropsite-ai/sqliteutils/pool"
	"zombiezen.com/go/sqlite"
)

// Exec executes a single SQL statement with parameters.
func Exec(ctx context.Context, query string, params map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	return ExecMulti(ctx, []string{query}, []map[string]interface{}{params}, resultFunc)
}

// Exec executes multiple SQL statements provided as separate queries with their respective parameters.
// Each query in the `queries` slice corresponds to the parameters in the `params` slice by index.
func ExecMulti(ctx context.Context, queries []string, params []map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	// Validate that the number of queries matches the number of params
	if len(queries) != len(params) {
		return fmt.Errorf("the number of queries (%d) does not match the number of params (%d)", len(queries), len(params))
	}

	// Obtain a connection pool
	pool, err := pool.GetPool()
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	// Take a connection from the pool
	conn, err := pool.Take(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain database connection: %w", err)
	}
	defer pool.Put(conn)

	// Execute each query with its corresponding parameters
	for i, query := range queries {
		trimmedQuery := trimQuery(query)
		if trimmedQuery == "" {
			continue
		}
		if err := executeSingleStatement(conn, trimmedQuery, params[i], i, resultFunc); err != nil {
			return fmt.Errorf("error executing statement %d: %w", i+1, err)
		}
	}

	return nil
}

// ExecTx executes multiple SQL statements within a single transaction.
// Each query in the `queries` slice corresponds to the parameters in the `params` slice by index.
func ExecMultiTx(ctx context.Context, queries []string, params []map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	// Validate that the number of queries matches the number of params
	if len(queries) != len(params) {
		return fmt.Errorf("the number of queries (%d) does not match the number of params (%d)", len(queries), len(params))
	}

	// Obtain a connection pool
	pool, err := pool.GetPool()
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	// Take a connection from the pool
	conn, err := pool.Take(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain database connection: %w", err)
	}
	defer pool.Put(conn)

	// Begin the transaction
	if err := executeRawStatement(conn, "BEGIN TRANSACTION;"); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := executeRawStatement(conn, "ROLLBACK;"); rollbackErr != nil {
				fmt.Printf("failed to rollback transaction: %v\n", rollbackErr)
			}
		}
	}()

	// Execute each query with its corresponding parameters
	for i, query := range queries {
		trimmedQuery := trimQuery(query)
		if trimmedQuery == "" {
			continue
		}
		if err := executeSingleStatement(conn, trimmedQuery, params[i], i, resultFunc); err != nil {
			return fmt.Errorf("error executing statement %d: %w", i+1, err)
		}
	}

	// Commit the transaction
	if err := executeRawStatement(conn, "COMMIT;"); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true
	return nil
}

// executeRawStatement executes a single SQL statement without parameter binding or result processing.
func executeRawStatement(conn *sqlite.Conn, statement string) error {
	stmt, err := conn.Prepare(statement)
	if err != nil {
		return fmt.Errorf("failed to prepare statement '%s': %w", statement, err)
	}
	defer stmt.Finalize()

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("error executing statement '%s': %w", statement, err)
		}
		if !hasRow {
			break
		}
	}
	return nil
}

// executeSingleStatement prepares and executes a single SQL statement with parameter binding and result processing.
func executeSingleStatement(conn *sqlite.Conn, query string, params map[string]interface{}, index int, resultFunc func(int, map[string]interface{})) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return fmt.Errorf("SQL preparation error for query '%s': %w", query, err)
	}
	defer stmt.Finalize()

	// Bind parameters specific to this query
	bindParams(stmt, params) // No error handling needed

	// Execute the statement and process results
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("error executing SQL query '%s': %w", query, err)
		}
		if !hasRow {
			break
		}
		if resultFunc != nil {
			resultFunc(index, readRow(stmt))
		}
	}

	// Reset the statement for potential reuse
	if err := stmt.Reset(); err != nil {
		return fmt.Errorf("failed to reset statement for query '%s': %w", query, err)
	}

	return nil
}

// readRow reads the current row from the statement and returns it as a map.
func readRow(stmt *sqlite.Stmt) map[string]interface{} {
	columnData := make(map[string]interface{})
	for i := 0; i < stmt.ColumnCount(); i++ {
		columnName := stmt.ColumnName(i)
		switch stmt.ColumnType(i) {
		case sqlite.TypeInteger:
			columnData[columnName] = stmt.ColumnInt64(i)
		case sqlite.TypeFloat:
			columnData[columnName] = stmt.ColumnFloat(i)
		case sqlite.TypeText:
			columnData[columnName] = stmt.ColumnText(i)
		case sqlite.TypeBlob:
			columnData[columnName] = stmt.ColumnBytes(i, nil)
		case sqlite.TypeNull:
			columnData[columnName] = nil
		default:
			columnData[columnName] = stmt.ColumnText(i)
		}
	}
	return columnData
}

// bindParams binds parameters to the SQL statement.
// NOTE: This function no longer returns an error because the Bind* methods do not.
func bindParams(stmt *sqlite.Stmt, params map[string]interface{}) {
	if params == nil {
		return
	}
	for i := 1; i <= stmt.BindParamCount(); i++ {
		paramName := stmt.BindParamName(i)
		if paramName == "" {
			continue
		}

		// Ensure that the parameter map keys include the prefix used in the SQL query (e.g., ":name")
		value, exists := params[paramName]
		if !exists || value == nil {
			stmt.BindNull(i)
			continue
		}

		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				stmt.BindNull(i)
				continue
			}
			value = val.Elem().Interface()
			val = reflect.ValueOf(value)
		}

		switch v := value.(type) {
		case string:
			stmt.BindText(i, v)
		case int, int32, int64:
			intVal := val.Int()
			stmt.BindInt64(i, intVal)
		case float32, float64:
			floatVal := val.Float()
			stmt.BindFloat(i, floatVal)
		case bool:
			stmt.BindBool(i, v)
		case []byte:
			stmt.BindBytes(i, v)
		default:
			// Unsupported types are handled by handleBindErr internally
			// Optionally, you can log or panic here if needed
			fmt.Printf("unsupported type for parameter '%s': %T\n", paramName, value)
		}
	}
}

// trimQuery trims whitespace and ensures the query does not end with a semicolon.
func trimQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	if strings.HasSuffix(trimmed, ";") {
		return strings.TrimSuffix(trimmed, ";")
	}
	return trimmed
}
