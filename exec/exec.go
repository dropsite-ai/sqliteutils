package exec

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/dropsite-ai/sqliteutils/pool"
	"zombiezen.com/go/sqlite"
)

// Exec executes a query string containing multiple SQL statements.
// It splits the query by ";\n" and executes each statement sequentially.
func Exec(ctx context.Context, queryString string, params map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	pool, err := pool.GetPool()
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	conn, err := pool.Take(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain database connection: %w", err)
	}
	defer pool.Put(conn)

	return executeMultipleStatements(conn, queryString, params, resultFunc)
}

// ExecTx executes a series of SQL statements wrapped in a transaction.
func ExecTx(ctx context.Context, queryString string, params map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	pool, err := pool.GetPool()
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	conn, err := pool.Take(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain database connection: %w", err)
	}
	defer pool.Put(conn)

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

	if err := executeMultipleStatements(conn, queryString, params, resultFunc); err != nil {
		return err
	}

	if err := executeRawStatement(conn, "COMMIT;"); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true
	return nil
}

func executeMultipleStatements(conn *sqlite.Conn, queryString string, params map[string]interface{}, resultFunc func(int, map[string]interface{})) error {
	queries := strings.Split(queryString, ";\n")
	statementIndex := 0
	for _, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		if err := executeSingleStatement(conn, query, params, statementIndex, resultFunc); err != nil {
			return fmt.Errorf("error executing statement %d: %w", statementIndex+1, err)
		}
		statementIndex++
	}
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
		return fmt.Errorf("SQL preparation error: %w", err)
	}
	defer stmt.Finalize()

	if err := bindParams(stmt, params); err != nil {
		return err
	}

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("error executing SQL query: %w", err)
		}
		if !hasRow {
			break
		}
		if resultFunc != nil {
			resultFunc(index, readRow(stmt))
		}
	}
	return nil
}

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
func bindParams(stmt *sqlite.Stmt, params map[string]interface{}) error {
	if params == nil {
		return nil
	}
	for i := 1; i <= stmt.BindParamCount(); i++ {
		paramName := stmt.BindParamName(i)
		if paramName == "" {
			continue
		}

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
			stmt.BindInt64(i, val.Int())
		case float32, float64:
			stmt.BindFloat(i, val.Float())
		case bool:
			stmt.BindBool(i, v)
		case []byte:
			stmt.BindBytes(i, v)
		default:
			return fmt.Errorf("unsupported type for parameter '%s': %T", paramName, value)
		}
	}
	return nil
}
