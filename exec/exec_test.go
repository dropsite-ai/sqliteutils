package exec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dropsite-ai/sqliteutils/exec"
	"github.com/dropsite-ai/sqliteutils/pool"
	"github.com/dropsite-ai/sqliteutils/test"
	"github.com/stretchr/testify/assert"
)

const migration = `
	CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT UNIQUE NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE orders (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		product TEXT NOT NULL,
		quantity INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(user_id) REFERENCES users(id)
	);
	CREATE TRIGGER update_users_updated_at
		AFTER UPDATE ON users
		FOR EACH ROW
		BEGIN
			UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
		END;
	CREATE TRIGGER update_orders_updated_at
		AFTER UPDATE ON orders
		FOR EACH ROW
		BEGIN
			UPDATE orders SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
		END;
`

func TestExec(t *testing.T) {
	ctx := context.Background()

	err := test.SetupTestPool(ctx, t, migration, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := pool.ClosePool()
		assert.NoError(t, err, "Failed to close pool after tests")
	}()

	t.Run("InsertMultipleUsers", func(t *testing.T) {
		query := `
		INSERT INTO users (name, email) VALUES ($name1, $email1);
		INSERT INTO users (name, email) VALUES ($name2, $email2);
		INSERT INTO users (name, email) VALUES ($name3, $email3);
		`
		params := map[string]interface{}{
			"$name1":  "Alice Smith",
			"$email1": "alice@example.com",
			"$name2":  "Bob Johnson",
			"$email2": "bob@example.com",
			"$name3":  "Charlie Brown",
			"$email3": "charlie@example.com",
		}
		err := exec.Exec(ctx, query, params, nil)
		assert.NoError(t, err, "Exec should execute multiple INSERT statements without error")

		verifyUsers := []struct {
			Name  string
			Email string
		}{
			{"Alice Smith", "alice@example.com"},
			{"Bob Johnson", "bob@example.com"},
			{"Charlie Brown", "charlie@example.com"},
		}

		for _, user := range verifyUsers {
			query := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
			var count int
			resultFunc := func(index int, row map[string]interface{}) {
				if c, ok := row["count"].(int64); ok {
					count = int(c)
				}
			}
			err := exec.Exec(ctx, query, map[string]interface{}{
				"$name":  user.Name,
				"$email": user.Email,
			}, resultFunc)
			assert.NoError(t, err, "Exec should execute SELECT without error")
			assert.Equal(t, 1, count, fmt.Sprintf("User %s should exist in the database", user.Name))
		}
	})

	t.Run("SelectUsers", func(t *testing.T) {
		query := `SELECT id, name, email FROM users ORDER BY id ASC;`
		var results []map[string]interface{}

		resultFunc := func(index int, row map[string]interface{}) {
			results = append(results, row)
		}

		err := exec.Exec(ctx, query, nil, resultFunc)
		assert.NoError(t, err, "Exec should execute SELECT statement without error")
		assert.Len(t, results, 3, "There should be 3 users in the database")

		expectedUsers := []struct {
			ID    int64
			Name  string
			Email string
		}{
			{1, "Alice Smith", "alice@example.com"},
			{2, "Bob Johnson", "bob@example.com"},
			{3, "Charlie Brown", "charlie@example.com"},
		}

		for i, user := range expectedUsers {
			assert.Equal(t, user.ID, results[i]["id"], "User ID should match")
			assert.Equal(t, user.Name, results[i]["name"], "User name should match")
			assert.Equal(t, user.Email, results[i]["email"], "User email should match")
		}
	})

	t.Run("ParameterBinding", func(t *testing.T) {
		query := `
		INSERT INTO users (name, email) VALUES ($name, $email);
		`
		params := map[string]interface{}{
			"$name":  "David Lee",
			"$email": "david@example.com",
		}
		err := exec.Exec(ctx, query, params, nil)
		assert.NoError(t, err, "Exec should bind string parameters correctly")

		query = `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
		var count int
		resultFunc := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				count = int(c)
			}
		}
		err = exec.Exec(ctx, query, map[string]interface{}{
			"$name":  "David Lee",
			"$email": "david@example.com",
		}, resultFunc)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 1, count, "User David Lee should exist in the database")
	})

	// Test Case 4: ExecTx with Successful Transaction
	t.Run("ExecTx_Success", func(t *testing.T) {
		query := `
		INSERT INTO users (name, email) VALUES ($name1, $email1);
		INSERT INTO orders (user_id, product, quantity) VALUES ($user_id1, $product1, $quantity1);
		`

		params := map[string]interface{}{
			"$name1":     "Eve Adams",
			"$email1":    "eve@example.com",
			"$user_id1":  1, // Assuming user with id=1 exists
			"$product1":  "World's Best Boss Mug",
			"$quantity1": 1,
		}

		err := exec.ExecTx(ctx, query, params, nil)
		assert.NoError(t, err, "ExecTx should execute transaction without error")

		// Verify that the user is inserted
		queryUser := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
		var userCount int
		resultFuncUser := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				userCount = int(c)
			}
		}
		err = exec.Exec(ctx, queryUser, map[string]interface{}{
			"$name":  "Eve Adams",
			"$email": "eve@example.com",
		}, resultFuncUser)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 1, userCount, "User Eve Adams should exist in the database")

		// Verify that the order is inserted
		queryOrder := `SELECT COUNT(1) as count FROM orders WHERE product = $product AND quantity = $quantity;`
		var orderCount int
		resultFuncOrder := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				orderCount = int(c)
			}
		}
		err = exec.Exec(ctx, queryOrder, map[string]interface{}{
			"$product":  "World's Best Boss Mug",
			"$quantity": 1,
		}, resultFuncOrder)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 1, orderCount, "Order for World's Best Boss Mug should exist in the database")
	})

	// Test Case 5: ExecTx with Transaction Rollback on Error
	t.Run("ExecTx_RollbackOnError", func(t *testing.T) {
		query := `
		INSERT INTO users (name, email) VALUES ($name1, $email1);
		INSERT INTO users (name, email) VALUES ($name2, $email1); -- Duplicate email to trigger UNIQUE constraint
		`

		params := map[string]interface{}{
			"$name1":  "Grace Hopper",
			"$email1": "grace@example.com",
			"$name2":  "Henry Ford",
			// "$email2" intentionally missing to cause duplicate email
		}

		err := exec.ExecTx(ctx, query, params, nil)
		assert.Error(t, err, "ExecTx should return an error due to UNIQUE constraint violation")

		// Verify that no new users were inserted
		queryVerify := `SELECT COUNT(1) as count FROM users WHERE name = $name OR name = $name2;`
		var count int
		resultFuncVerify := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				count = int(c)
			}
		}
		err = exec.Exec(ctx, queryVerify, map[string]interface{}{
			"$name":  "Grace Hopper",
			"$name2": "Henry Ford",
		}, resultFuncVerify)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 0, count, "Neither Grace Hopper nor Henry Ford should exist due to transaction rollback")
	})

	// Test Case 6: ExecTx Nested Transactions (Should Fail or Handle Appropriately)
	t.Run("ExecTx_NestedTransactions", func(t *testing.T) {
		// Attempt to insert an order with a non-existent user_id, causing a FOREIGN KEY constraint violation.
		// This should trigger a transaction rollback.

		query := `
		INSERT INTO users (name, email) VALUES ($name1, $email1);
		INSERT INTO orders (user_id, product, quantity) VALUES ($user_id2, $product2, $quantity2);
		`

		params := map[string]interface{}{
			"$name1":     "Oscar Wilde",
			"$email1":    "oscar@example.com",
			"$user_id2":  999, // Non-existent user_id to trigger FOREIGN KEY constraint
			"$product2":  "Literary Classics",
			"$quantity2": 2,
		}

		err := exec.ExecTx(ctx, query, params, func(index int, row map[string]interface{}) {
			fmt.Println("Logging map:", row)
		})
		assert.Error(t, err, "ExecTx should return an error due to FOREIGN KEY constraint violation")

		// Verify that neither the user nor the order was inserted
		// Check user
		queryUser := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
		var userCount int
		resultFuncUser := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				userCount = int(c)
			}
		}
		err = exec.Exec(ctx, queryUser, map[string]interface{}{
			"$name":  "Oscar Wilde",
			"$email": "oscar@example.com",
		}, resultFuncUser)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 0, userCount, "User Oscar Wilde should not exist due to transaction rollback")

		// Check order
		queryOrder := `SELECT COUNT(1) as count FROM orders WHERE product = $product AND quantity = $quantity;`
		var orderCount int
		resultFuncOrder := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				orderCount = int(c)
			}
		}
		err = exec.Exec(ctx, queryOrder, map[string]interface{}{
			"$product":  "Literary Classics",
			"$quantity": 2,
		}, resultFuncOrder)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 0, orderCount, "Order Literary Classics should not exist due to transaction rollback")
	})

	// Test Case 7: ExecTx Commit Failure (Simulated)
	t.Run("ExecTx_CommitFailure", func(t *testing.T) {
		// To simulate a commit failure, include an invalid SQL statement which will cause the transaction to fail.
		query := `
		INSERT INTO users (name, email) VALUES ($name, $email);
		INVALID SQL STATEMENT;
		`

		params := map[string]interface{}{
			"$name":  "Peter Parker",
			"$email": "peter@example.com",
		}

		err := exec.ExecTx(ctx, query, params, nil)
		assert.Error(t, err, "ExecTx should return an error due to invalid SQL statement")

		// Verify that the user was not inserted due to transaction rollback
		queryUser := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
		var count int
		resultFuncUser := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				count = int(c)
			}
		}
		err = exec.Exec(ctx, queryUser, map[string]interface{}{
			"$name":  "Peter Parker",
			"$email": "peter@example.com",
		}, resultFuncUser)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 0, count, "User Peter Parker should not exist due to transaction rollback")
	})

	// Test Case 8: Exec with No Statements
	t.Run("Exec_NoStatements", func(t *testing.T) {
		query := `
		;
		;
		`

		err := exec.Exec(ctx, query, nil, nil)
		assert.NoError(t, err, "Exec should handle empty statements gracefully")
	})

	// Test Case 9: ExecTx with No Statements
	t.Run("ExecTx_NoStatements", func(t *testing.T) {
		query := `
		`
		// Empty query should not cause any issues

		err := exec.ExecTx(ctx, query, nil, nil)
		assert.NoError(t, err, "ExecTx should handle empty transaction gracefully")
	})

	// Test Case 10: Exec with Parameter Binding Edge Cases
	t.Run("Exec_ParameterBinding_EdgeCases", func(t *testing.T) {
		// Insert a user with NULL email
		query := `
		INSERT INTO users (name, email) VALUES ($name, $email);
		`

		params := map[string]interface{}{
			"$name":  "Karen Page",
			"$email": nil, // NULL email
		}

		err := exec.Exec(ctx, query, params, nil)
		assert.Error(t, err, "Exec should return an error when inserting NULL into NOT NULL column")
	})

	// Test Case 11: ExecTx with Complex Statements and Result Processing
	// Test Case 11: ExecTx with Complex Statements and Result Processing
	t.Run("ExecTx_WithResults", func(t *testing.T) {
		// Define a transaction that inserts a user
		insertQuery := `
	INSERT INTO users (name, email) VALUES ($name, $email);
	`

		insertParams := map[string]interface{}{
			"$name":  "Laura Palmer",
			"$email": "laura@example.com",
		}

		err := exec.ExecTx(ctx, insertQuery, insertParams, nil)
		assert.NoError(t, err, "ExecTx should execute transaction without error")

		// Verify that the user exists by querying with unique fields
		queryVerify := `SELECT name, email FROM users WHERE name = $name AND email = $email;`
		var name, email string
		resultFuncVerify := func(index int, row map[string]interface{}) {
			if n, ok := row["name"].(string); ok {
				name = n
			}
			if e, ok := row["email"].(string); ok {
				email = e
			}
		}
		err = exec.Exec(ctx, queryVerify, map[string]interface{}{
			"$name":  "Laura Palmer",
			"$email": "laura@example.com",
		}, resultFuncVerify)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, "Laura Palmer", name, "User name should match")
		assert.Equal(t, "laura@example.com", email, "User email should match")
	})
}

// TestExec_Concurrency tests concurrent executions of Exec and ExecTx.
func TestExec_Concurrency(t *testing.T) {
	ctx := context.Background()

	m := migration + `
		INSERT INTO users (name, email) VALUES ('Initial User', 'initial@example.com');
	`

	err := test.SetupTestPool(ctx, t, m, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = pool.ClosePool()
		assert.NoError(t, err, "Failed to close pool after tests")
	}()

	// Define concurrent tasks
	tasks := []struct {
		name    string
		query   string
		params  map[string]interface{}
		isTx    bool
		wantErr bool
	}{
		{
			name: "Concurrent_Insert_User_1",
			query: `
			INSERT INTO users (name, email) VALUES ($name, $email);
			`,
			params: map[string]interface{}{
				"$name":  "Concurrent User 1",
				"$email": "concurrent1@example.com",
			},
			isTx:    false,
			wantErr: false,
		},
		{
			name: "Concurrent_Insert_User_2",
			query: `
			INSERT INTO users (name, email) VALUES ($name, $email);
			`,
			params: map[string]interface{}{
				"$name":  "Concurrent User 2",
				"$email": "concurrent2@example.com",
			},
			isTx:    false,
			wantErr: false,
		},
		{
			name: "Concurrent_Transaction_Insert_Order",
			query: `
			INSERT INTO orders (user_id, product, quantity) VALUES ($user_id, $product, $quantity);
			`,
			params: map[string]interface{}{
				"$user_id":  1, // Assuming user with id=1 exists
				"$product":  "Concurrent Product",
				"$quantity": 10,
			},
			isTx:    true,
			wantErr: false,
		},
		{
			name: "Concurrent_Transaction_Failure",
			query: `
			INSERT INTO users (name, email) VALUES ($name, $email);
			INSERT INTO orders (user_id, product, quantity) VALUES ($user_id, $product, $quantity); -- user_id might not exist
			`,
			params: map[string]interface{}{
				"$name":     "Concurrent User 3",
				"$email":    "concurrent3@example.com",
				"$user_id":  999, // Non-existent user_id to trigger FOREIGN KEY constraint
				"$product":  "Invalid Order",
				"$quantity": 5,
			},
			isTx:    true,
			wantErr: true,
		},
	}

	// Execute tasks concurrently
	done := make(chan bool)
	for _, task := range tasks {
		go func(task struct {
			name    string
			query   string
			params  map[string]interface{}
			isTx    bool
			wantErr bool
		}) {
			defer func() { done <- true }()
			if task.isTx {
				err = exec.ExecTx(ctx, task.query, task.params, nil)
				if task.wantErr {
					assert.Error(t, err, fmt.Sprintf("%s should return an error", task.name))
				} else {
					assert.NoError(t, err, fmt.Sprintf("%s should execute without error", task.name))
				}
			} else {
				err = exec.Exec(ctx, task.query, task.params, nil)
				if task.wantErr {
					assert.Error(t, err, fmt.Sprintf("%s should return an error", task.name))
				} else {
					assert.NoError(t, err, fmt.Sprintf("%s should execute without error", task.name))
				}
			}
		}(task)
	}

	// Wait for all tasks to complete
	for range tasks {
		<-done
	}

	// Verify that only valid inserts were committed
	// Concurrent_User_1 and Concurrent_User_2 should exist
	verifyUsers := []struct {
		Name  string
		Email string
	}{
		{"Concurrent User 1", "concurrent1@example.com"},
		{"Concurrent User 2", "concurrent2@example.com"},
	}

	for _, user := range verifyUsers {
		query := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
		var count int
		resultFunc := func(index int, row map[string]interface{}) {
			if c, ok := row["count"].(int64); ok {
				count = int(c)
			}
		}
		err = exec.Exec(ctx, query, map[string]interface{}{
			"$name":  user.Name,
			"$email": user.Email,
		}, resultFunc)
		assert.NoError(t, err, "Exec should execute SELECT without error")
		assert.Equal(t, 1, count, fmt.Sprintf("User %s should exist in the database", user.Name))
	}

	// Verify that Concurrent_User_3 does not exist due to transaction rollback
	queryUser := `SELECT COUNT(1) as count FROM users WHERE name = $name AND email = $email;`
	var userCount int
	resultFuncUser := func(index int, row map[string]interface{}) {
		if c, ok := row["count"].(int64); ok {
			userCount = int(c)
		}
	}
	err = exec.Exec(ctx, queryUser, map[string]interface{}{
		"$name":  "Concurrent User 3",
		"$email": "concurrent3@example.com",
	}, resultFuncUser)
	assert.NoError(t, err, "Exec should execute SELECT without error")
	assert.Equal(t, 0, userCount, "User Concurrent User 3 should not exist due to transaction rollback")
}
