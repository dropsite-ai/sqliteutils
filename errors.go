package sqliteutils

import (
	"errors"
	"fmt"
)

// Common errors
var (
	ErrPoolNotInitialized = errors.New("pool not initialized")
)

// Error functions
func FailedToClosePoolError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to close pool: %w", err)
}

func FailedToEnableForeignKeysError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to close pool: %w", err)
}

func FailedToInitPoolError(err error, uri string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to init pool: [%s] %w", uri, err)
}

func FailedToGetPoolError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to get pool: %w", err)
}

func FailedToTakeConnectionFromPoolError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to take connection from pool: %w", err)
}

func FailedToExecScriptError(err error, script string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to exec script: [%s] %w", script, err)
}

func FailedToOpenDatabaseError(err error, path string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to open database: [%s] %w", path, err)
}

func FailedToInitBackupError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to get pool: %w", err)
}

func BackupStepFailedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backup step failed: %w", err)
}
