package backup

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dropsite-ai/sqliteutils"
	"zombiezen.com/go/sqlite"
)

func BackupDatabase(sourceDBPath, destDBPath string) error {
	// Open the source database
	srcConn, err := sqlite.OpenConn(sourceDBPath, sqlite.OpenReadOnly)
	if err != nil {
		return sqliteutils.FailedToOpenDatabaseError(err, sourceDBPath)
	}
	defer srcConn.Close()

	// Open the destination database
	dstConn, err := sqlite.OpenConn(destDBPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		return sqliteutils.FailedToOpenDatabaseError(err, destDBPath)
	}
	defer func() {
		if err = dstConn.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	// Create Backup object
	backup, err := sqlite.NewBackup(dstConn, "main", srcConn, "main")
	if err != nil {
		return sqliteutils.FailedToInitBackupError(err)
	}
	defer func() {
		if err := backup.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	// Perform online backup/copy with step iterations
	for {
		more, err := backup.Step(5) // Copy 5 pages at a time
		if err != nil {
			if strings.Contains(err.Error(), "database is locked") || strings.Contains(err.Error(), "database is busy") {
				time.Sleep(250 * time.Millisecond) // Wait and retry
				continue
			}
			return sqliteutils.BackupStepFailedError(err)
		}
		if !more {
			break
		}
	}

	return nil
}
