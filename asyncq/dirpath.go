package asyncq

import (
	"fmt"
	"os"
	"path/filepath"
)

func mkDirAll(durablePath string) error {
	if durablePath == "" {
		return InvalidDurablePathErr
	}
	for _, sub := range []dirType{wal, commited, corrupted, temp} {
		if err := os.MkdirAll(filepath.Join(durablePath, string(sub)), 0755); err != nil {
			return fmt.Errorf("failed to make WAL folder: %w", err)
		}
	}
	return nil
}

func joinPath(durablePath string, dir dirType) string {
	return filepath.Join(durablePath, string(dir))
}

type WALPath struct {
	path string
}

func (w *WALPath) Path() string {
	return w.path
}
