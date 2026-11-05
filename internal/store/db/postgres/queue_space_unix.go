//go:build linux || darwin

package postgres

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func queueFreeSpace(path string) (uint64, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil {
		return 0, err
	}
	if fs.Files > 0 && fs.Ffree < 1024 {
		return 0, fmt.Errorf("queue filesystem has fewer than 1024 free inodes")
	}
	return fs.Bavail * uint64(fs.Bsize), nil
}
