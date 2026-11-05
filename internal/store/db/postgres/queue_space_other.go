//go:build !linux && !darwin

package postgres

import "fmt"

func queueFreeSpace(path string) (uint64, error) {
	return 0, fmt.Errorf("filesystem queue pressure checks are unsupported on this platform")
}
