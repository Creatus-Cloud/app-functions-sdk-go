package postgres

import (
	"fmt"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
)

// queueStoragePressure takes normalized limits. The critical floor is independent
// of the rotation threshold so service priority cannot move the emergency cutoff.
func (c Client) queueStoragePressure(l interfaces.QueueLimits, size uint64) (bool, error) {
	if l.StoragePath == "" {
		return false, nil
	}
	read := c.freeSpace
	if read == nil {
		read = queueFreeSpace
	}
	available, err := read(l.StoragePath)
	if err != nil {
		return false, fmt.Errorf("inspect queue storage: %w", err)
	}
	// Estimate room for tuple/index/WAL writes; other writers and database
	// maintenance can still consume space, so this is not a physical quota.
	if size > (^uint64(0)-(1<<20))/4 {
		return false, fmt.Errorf("queued write size overflows storage headroom estimate")
	}
	headroom := size*4 + (1 << 20)
	critical := uint64(l.CriticalFreeBytes)
	if available <= critical || headroom > available-critical {
		return false, fmt.Errorf("StoreAndForward storage critically low: %d bytes available, queue write needs %d bytes above the %d-byte emergency floor", available, headroom, critical)
	}
	rotation := uint64(l.MinFreeBytes)
	return available <= rotation || headroom > available-rotation, nil
}
