package interfaces

import (
	"fmt"
	"math"
	"path/filepath"
	"time"
)

const (
	KeepNewest                          = "keep-newest"
	KeepOldest                          = "keep-oldest"
	DefaultQueueItems                   = math.MaxInt
	DefaultQueueBatchItems              = 100
	MaxQueueBatchItems                  = 1000
	MaxQueueBatchBytes                  = 4 << 20
	DefaultQueueBytes             int64 = 64 << 20
	DefaultQueueMinFreeBytes      int64 = 1536 << 20
	DefaultQueueCriticalFreeBytes int64 = 500 << 20
)

// QueueLimits bounds stored JSON, including payload and context. Database indexes,
// dead tuples and WAL need additional disk space. Zero values select safe defaults.
type QueueLimits struct {
	MaxItems          int
	MaxBatchItems     int   // Zero selects DefaultQueueBatchItems.
	MaxBytes          int64 // -1 disables the byte cap with StoragePath; 0 selects the default.
	RetentionPolicy   string
	MinFreeBytes      int64
	CriticalFreeBytes int64
	StoragePath       string
}

func (l QueueLimits) Normalize() (QueueLimits, error) {
	if l.MaxBatchItems < 0 || l.MaxBatchItems > MaxQueueBatchItems {
		return l, fmt.Errorf("StoreAndForward MaxBatchItems must be between 1 and %d (0 selects the default)", MaxQueueBatchItems)
	}
	if l.MaxBatchItems == 0 {
		l.MaxBatchItems = DefaultQueueBatchItems
	}
	if l.MaxItems < 0 || l.MinFreeBytes < 0 || l.CriticalFreeBytes < 0 {
		return l, fmt.Errorf("StoreAndForward queue limits cannot be negative")
	}
	if l.MaxBytes < -1 {
		return l, fmt.Errorf("StoreAndForward MaxStoredBytes must be -1, 0, or a positive byte limit")
	}
	if l.MaxBytes == -1 && l.StoragePath == "" {
		return l, fmt.Errorf("StoreAndForward MaxStoredBytes=-1 requires StoragePath for free-space protection")
	}
	if l.MaxItems == 0 {
		l.MaxItems = DefaultQueueItems
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = DefaultQueueBytes
	}
	if l.StoragePath != "" && !filepath.IsAbs(l.StoragePath) {
		return l, fmt.Errorf("StoreAndForward StoragePath must be absolute")
	}
	if l.MinFreeBytes == 0 {
		l.MinFreeBytes = DefaultQueueMinFreeBytes
	}
	if l.CriticalFreeBytes == 0 {
		l.CriticalFreeBytes = DefaultQueueCriticalFreeBytes
	}
	if l.MinFreeBytes <= l.CriticalFreeBytes {
		return l, fmt.Errorf("StoreAndForward MinFreeBytes must exceed CriticalFreeBytes")
	}
	if l.RetentionPolicy == "" {
		l.RetentionPolicy = KeepNewest
	}
	if l.RetentionPolicy != KeepNewest && l.RetentionPolicy != KeepOldest {
		return l, fmt.Errorf("StoreAndForward RetentionPolicy must be keep-newest or keep-oldest")
	}
	return l, nil
}

// BoundedStoreClient is an optional extension implemented by PostgreSQL. Operations
// atomically enforce capacity across writers. Updates never resurrect objects
// evicted while a retry was in flight.
type BoundedStoreClient interface {
	StoreWithLimits(StoredObject, QueueLimits) (id string, dropped int64, err error)
	UpdateWithLimits(StoredObject, QueueLimits) (dropped int64, err error)
	EnforceLimits(QueueLimits) (count int64, dropped int64, err error)
	ReadQueuePage(after QueueCursor, maxItems int) ([]QueuedObject, error)
}

// BatchStoreRemover is an optional extension for acknowledging retry pages.
// Each bounded batch commits separately; removed counts committed rows even if
// a subsequent batch fails. Already evicted IDs are not counted again.
type BatchStoreRemover interface {
	RemoveFromStoreBatch([]StoredObject, QueueLimits) (removed int64, err error)
}

// QueueCursor remains usable after the corresponding row has been deleted.
type QueueCursor struct {
	Created time.Time
	ID      string
}
type QueuedObject struct {
	StoredObject
	Cursor QueueCursor
}
