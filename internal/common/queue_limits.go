package common

import (
	"fmt"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
)

func (s StoreAndForwardInfo) QueueLimits() (interfaces.QueueLimits, error) {
	return (interfaces.QueueLimits{
		MaxBatchItems:     s.MaxBatchItems,
		MaxBytes:          s.MaxStoredBytes,
		RetentionPolicy:   s.RetentionPolicy,
		MinFreeBytes:      s.MinFreeBytes,
		CriticalFreeBytes: s.CriticalFreeBytes,
		StoragePath:       s.StoragePath,
	}).Normalize()
}

// ValidateQueueStore checks both the values and the backend's ability to enforce
// explicitly configured limits. Plain stores remain supported with zero values.
func (s StoreAndForwardInfo) ValidateQueueStore(store interfaces.StoreClient) error {
	if _, err := s.QueueLimits(); err != nil {
		return err
	}
	if s.MaxBatchItems != 0 || s.MaxStoredBytes != 0 || s.RetentionPolicy != "" || s.StoragePath != "" || s.MinFreeBytes != 0 || s.CriticalFreeBytes != 0 {
		if _, ok := store.(interfaces.BoundedStoreClient); !ok {
			return fmt.Errorf("configured StoreAndForward capacity limits require a bounded store (PostgreSQL)")
		}
	}
	return nil
}
