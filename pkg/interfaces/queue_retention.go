package interfaces

import "context"

// RetainedPayload is one independently retriable item. RetentionKey is opaque to
// the SDK and must be stable across payloads belonging to the same resource.
type RetainedPayload struct {
	Payload      []byte
	RetentionKey string
}

// RetryDataSplitter must be deterministic, side-effect free, and preserve every
// item and its timestamps. It is used for new writes and existing queued batches.
type RetryDataSplitter func([]byte) ([]RetainedPayload, error)

// StoreForwardRetentionConfigurer is an optional service API. Register before
// Run, after defining the pipeline. Existing batches migrate before input starts.
type StoreForwardRetentionConfigurer interface {
	RegisterStoreForwardRetention(pipelineID string, split RetryDataSplitter) error
}

// RetentionStoreClient extends bounded storage without coupling the SDK to a
// particular reading format. Migration preserves the original rows on failure.
// StoreRetained commits bounded batches. Its counts include only committed
// batches, including when a later batch returns an error. Stored counts accepted
// inputs; dropped includes inputs rotated out within the same batch.
type RetentionStoreClient interface {
	ConfigureRetention(context.Context, string, RetryDataSplitter, QueueLimits) error
	StoreRetained(StoredObject, QueueLimits) (stored int64, dropped int64, err error)
}
