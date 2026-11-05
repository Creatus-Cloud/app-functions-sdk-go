package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/models"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ interfaces.RetentionStoreClient = (*Client)(nil)

type retentionRegistry struct {
	mu          sync.RWMutex
	migrationMu sync.Mutex
	splitters   map[string]interfaces.RetryDataSplitter
}

func newRetentionRegistry() *retentionRegistry {
	return &retentionRegistry{splitters: make(map[string]interfaces.RetryDataSplitter)}
}

func (c Client) splitter(pipelineID string) interfaces.RetryDataSplitter {
	if c.retention == nil {
		return nil
	}
	c.retention.mu.RLock()
	defer c.retention.mu.RUnlock()
	return c.retention.splitters[pipelineID]
}

func splitQueueObject(o interfaces.StoredObject, split interfaces.RetryDataSplitter) ([]interfaces.RetainedPayload, error) {
	if len(o.Payload) > int(interfaces.DefaultQueueBytes) {
		return nil, fmt.Errorf("individual queued payload exceeds 64 MiB")
	}
	if split == nil {
		return []interfaces.RetainedPayload{{Payload: o.Payload}}, nil
	}
	items, err := split(o.Payload)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("queue splitter returned no items")
	}
	for _, item := range items {
		if len(item.Payload) == 0 || len(item.Payload) > int(interfaces.DefaultQueueBytes) || len(item.RetentionKey) == 0 || len(item.RetentionKey) > 1024 {
			return nil, fmt.Errorf("queue splitter returned an invalid payload or retention key (1–1024 bytes required)")
		}
	}
	return items, nil
}

// ConfigureRetention migrates one old batch per transaction before the service
// exposes this client to input/retry workers. Completed batches are resumable;
// failures leave the current batch intact. No retention eviction runs on upgrade.
func (c Client) ConfigureRetention(ctx context.Context, pipelineID string, split interfaces.RetryDataSplitter, limits interfaces.QueueLimits) error {
	if pipelineID == "" || split == nil || c.retention == nil {
		return fmt.Errorf("queue retention requires a pipeline, splitter and initialized registry")
	}
	c.retention.migrationMu.Lock()
	defer c.retention.migrationMu.Unlock()
	for {
		found := false
		_, err := c.queueTransactionContext(ctx, limits, func(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
			var model models.StoredObject
			var created time.Time
			var rowID string
			err := tx.QueryRow(ctx, "SELECT id::text,content,created FROM "+c.getFullStoreTableName()+" WHERE retention_key IS NULL AND content->>'pipelineId'=$1 ORDER BY created,id LIMIT 1 FOR UPDATE", pipelineID).Scan(&rowID, &model, &created)
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, nil
			}
			if err != nil {
				return 0, err
			}
			found = true
			original := model.ToContract()
			if original.ID != rowID || original.AppServiceKey != c.appServiceKey {
				return 0, fmt.Errorf("queued object %s has inconsistent identity", rowID)
			}
			parts, err := splitQueueObject(original, split)
			if err != nil {
				return 0, fmt.Errorf("split queued object %s: %w", original.ID, err)
			}
			for _, part := range parts {
				item := original
				// V7 IDs preserve the batch's item order when created timestamps tie.
				id, err := uuid.NewV7()
				if err != nil {
					return 0, err
				}
				item.ID, item.Payload = id.String(), part.Payload
				model.FromContract(item)
				data, err := model.MarshalJSON()
				if err != nil {
					return 0, err
				}
				if len(data) > int(interfaces.DefaultQueueBytes) {
					return 0, fmt.Errorf("migrated queued object exceeds 64 MiB")
				}
				if _, err := c.queueStoragePressure(l, uint64(len(data))); err != nil {
					return 0, err
				}
				if _, err := tx.Exec(ctx, "INSERT INTO "+c.getFullStoreTableName()+" (id,created,content,retention_key) VALUES ($1,$2,$3,$4)", item.ID, created, data, part.RetentionKey); err != nil {
					return 0, err
				}
			}
			_, err = tx.Exec(ctx, sqlDeleteById(c.getFullStoreTableName()), original.ID)
			return 0, err
		})
		if err != nil {
			return err
		}
		if !found {
			break
		}
	}
	c.retention.mu.Lock()
	c.retention.splitters[pipelineID] = split
	c.retention.mu.Unlock()
	return nil
}

// StoreRetained commits bounded batches immediately. Errors roll back the current
// batch; counts still describe earlier committed batches from the same payload.
func (c Client) StoreRetained(o interfaces.StoredObject, l interfaces.QueueLimits) (stored, dropped int64, err error) {
	return c.storeRetained(&o, l)
}

func (c Client) storeRetained(o *interfaces.StoredObject, l interfaces.QueueLimits) (stored, dropped int64, err error) {
	l, err = l.Normalize()
	if err != nil {
		return 0, 0, err
	}
	if err := o.ValidateContract(false); err != nil {
		return 0, 0, err
	}
	if o.AppServiceKey != c.appServiceKey {
		return 0, 0, fmt.Errorf("queue service key mismatch")
	}
	parts, err := splitQueueObject(*o, c.splitter(o.PipelineId))
	if err != nil {
		return 0, 0, err
	}
	batch := make([]queueBatchItem, 0, min(l.MaxBatchItems, len(parts)))
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := c.queueTransaction(l, func(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
			return c.storeQueueBatch(ctx, tx, o.PipelineId, batch, l)
		})
		if err != nil {
			return err
		}
		stored += int64(len(batch))
		dropped += n
		batch, batchBytes = batch[:0], 0
		return nil
	}
	for i, part := range parts {
		item := *o
		item.Payload = part.Payload
		if part.RetentionKey != "" || i > 0 {
			id, err := uuid.NewV7()
			if err != nil {
				return stored, dropped, err
			}
			item.ID = id.String()
			if i == 0 {
				o.ID = item.ID
			}
		}
		encoded, err := encodeQueueItem(item, part.RetentionKey)
		if err != nil {
			return stored, dropped, err
		}
		if batchBytes+len(encoded.data) > interfaces.MaxQueueBatchBytes {
			if err := flush(); err != nil {
				return stored, dropped, err
			}
		}
		batch = append(batch, encoded)
		batchBytes += len(encoded.data)
		if len(batch) == l.MaxBatchItems {
			if err := flush(); err != nil {
				return stored, dropped, err
			}
		}
	}
	err = flush()
	return stored, dropped, err
}
