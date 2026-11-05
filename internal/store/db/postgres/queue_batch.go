package postgres

import (
	"context"
	"fmt"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/models"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/jackc/pgx/v5"
)

type queueBatchItem struct {
	id, data, key string
	size          int64
}

func encodeQueueItem(o interfaces.StoredObject, key string) (queueBatchItem, error) {
	var model models.StoredObject
	model.FromContract(o)
	data, err := model.MarshalJSON()
	if err != nil {
		return queueBatchItem{}, err
	}
	if len(data) > interfaces.MaxQueueBatchBytes {
		return queueBatchItem{}, fmt.Errorf("queued object exceeds the 4 MiB batch byte limit")
	}
	return queueBatchItem{id: o.ID, data: string(data), key: key}, nil
}

// measureQueueBatch measures all canonical JSONB sizes in one database call.
// The hard batch bound is on encoded JSON sent by the SDK; these sizes also
// include PostgreSQL's formatting for the optional total queue byte cap.
func measureQueueBatch(ctx context.Context, tx pgx.Tx, items []queueBatchItem) (int64, error) {
	data := make([]string, len(items))
	for i := range items {
		data[i] = items[i].data
	}
	rows, err := tx.Query(ctx, "SELECT octet_length(data::jsonb::text) FROM unnest($1::text[]) WITH ORDINALITY AS input(data,position) ORDER BY position", data)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	i := 0
	for rows.Next() {
		if err := rows.Scan(&items[i].size); err != nil {
			return 0, err
		}
		total += items[i].size
		i++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if i != len(items) {
		return 0, fmt.Errorf("incomplete queue batch size result")
	}
	return total, nil
}

func (c Client) storeQueueBatch(ctx context.Context, tx pgx.Tx, pipelineID string, items []queueBatchItem, l interfaces.QueueLimits) (int64, error) {
	size, err := measureQueueBatch(ctx, tx, items)
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if l.MaxBytes > 0 && item.size > l.MaxBytes {
			return 0, fmt.Errorf("queued object exceeds StoreAndForward MaxStoredBytes (%d)", l.MaxBytes)
		}
	}
	// Reserve space for the whole transaction, not each row independently.
	pressure, err := c.queueStoragePressure(l, uint64(size))
	if err != nil {
		return 0, err
	}
	if pressure && l.RetentionPolicy == interfaces.KeepOldest {
		return 0, fmt.Errorf("StoreAndForward storage reserve reached: rejected new batch (keep-oldest)")
	}
	var dropped int64
	if pressure {
		if items[0].key != "" {
			originalCount := len(items)
			items, err = c.rotateQueueBatch(ctx, tx, pipelineID, items)
			// Includes incoming rows superseded by newer rows of the same key.
			dropped = int64(originalCount)
			size = 0
			for _, item := range items {
				size += item.size
			}
		} else {
			dropped, err = c.evictQueueBytes(ctx, tx, size, l.MaxBatchItems)
		}
		if err != nil {
			return 0, err
		}
	}
	if l.RetentionPolicy == interfaces.KeepOldest && (l.MaxBytes > 0 || l.MaxItems < interfaces.DefaultQueueItems) {
		var count, bytes int64
		if err := tx.QueryRow(ctx, "SELECT count(*), COALESCE(sum(octet_length(content::text)),0) FROM "+c.getFullStoreTableName()).Scan(&count, &bytes); err != nil {
			return 0, err
		}
		if count > int64(l.MaxItems-len(items)) || (l.MaxBytes > 0 && bytes > l.MaxBytes-size) {
			return 0, fmt.Errorf("StoreAndForward queue full: rejected new batch (keep-oldest)")
		}
	} else if l.MaxBytes > 0 || l.MaxItems < interfaces.DefaultQueueItems {
		// A batch larger than the optional queue cap retains its newest suffix,
		// just as consecutive one-row replacements would do.
		for len(items) > l.MaxItems || (l.MaxBytes > 0 && size > l.MaxBytes) {
			size -= items[0].size
			items = items[1:]
			dropped++
		}
		budget := l
		budget.MaxItems -= len(items)
		if budget.MaxBytes > 0 {
			budget.MaxBytes -= size
		}
		n, err := c.trimQueue(ctx, tx, budget)
		if err != nil {
			return 0, err
		}
		dropped += n
	}
	ids, data, keys := make([]string, len(items)), make([]string, len(items)), make([]string, len(items))
	for i, item := range items {
		ids[i], data[i], keys[i] = item.id, item.data, item.key
	}
	// Keyed IDs are UUIDv7, so the batch's order is preserved when timestamps tie.
	_, err = tx.Exec(ctx, "INSERT INTO "+c.getFullStoreTableName()+" (id,created,content,retention_key) SELECT id::uuid,statement_timestamp(),data::jsonb,NULLIF(key,'') FROM unnest($1::text[],$2::text[],$3::text[]) WITH ORDINALITY AS input(id,data,key,position) ORDER BY position", ids, data, keys)
	return dropped, err
}

// rotateQueueBatch deletes the oldest matching rows in one statement. If a key
// has fewer queued rows than incoming rows, keep only that many newest inputs.
// This preserves the key's capacity instead of growing it during rotation.
func (c Client) rotateQueueBatch(ctx context.Context, tx pgx.Tx, pipelineID string, items []queueBatchItem) ([]queueBatchItem, error) {
	keys := make([]string, len(items))
	for i := range items {
		keys[i] = items[i].key
	}
	table := c.getFullStoreTableName()
	query := `WITH wanted AS (
 SELECT key,count(*) AS needed FROM unnest($1::text[]) AS input(key) GROUP BY key
), victims AS (
 SELECT old.id FROM wanted w CROSS JOIN LATERAL (
  SELECT id FROM ` + table + ` WHERE retention_key=w.key AND content->>'pipelineId'=$2
  ORDER BY created,id LIMIT w.needed FOR UPDATE
 ) old
) DELETE FROM ` + table + ` s USING victims v WHERE s.id=v.id RETURNING s.retention_key`
	rows, err := tx.Query(ctx, query, keys, pipelineID)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return nil, err
		}
		counts[key]++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if counts[key] == 0 {
			return nil, fmt.Errorf("StoreAndForward storage reserve reached: no queued item with the same retention key to replace")
		}
	}
	keep := make([]bool, len(items))
	for i := len(items) - 1; i >= 0; i-- {
		if counts[items[i].key] > 0 {
			keep[i] = true
			counts[items[i].key]--
		}
	}
	retained := make([]queueBatchItem, 0, len(items))
	for i, item := range items {
		if keep[i] {
			retained = append(retained, item)
		}
	}
	return retained, nil
}

var _ interfaces.BatchStoreRemover = (*Client)(nil)

func (c Client) RemoveFromStoreBatch(items []interfaces.StoredObject, l interfaces.QueueLimits) (removed int64, err error) {
	l, err = l.Normalize()
	if err != nil {
		return 0, err
	}
	for len(items) > 0 {
		n := min(l.MaxBatchItems, len(items))
		ids := make([]string, n)
		for i, item := range items[:n] {
			if err := item.ValidateContract(true); err != nil {
				return removed, err
			}
			if item.AppServiceKey != c.appServiceKey {
				return removed, fmt.Errorf("queue service key mismatch")
			}
			ids[i] = item.ID
		}
		count, err := c.queueTransaction(l, func(ctx context.Context, tx pgx.Tx, _ interfaces.QueueLimits) (int64, error) {
			result, err := tx.Exec(ctx, "DELETE FROM "+c.getFullStoreTableName()+" WHERE id=ANY($1::uuid[])", ids)
			return result.RowsAffected(), err
		})
		if err != nil {
			return removed, err
		}
		removed += count
		items = items[n:]
	}
	return removed, nil
}
