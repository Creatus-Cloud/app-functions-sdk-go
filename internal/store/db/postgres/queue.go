package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/models"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/jackc/pgx/v5"
)

var _ interfaces.BoundedStoreClient = (*Client)(nil)

// Queue mutations share a transaction advisory lock across SDK instances. This
// serializes capacity decisions without taking a table lock that blocks VACUUM.
func (c Client) queueTransaction(limits interfaces.QueueLimits, action func(context.Context, pgx.Tx, interfaces.QueueLimits) (int64, error)) (int64, error) {
	return c.queueTransactionContext(context.Background(), limits, action)
}

func (c Client) queueTransactionContext(parent context.Context, limits interfaces.QueueLimits, action func(context.Context, pgx.Tx, interfaces.QueueLimits) (int64, error)) (int64, error) {
	limits, err := limits.Normalize()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	tx, err := c.connPool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	lock := sha256.Sum256([]byte("edgex-store-and-forward:" + c.getFullStoreTableName()))
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(binary.BigEndian.Uint64(lock[:8]))); err != nil {
		return 0, err
	}
	dropped, err := action(ctx, tx, limits)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return dropped, nil
}

// Explicit byte caps measure serialized JSON on demand. Free-space-only keyed
// retention does not scan or total queue payload sizes.
func (c Client) trimQueue(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
	order := "DESC"
	if l.RetentionPolicy == interfaces.KeepOldest {
		order = "ASC"
	}
	table := c.getFullStoreTableName()
	bytes := "0::bigint"
	if l.MaxBytes >= 0 {
		bytes = "sum(octet_length(content::text)) OVER w"
	}
	query := fmt.Sprintf(`WITH ranked AS (
 SELECT id, row_number() OVER w AS position,
 %s AS bytes
 FROM %s WINDOW w AS (ORDER BY created %s, id %s ROWS UNBOUNDED PRECEDING)
 ) DELETE FROM %s s USING ranked r WHERE s.id=r.id AND (r.position > $1 OR ($2 >= 0 AND r.bytes > $2))`, bytes, table, order, order, table)
	result, err := tx.Exec(ctx, query, l.MaxItems, l.MaxBytes)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (c Client) EnforceLimits(l interfaces.QueueLimits) (count int64, dropped int64, err error) {
	l, err = l.Normalize()
	if err != nil {
		return
	}
	// Counting and draining existing rows must work even when the disk/inode
	// guard rejects new writes. An uncapped queue needs no writer lock to count.
	if l.MaxBytes < 0 && l.MaxItems == interfaces.DefaultQueueItems {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = c.connPool.QueryRow(ctx, "SELECT count(*) FROM "+c.getFullStoreTableName()).Scan(&count)
		return
	}
	dropped, err = c.queueTransaction(l, func(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
		n, err := c.trimQueue(ctx, tx, l)
		if err != nil {
			return 0, err
		}
		err = tx.QueryRow(ctx, "SELECT count(*) FROM "+c.getFullStoreTableName()).Scan(&count)
		return n, err
	})
	return
}

func (c Client) StoreWithLimits(o interfaces.StoredObject, l interfaces.QueueLimits) (string, int64, error) {
	if err := o.ValidateContract(false); err != nil {
		return "", 0, err
	}
	_, dropped, err := c.storeRetained(&o, l)
	if err != nil {
		return "", 0, err
	}
	return o.ID, dropped, nil
}

// Recycle only enough oldest entries for this incoming object. Work is proportional
// to the replaced data, even when a long outage has filled a multi-GB queue.
func (c Client) evictQueueBytes(ctx context.Context, tx pgx.Tx, required int64, maxItems int) (int64, error) {
	var released, dropped int64
	for released < required {
		rows, err := tx.Query(ctx, "SELECT id::text,octet_length(content::text) FROM "+c.getFullStoreTableName()+" ORDER BY created,id LIMIT $1", maxItems)
		if err != nil {
			return 0, err
		}
		ids := make([]string, 0, maxItems)
		for rows.Next() {
			var id string
			var size int64
			if err := rows.Scan(&id, &size); err != nil {
				rows.Close()
				return 0, err
			}
			ids = append(ids, id)
			released += size
			if released >= required {
				break
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return 0, err
		}
		if len(ids) == 0 {
			return 0, fmt.Errorf("StoreAndForward storage reserve reached: insufficient queued bytes to replace")
		}
		result, err := tx.Exec(ctx, "DELETE FROM "+c.getFullStoreTableName()+" WHERE id=ANY($1::uuid[])", ids)
		if err != nil {
			return 0, err
		}
		dropped += result.RowsAffected()
	}
	return dropped, nil
}

func (c Client) UpdateWithLimits(o interfaces.StoredObject, l interfaces.QueueLimits) (int64, error) {
	if err := o.ValidateContract(true); err != nil {
		return 0, err
	}
	var model models.StoredObject
	model.FromContract(o)
	data, err := model.MarshalJSON()
	if err != nil {
		return 0, err
	}
	return c.queueTransaction(l, func(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
		if _, err := c.queueStoragePressure(l, uint64(len(data))); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, sqlUpdateContentById(c.getFullStoreTableName()), data, o.ID); err != nil {
			return 0, err
		}
		if l.MaxBytes > 0 || l.MaxItems < interfaces.DefaultQueueItems {
			return c.trimQueue(ctx, tx, l)
		}
		return 0, nil
	})
}

func (c Client) ReadQueuePage(after interfaces.QueueCursor, maxItems int) ([]interfaces.QueuedObject, error) {
	if maxItems < 1 || maxItems > interfaces.MaxQueueBatchItems {
		return nil, fmt.Errorf("invalid queue page size")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	query := "SELECT content,created,id,octet_length(content::text) AS item_bytes FROM " + c.getFullStoreTableName()
	args := []any{maxItems}
	if after.ID != "" {
		query += " WHERE (created,id) > ($2,$3::uuid)"
		args = append(args, after.Created, after.ID)
	}
	query += " ORDER BY created,id LIMIT $1"
	query = "WITH page AS (" + query + "), ranked AS (SELECT *,row_number() OVER w AS position,sum(item_bytes) OVER w AS bytes FROM page WINDOW w AS (ORDER BY created,id ROWS UNBOUNDED PRECEDING)) SELECT content,created,id::text FROM ranked WHERE position=1 OR bytes <= 4194304 ORDER BY created,id"
	rows, err := c.connPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (interfaces.QueuedObject, error) {
		var model models.StoredObject
		var cursor interfaces.QueueCursor
		err := row.Scan(&model, &cursor.Created, &cursor.ID)
		return interfaces.QueuedObject{StoredObject: model.ToContract(), Cursor: cursor}, err
	})
}
