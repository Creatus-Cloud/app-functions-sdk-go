package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Statement triggers verify real multi-row statements and commit boundaries,
// including that audit records disappear when a batch rolls back.
func auditQueueBatches(t *testing.T, c *Client) string {
	t.Helper()
	audit := pgx.Identifier{c.appServiceKey, "batch_audit"}.Sanitize()
	fn := pgx.Identifier{c.appServiceKey, "audit_batch"}.Sanitize()
	_, err := c.connPool.Exec(context.Background(), "CREATE TABLE "+audit+" (seq bigserial,kind text,n bigint,xid bigint)")
	require.NoError(t, err)
	_, err = c.connPool.Exec(context.Background(), "CREATE FUNCTION "+fn+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO "+audit+" (kind,n,xid) SELECT TG_OP,count(*),txid_current() FROM changed; RETURN NULL; END $$")
	require.NoError(t, err)
	for _, operation := range []string{"INSERT", "DELETE"} {
		reference := "NEW"
		if operation == "DELETE" {
			reference = "OLD"
		}
		_, err := c.connPool.Exec(context.Background(), "CREATE TRIGGER audit_"+strings.ToLower(operation)+" AFTER "+operation+" ON "+c.getFullStoreTableName()+" REFERENCING "+reference+" TABLE AS changed FOR EACH STATEMENT EXECUTE FUNCTION "+fn+"()")
		require.NoError(t, err)
	}
	return audit
}

func auditSizes(t *testing.T, c *Client, audit, operation string) ([]int64, []int64) {
	t.Helper()
	rows, err := c.connPool.Query(context.Background(), "SELECT n,xid FROM "+audit+" WHERE kind=$1 ORDER BY seq", operation)
	require.NoError(t, err)
	defer rows.Close()
	var counts, transactions []int64
	for rows.Next() {
		var n, xid int64
		require.NoError(t, rows.Scan(&n, &xid))
		counts, transactions = append(counts, n), append(transactions, xid)
	}
	require.NoError(t, rows.Err())
	return counts, transactions
}

func batchTestQueue(t *testing.T, maxItems int) (*Client, interfaces.QueueLimits) {
	t.Helper()
	c := queueTestClient(t)
	c.freeSpace = func(string) (uint64, error) { return 8 << 30, nil }
	l := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage", MaxBatchItems: maxItems}
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	return c, l
}

func TestQueueUsesBoundedMultiRowTransactions(t *testing.T) {
	c, l := batchTestQueue(t, 3)
	audit := auditQueueBatches(t, c)
	var input []retentionTestItem
	for i := 0; i < 7; i++ {
		input = append(input, retentionTestItem{fmt.Sprint(i), fmt.Sprint(i)})
	}
	stored, dropped, err := c.StoreRetained(retentionObject(t, c, input...), l)
	require.NoError(t, err)
	require.Equal(t, int64(7), stored)
	require.Zero(t, dropped)
	sizes, transactions := auditSizes(t, c, audit, "INSERT")
	require.Equal(t, []int64{3, 3, 1}, sizes)
	require.NotEqual(t, transactions[0], transactions[1])
	require.NotEqual(t, transactions[1], transactions[2])
	// A changed live limit applies to the next write without recreating the client.
	l.MaxBatchItems = 2
	_, _, err = c.StoreRetained(retentionObject(t, c, input[:3]...), l)
	require.NoError(t, err)
	sizes, _ = auditSizes(t, c, audit, "INSERT")
	require.Equal(t, []int64{3, 3, 1, 2, 1}, sizes)
}

func TestBatchRotationPreservesResourceCapacityAndOrder(t *testing.T) {
	c, l := batchTestQueue(t, 100)
	_, _, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "old-1"}, retentionTestItem{"a", "old-2"},
		retentionTestItem{"b", "old"}, retentionTestItem{"quiet", "keep"}), l)
	require.NoError(t, err)
	audit := auditQueueBatches(t, c)
	c.freeSpace = func(string) (uint64, error) { return 1 << 30, nil }
	stored, dropped, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "1"}, retentionTestItem{"a", "2"}, retentionTestItem{"b", "1"},
		retentionTestItem{"a", "3"}, retentionTestItem{"a", "4"}, retentionTestItem{"b", "2"}, retentionTestItem{"a", "5"}), l)
	require.NoError(t, err)
	require.Equal(t, int64(7), stored)
	require.Equal(t, int64(7), dropped)
	require.Equal(t, map[string][]string{"a": {"4", "5"}, "b": {"2"}, "quiet": {"keep"}}, retentionValues(t, c))
	inserted, insertTX := auditSizes(t, c, audit, "INSERT")
	deleted, deleteTX := auditSizes(t, c, audit, "DELETE")
	require.Equal(t, []int64{3}, inserted)
	require.Equal(t, inserted, deleted)
	require.Equal(t, insertTX, deleteTX)
}

func TestBatchRotationKeepsOtherPipelines(t *testing.T) {
	c, l := batchTestQueue(t, 100)
	require.NoError(t, c.ConfigureRetention(context.Background(), "other", splitRetentionTestItems, l))
	other := retentionObject(t, c, retentionTestItem{"a", "other-pipeline"})
	other.PipelineId = "other"
	_, _, err := c.StoreRetained(other, l)
	require.NoError(t, err)
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"a", "old"}), l)
	require.NoError(t, err)
	c.freeSpace = func(string) (uint64, error) { return 1 << 30, nil }
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"a", "new"}), l)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"a": {"other-pipeline", "new"}}, retentionValues(t, c))
}

func TestFailedBatchRollsBackOnlyItsOwnReplacements(t *testing.T) {
	c, l := batchTestQueue(t, 2)
	_, _, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "old"}, retentionTestItem{"b", "old"},
		retentionTestItem{"c", "old"}, retentionTestItem{"fail", "old"}), l)
	require.NoError(t, err)
	_, err = c.connPool.Exec(context.Background(), "ALTER TABLE "+c.getFullStoreTableName()+" ADD CONSTRAINT fail_test CHECK (retention_key <> 'fail') NOT VALID")
	require.NoError(t, err)
	audit := auditQueueBatches(t, c)
	c.freeSpace = func(string) (uint64, error) { return 1 << 30, nil }
	stored, dropped, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "new"}, retentionTestItem{"b", "new"},
		retentionTestItem{"c", "new"}, retentionTestItem{"fail", "new"}), l)
	require.Error(t, err)
	require.Equal(t, int64(2), stored)
	require.Equal(t, int64(2), dropped)
	require.Equal(t, map[string][]string{"a": {"new"}, "b": {"new"}, "c": {"old"}, "fail": {"old"}}, retentionValues(t, c))
	sizes, _ := auditSizes(t, c, audit, "INSERT")
	require.Equal(t, []int64{2}, sizes)
	sizes, _ = auditSizes(t, c, audit, "DELETE")
	require.Equal(t, []int64{2}, sizes)
}

func TestBatchByteLimitAndWholeBatchHeadroom(t *testing.T) {
	c, l := batchTestQueue(t, 100)
	audit := auditQueueBatches(t, c)
	big := strings.Repeat("v", 2<<20)
	stored, _, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", big}, retentionTestItem{"b", big}, retentionTestItem{"c", big}), l)
	require.NoError(t, err)
	require.Equal(t, int64(3), stored)
	sizes, _ := auditSizes(t, c, audit, "INSERT")
	require.Equal(t, []int64{1, 1, 1}, sizes)
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"d", strings.Repeat("v", 4<<20)}), l)
	require.ErrorContains(t, err, "4 MiB")
	// Each input fits individually, but the combined transaction must be checked.
	small := strings.Repeat("v", 700)
	encoded, err := encodeQueueItem(retentionObject(t, c, retentionTestItem{"a", small}), "a")
	require.NoError(t, err)
	var size uint64
	require.NoError(t, c.connPool.QueryRow(context.Background(), "SELECT octet_length($1::jsonb::text)", encoded.data).Scan(&size))
	c.freeSpace = func(string) (uint64, error) {
		return uint64(interfaces.DefaultQueueCriticalFreeBytes) + (1 << 20) + size*6, nil
	}
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"a", small}, retentionTestItem{"b", small}), l)
	require.ErrorContains(t, err, "emergency floor")
}

func TestAcknowledgmentsUseBoundedDeleteStatementsEvenAtEmergencyFloor(t *testing.T) {
	c, l := batchTestQueue(t, 2)
	_, _, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "1"}, retentionTestItem{"b", "2"}, retentionTestItem{"c", "3"},
		retentionTestItem{"d", "4"}, retentionTestItem{"e", "5"}), l)
	require.NoError(t, err)
	items, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	audit := auditQueueBatches(t, c)
	c.freeSpace = func(string) (uint64, error) { return 0, fmt.Errorf("queue filesystem has fewer than 1024 free inodes") }
	count, _, err := c.EnforceLimits(l)
	require.NoError(t, err)
	require.Equal(t, int64(5), count)
	removed, err := c.RemoveFromStoreBatch(items, l)
	require.NoError(t, err)
	require.Equal(t, int64(5), removed)
	sizes, transactions := auditSizes(t, c, audit, "DELETE")
	require.Equal(t, []int64{2, 2, 1}, sizes)
	require.NotEqual(t, transactions[0], transactions[1])
	removed, err = c.RemoveFromStoreBatch(items, l)
	require.NoError(t, err)
	require.Zero(t, removed, "counted already removed/evicted rows a second time")
}

func TestConcurrentBatchesPreserveCapacity(t *testing.T) {
	c, l := batchTestQueue(t, 3)
	_, _, err := c.StoreRetained(retentionObject(t, c,
		retentionTestItem{"a", "1"}, retentionTestItem{"a", "2"}, retentionTestItem{"a", "3"}, retentionTestItem{"quiet", "keep"}), l)
	require.NoError(t, err)
	c.freeSpace = func(string) (uint64, error) { return 1 << 30, nil }
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		item := retentionObject(t, c, retentionTestItem{"a", fmt.Sprint(i)}, retentionTestItem{"a", fmt.Sprint(i + 100)})
		client := *c
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := client.StoreRetained(item, l); failures <- err }()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	values := retentionValues(t, c)
	require.Len(t, values["a"], 3)
	require.Equal(t, []string{"keep"}, values["quiet"])
}

func TestQueueWriteDuringVacuum(t *testing.T) {
	c, l := batchTestQueue(t, 100)
	table := c.getFullStoreTableName()
	_, err := c.connPool.Exec(context.Background(), "ALTER TABLE "+table+" SET (autovacuum_enabled = false)")
	require.NoError(t, err)
	_, err = c.connPool.Exec(context.Background(), "INSERT INTO "+table+" (id,content) SELECT md5(g::text)::uuid,jsonb_build_object('x',repeat('x',128)) FROM generate_series(1,10000) g")
	require.NoError(t, err)
	_, err = c.connPool.Exec(context.Background(), "DELETE FROM "+table)
	require.NoError(t, err)
	conn, err := c.connPool.Acquire(context.Background())
	require.NoError(t, err)
	pid := conn.Conn().PgConn().PID()
	_, err = conn.Exec(context.Background(), "SET vacuum_cost_delay = 50; SET vacuum_cost_limit = 1")
	require.NoError(t, err)
	vacuumCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(vacuumCtx, "VACUUM (TRUNCATE FALSE, INDEX_CLEANUP ON) "+table)
		done <- err
	}()
	defer func() { cancel(); <-done; conn.Release() }()
	require.Eventually(t, func() bool {
		var active bool
		err := c.connPool.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_stat_progress_vacuum WHERE pid=$1)", pid).Scan(&active)
		return err == nil && active
	}, 5*time.Second, 10*time.Millisecond)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	item, err := encodeQueueItem(queueObject(c, "new reading"), "")
	require.NoError(t, err)
	_, err = c.queueTransactionContext(ctx, l, func(ctx context.Context, tx pgx.Tx, l interfaces.QueueLimits) (int64, error) {
		return c.storeQueueBatch(ctx, tx, "pipeline", []queueBatchItem{item}, l)
	})
	require.NoError(t, err, "VACUUM blocked a queue writer")
}
