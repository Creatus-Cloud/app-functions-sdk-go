package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/postgres/embed"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/stretchr/testify/require"
)

type retentionTestItem struct{ Key, Value string }

func splitRetentionTestItems(data []byte) ([]interfaces.RetainedPayload, error) {
	var batch []retentionTestItem
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, err
	}
	result := make([]interfaces.RetainedPayload, 0, len(batch))
	for _, item := range batch {
		payload, err := json.Marshal([]retentionTestItem{item})
		if err != nil {
			return nil, err
		}
		result = append(result, interfaces.RetainedPayload{Payload: payload, RetentionKey: item.Key})
	}
	return result, nil
}

func retentionObject(t *testing.T, c *Client, items ...retentionTestItem) interfaces.StoredObject {
	t.Helper()
	data, err := json.Marshal(items)
	require.NoError(t, err)
	return queueObject(c, string(data))
}

func retentionValues(t *testing.T, c *Client) map[string][]string {
	t.Helper()
	page, err := c.ReadQueuePage(interfaces.QueueCursor{}, 256)
	require.NoError(t, err)
	result := map[string][]string{}
	for _, row := range page {
		var items []retentionTestItem
		require.NoError(t, json.Unmarshal(row.Payload, &items))
		require.Len(t, items, 1, "each database row must contain one item")
		result[items[0].Key] = append(result[items[0].Key], items[0].Value)
	}
	return result
}

func TestKeyedQueueRotatesSameResourceAndRejectsStaleRetry(t *testing.T) {
	c := queueTestClient(t)
	available := uint64(8 << 30)
	c.freeSpace = func(string) (uint64, error) { return available, nil }
	l := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage"}
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	first := retentionObject(t, c, retentionTestItem{"device-a/temp", "1"}, retentionTestItem{"device-b/temp", "quiet"}, retentionTestItem{"device-a/humidity", "quiet"})
	stored, dropped, err := c.StoreRetained(first, l)
	require.NoError(t, err)
	require.Equal(t, int64(3), stored)
	require.Zero(t, dropped)
	page, err := c.ReadQueuePage(interfaces.QueueCursor{}, 256)
	require.NoError(t, err)
	stale := page[0].StoredObject
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"device-a/temp", "2"}), l)
	require.NoError(t, err)
	available = 1 << 30
	large := strings.Repeat("3", 1000) // One-for-one even when the new value is larger.
	stored, dropped, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"device-a/temp", large}), l)
	require.NoError(t, err)
	require.Equal(t, int64(1), stored)
	require.Equal(t, int64(1), dropped)
	expected := map[string][]string{"device-a/temp": {"2", large}, "device-b/temp": {"quiet"}, "device-a/humidity": {"quiet"}}
	require.Equal(t, expected, retentionValues(t, c))
	stale.RetryCount++
	_, err = c.UpdateWithLimits(stale, l)
	require.NoError(t, err)
	require.Equal(t, expected, retentionValues(t, c), "retry restored an evicted item")
	// A failing later piece rolls back earlier rotations from the same input.
	stored, dropped, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"device-a/temp", "4"}, retentionTestItem{"new/resource", "1"}), l)
	require.ErrorContains(t, err, "same retention key")
	require.Zero(t, stored)
	require.Zero(t, dropped)
	require.Equal(t, expected, retentionValues(t, c))
	l.RetentionPolicy = interfaces.KeepOldest
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"device-a/temp", "4"}), l)
	require.ErrorContains(t, err, "keep-oldest")
	require.Equal(t, expected, retentionValues(t, c))
	l.RetentionPolicy = interfaces.KeepNewest
	available = 500 << 20
	_, _, err = c.StoreRetained(retentionObject(t, c, retentionTestItem{"device-a/temp", "4"}), l)
	require.ErrorContains(t, err, "emergency floor")
	require.Equal(t, expected, retentionValues(t, c))
}

func TestKeyedQueueConcurrentRotation(t *testing.T) {
	c := queueTestClient(t)
	available := uint64(8 << 30)
	c.freeSpace = func(string) (uint64, error) { return available, nil }
	l := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage"}
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	for i := 0; i < 5; i++ {
		_, _, err := c.StoreRetained(retentionObject(t, c, retentionTestItem{"busy", fmt.Sprint(i)}), l)
		require.NoError(t, err)
	}
	_, _, err := c.StoreRetained(retentionObject(t, c, retentionTestItem{"quiet", "unchanged"}), l)
	require.NoError(t, err)
	available = 1 << 30
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		item := retentionObject(t, c, retentionTestItem{"busy", fmt.Sprint(i + 5)})
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := c.StoreRetained(item, l); failures <- err }()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	values := retentionValues(t, c)
	require.Len(t, values["busy"], 5)
	require.Equal(t, []string{"unchanged"}, values["quiet"])
}

func TestRetentionMigrationPreservesBatchAndResumes(t *testing.T) {
	c := queueTestClient(t)
	c.freeSpace = func(string) (uint64, error) { return 8 << 30, nil }
	original := retentionObject(t, c, retentionTestItem{"a", "first"}, retentionTestItem{"b", "second"})
	original.RetryCount = 4
	original.ContextData = map[string]string{"sync_cache": "1"}
	original.CorrelationID = "original-correlation"
	_, _, err := c.StoreWithLimits(original, interfaces.QueueLimits{})
	require.NoError(t, err)
	created := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err = c.connPool.Exec(context.Background(), "UPDATE "+c.getFullStoreTableName()+" SET created=$1 WHERE id=$2", created, original.ID)
	require.NoError(t, err)
	// Upgrade a schema from the earlier queue implementation.
	_, err = c.connPool.Exec(context.Background(), "ALTER TABLE "+c.getFullStoreTableName()+" ADD COLUMN stored_bytes bigint")
	require.NoError(t, err)
	script, err := embed.SQLFiles.ReadFile("sql/idempotent/01-tables.sql")
	require.NoError(t, err)
	_, err = c.connPool.Exec(context.Background(), strings.ReplaceAll(string(script), "<serviceKey>", c.appServiceKey))
	require.NoError(t, err)
	var oldColumn bool
	require.NoError(t, c.connPool.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=$1 AND table_name='store' AND column_name='stored_bytes')", c.appServiceKey).Scan(&oldColumn))
	require.False(t, oldColumn)
	l := interfaces.QueueLimits{MaxBytes: 1, StoragePath: "/queue-storage"} // Migration must not evict history to meet a new cap.
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	page, err := c.ReadQueuePage(interfaces.QueueCursor{}, 256)
	require.NoError(t, err)
	require.Len(t, page, 2)
	for _, row := range page {
		require.NotEqual(t, original.ID, row.ID)
		require.Equal(t, original.RetryCount, row.RetryCount)
		require.Equal(t, original.ContextData, row.ContextData)
		require.Equal(t, original.CorrelationID, row.CorrelationID)
		require.Equal(t, original.Version, row.Version)
		require.True(t, created.Equal(row.Cursor.Created))
	}
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	again, err := c.ReadQueuePage(interfaces.QueueCursor{}, 256)
	require.NoError(t, err)
	require.Equal(t, page, again)
	l.MaxBytes = -1
	_, err = c.UpdateWithLimits(original, l)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"a": {"first"}, "b": {"second"}}, retentionValues(t, c))
}

func TestRetentionMigrationFailurePreservesOriginalBatch(t *testing.T) {
	c := queueTestClient(t)
	original := retentionObject(t, c, retentionTestItem{"a", "first"}, retentionTestItem{"b", "second"})
	_, _, err := c.StoreWithLimits(original, interfaces.QueueLimits{})
	require.NoError(t, err)
	checks := 0
	c.freeSpace = func(string) (uint64, error) {
		checks++
		if checks == 2 {
			return 0, fmt.Errorf("probe failed")
		}
		return 8 << 30, nil
	}
	l := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage"}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, c.ConfigureRetention(canceled, "pipeline", splitRetentionTestItems, l), context.Canceled)
	require.ErrorContains(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l), "probe failed")
	rows, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	require.Equal(t, []interfaces.StoredObject{original}, rows)
	c.freeSpace = func(string) (uint64, error) { return 8 << 30, nil }
	require.NoError(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, l))
	require.Equal(t, map[string][]string{"a": {"first"}, "b": {"second"}}, retentionValues(t, c))
}

func TestRetentionMigrationRejectsInvalidPayloadWithoutDeletingIt(t *testing.T) {
	c := queueTestClient(t)
	original := queueObject(c, "malformed")
	_, _, err := c.StoreWithLimits(original, interfaces.QueueLimits{})
	require.NoError(t, err)
	require.Error(t, c.ConfigureRetention(context.Background(), "pipeline", splitRetentionTestItems, interfaces.QueueLimits{}))
	rows, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	require.Equal(t, []interfaces.StoredObject{original}, rows)
}
