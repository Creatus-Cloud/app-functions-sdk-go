package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/models"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/postgres/embed"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func queueTestClient(t *testing.T) *Client {
	t.Helper()
	endpoint := os.Getenv("QUEUE_TEST_POSTGRES_URL")
	if endpoint == "" {
		t.Skip("set QUEUE_TEST_POSTGRES_URL to an isolated PostgreSQL database")
	}
	pool, err := pgxpool.New(context.Background(), endpoint)
	require.NoError(t, err)
	schema := "queue-test-" + uuid.NewString()
	c := &Client{connPool: pool, appServiceKey: schema, retention: newRetentionRegistry()}
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`CREATE SCHEMA "%s"`, schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schema))
		pool.Close()
	})
	sql, err := embed.SQLFiles.ReadFile("sql/idempotent/01-tables.sql")
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), strings.ReplaceAll(string(sql), "<serviceKey>", schema))
	require.NoError(t, err)
	return c
}

func queueObject(c *Client, payload string) interfaces.StoredObject {
	o := interfaces.NewStoredObject(c.appServiceKey, []byte(payload), "pipeline", 0, "hash", nil)
	o.ID = uuid.NewString()
	return o
}

func queuePayloads(t *testing.T, c *Client) []string {
	t.Helper()
	items, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	result := make([]string, 0, len(items))
	for _, o := range items {
		result = append(result, string(o.Payload))
	}
	return result
}

func TestQueueRetentionPolicies(t *testing.T) {
	for _, policy := range []string{interfaces.KeepNewest, interfaces.KeepOldest} {
		t.Run(policy, func(t *testing.T) {
			c := queueTestClient(t)
			l := interfaces.QueueLimits{MaxItems: 3, RetentionPolicy: policy}
			dropped := int64(0)
			for i := 0; i < 5; i++ {
				_, n, err := c.StoreWithLimits(queueObject(c, fmt.Sprint(i)), l)
				if policy == interfaces.KeepOldest && i >= 3 {
					require.ErrorContains(t, err, "queue full")
				} else {
					require.NoError(t, err)
				}
				dropped += n
			}
			if policy == interfaces.KeepNewest {
				require.Equal(t, []string{"2", "3", "4"}, queuePayloads(t, c))
				require.Equal(t, int64(2), dropped)
			} else {
				require.Equal(t, []string{"0", "1", "2"}, queuePayloads(t, c))
				require.Zero(t, dropped)
			}
			// A changed Keeper limit is applied to existing objects, without a new write.
			l.MaxItems = 1
			count, n, err := c.EnforceLimits(l)
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
			require.Equal(t, int64(2), n)
			if policy == interfaces.KeepNewest {
				require.Equal(t, []string{"4"}, queuePayloads(t, c))
			} else {
				require.Equal(t, []string{"0"}, queuePayloads(t, c))
			}
		})
	}
}

func TestQueueByteLimitAndOversizedObject(t *testing.T) {
	c := queueTestClient(t)
	first := queueObject(c, strings.Repeat("x", 200))
	var model models.StoredObject
	model.FromContract(first)
	data, err := model.MarshalJSON()
	require.NoError(t, err)
	var size int64
	require.NoError(t, c.connPool.QueryRow(context.Background(), "SELECT octet_length($1::jsonb::text)", data).Scan(&size))
	l := interfaces.QueueLimits{MaxItems: 10, MaxBytes: size * 2}
	for i := 0; i < 3; i++ {
		_, _, err := c.StoreWithLimits(queueObject(c, strings.Repeat("x", 200)), l)
		require.NoError(t, err)
	}
	require.Len(t, queuePayloads(t, c), 2)
	before := queuePayloads(t, c)
	_, _, err = c.StoreWithLimits(queueObject(c, strings.Repeat("y", int(size*4))), l)
	require.ErrorContains(t, err, "exceeds")
	require.Equal(t, before, queuePayloads(t, c))
	var bytes int64
	require.NoError(t, c.connPool.QueryRow(context.Background(), "SELECT sum(octet_length(content::text)) FROM "+c.getFullStoreTableName()).Scan(&bytes))
	require.LessOrEqual(t, bytes, l.MaxBytes)
}

func TestQueueConcurrentWritersAndRetryEviction(t *testing.T) {
	c := queueTestClient(t)
	l := interfaces.QueueLimits{MaxItems: 10}
	var wg sync.WaitGroup
	errors := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := c.StoreWithLimits(queueObject(c, "value"), l); errors <- err }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Len(t, queuePayloads(t, c), 10)
	items, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	victim := items[0]
	l.MaxItems = 1
	_, _, err = c.EnforceLimits(l)
	require.NoError(t, err)
	victim.RetryCount++
	_, err = c.UpdateWithLimits(victim, l)
	require.NoError(t, err)
	remaining, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	require.NotEqual(t, victim.ID, remaining[0].ID)
}

func TestQueueFailedInsertRollsBackEviction(t *testing.T) {
	c := queueTestClient(t)
	l := interfaces.QueueLimits{MaxItems: 2}
	a, b := queueObject(c, "a"), queueObject(c, "b")
	_, _, err := c.StoreWithLimits(a, l)
	require.NoError(t, err)
	_, _, err = c.StoreWithLimits(b, l)
	require.NoError(t, err)
	_, _, err = c.StoreWithLimits(b, l)
	require.Error(t, err)
	require.Equal(t, []string{"a", "b"}, queuePayloads(t, c))
}

func TestLegacyQueueAccountingAndRetryGrowth(t *testing.T) {
	c := queueTestClient(t)
	for _, value := range []string{"a", "b", "c"} {
		o := queueObject(c, value)
		var model models.StoredObject
		model.FromContract(o)
		data, err := model.MarshalJSON()
		require.NoError(t, err)
		_, err = c.connPool.Exec(context.Background(), "INSERT INTO "+c.getFullStoreTableName()+" (id,content) VALUES ($1,$2)", o.ID, data)
		require.NoError(t, err)
	}
	count, dropped, err := c.EnforceLimits(interfaces.QueueLimits{MaxItems: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.Equal(t, int64(2), dropped)
	var size int64
	require.NoError(t, c.connPool.QueryRow(context.Background(), "SELECT octet_length(content::text) FROM "+c.getFullStoreTableName()).Scan(&size))
	items, err := c.RetrieveFromStore(c.appServiceKey)
	require.NoError(t, err)
	items[0].RetryCount = 10000
	n, err := c.UpdateWithLimits(items[0], interfaces.QueueLimits{MaxItems: 1, MaxBytes: size})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Empty(t, queuePayloads(t, c))
}

func TestQueueDiskPressure(t *testing.T) {
	for _, policy := range []string{interfaces.KeepNewest, interfaces.KeepOldest} {
		t.Run(policy, func(t *testing.T) {
			c := queueTestClient(t)
			available := uint64(2 << 30)
			c.freeSpace = func(path string) (uint64, error) { require.Equal(t, "/queue-storage", path); return available, nil }
			l := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage", MinFreeBytes: 1 << 30, RetentionPolicy: policy}
			for _, value := range []string{"a", "b", "c"} {
				_, _, err := c.StoreWithLimits(queueObject(c, value), l)
				require.NoError(t, err)
			}
			available = 1 << 30
			_, dropped, err := c.StoreWithLimits(queueObject(c, "d"), l)
			if policy == interfaces.KeepNewest {
				require.NoError(t, err)
				require.Equal(t, int64(1), dropped)
				require.Equal(t, []string{"b", "c", "d"}, queuePayloads(t, c))
			} else {
				require.ErrorContains(t, err, "keep-oldest")
				require.Equal(t, []string{"a", "b", "c"}, queuePayloads(t, c))
			}
			// Deletions do not necessarily change OS free space. Replace equivalent
			// queued bytes on the next write instead of trying to empty the whole queue.
			_, _, err = c.StoreWithLimits(queueObject(c, "e"), l)
			if policy == interfaces.KeepNewest {
				require.NoError(t, err)
				require.Equal(t, []string{"c", "d", "e"}, queuePayloads(t, c))
			}
			before := queuePayloads(t, c)
			available = 100 << 20
			_, _, err = c.StoreWithLimits(queueObject(c, "f"), l)
			require.Error(t, err)
			require.Equal(t, before, queuePayloads(t, c))
			available = 2 << 30
			_, _, err = c.StoreWithLimits(queueObject(c, "g"), l)
			require.NoError(t, err)
			require.Len(t, queuePayloads(t, c), 4)
			c.freeSpace = func(string) (uint64, error) { return 0, fmt.Errorf("mount missing") }
			_, _, err = c.StoreWithLimits(queueObject(c, "h"), l)
			require.ErrorContains(t, err, "mount missing")
			require.Len(t, queuePayloads(t, c), 4)
		})
	}
}

func TestQueuePageCursorSurvivesDeletion(t *testing.T) {
	c := queueTestClient(t)
	l := interfaces.QueueLimits{}
	for _, value := range []string{"a", "b", "c", "d"} {
		_, _, err := c.StoreWithLimits(queueObject(c, value), l)
		require.NoError(t, err)
	}
	page, err := c.ReadQueuePage(interfaces.QueueCursor{}, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	cursor := page[1].Cursor
	for _, row := range page {
		require.NoError(t, c.RemoveFromStore(row.StoredObject))
	}
	page, err = c.ReadQueuePage(cursor, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.Equal(t, "c", string(page[0].Payload))
	require.Equal(t, "d", string(page[1].Payload))
}

func TestReadingPriorityAndIndependentEmergencyFloor(t *testing.T) {
	readings, notifications := queueTestClient(t), queueTestClient(t)
	available := uint64(8 << 30)
	probe := func(string) (uint64, error) { return available, nil }
	readings.freeSpace = probe
	notifications.freeSpace = probe
	readingLimits := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage", MinFreeBytes: 2 << 30, CriticalFreeBytes: 500 << 20}
	notificationLimits := interfaces.QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage", MinFreeBytes: 1536 << 20, CriticalFreeBytes: 500 << 20}
	for _, pair := range []struct {
		client *Client
		limits interfaces.QueueLimits
	}{{readings, readingLimits}, {notifications, notificationLimits}} {
		for _, value := range []string{"a", "b"} {
			_, _, err := pair.client.StoreWithLimits(queueObject(pair.client, value), pair.limits)
			require.NoError(t, err)
		}
	}
	available = 1792 << 20 // Readings rotate, notifications retain room to grow.
	_, dropped, err := readings.StoreWithLimits(queueObject(readings, "c"), readingLimits)
	require.NoError(t, err)
	require.Equal(t, int64(1), dropped)
	_, dropped, err = notifications.StoreWithLimits(queueObject(notifications, "c"), notificationLimits)
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Equal(t, []string{"b", "c"}, queuePayloads(t, readings))
	require.Equal(t, []string{"a", "b", "c"}, queuePayloads(t, notifications))
	// A high rotation threshold must not stop recycling at half that threshold.
	available = 700 << 20
	_, _, err = readings.StoreWithLimits(queueObject(readings, "d"), readingLimits)
	require.NoError(t, err)
	_, _, err = notifications.StoreWithLimits(queueObject(notifications, "d"), notificationLimits)
	require.NoError(t, err)
	for _, pair := range []struct {
		client *Client
		limits interfaces.QueueLimits
	}{{readings, readingLimits}, {notifications, notificationLimits}} {
		before := queuePayloads(t, pair.client)
		items, err := pair.client.RetrieveFromStore(pair.client.appServiceKey)
		require.NoError(t, err)
		available = 500 << 20
		_, _, err = pair.client.StoreWithLimits(queueObject(pair.client, "e"), pair.limits)
		require.ErrorContains(t, err, "emergency floor")
		items[0].RetryCount++
		_, err = pair.client.UpdateWithLimits(items[0], pair.limits)
		require.ErrorContains(t, err, "emergency floor")
		require.Equal(t, before, queuePayloads(t, pair.client))
		after, err := pair.client.RetrieveFromStore(pair.client.appServiceKey)
		require.NoError(t, err)
		require.Zero(t, after[0].RetryCount)
	}
}
