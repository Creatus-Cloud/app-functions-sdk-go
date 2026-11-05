package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestClientRetriesAfterInvalidPoolConfiguration(t *testing.T) {
	lc := logger.NewMockClient()
	config := bootstrapconfig.Database{Host: "127.0.0.1", Port: -1}
	_, err := NewClient(context.Background(), config, bootstrapconfig.Credentials{}, lc, "test")
	require.Error(t, err)

	config.Port = 5432
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No server is needed: a valid second attempt must reach Ping and honor
	// cancellation, rather than retain the failed pool creation or panic.
	client, err := NewClient(ctx, config, bootstrapconfig.Credentials{}, lc, "test")
	require.Nil(t, client)
	require.ErrorContains(t, err, "context canceled")
}

func clientLifecycleConfig(t *testing.T) (bootstrapconfig.Database, bootstrapconfig.Credentials, string) {
	t.Helper()
	endpoint := os.Getenv("QUEUE_TEST_POSTGRES_URL")
	if endpoint == "" {
		t.Skip("set QUEUE_TEST_POSTGRES_URL to an isolated PostgreSQL database")
	}
	parsed, err := pgxpool.ParseConfig(endpoint)
	require.NoError(t, err)
	conn := parsed.ConnConfig
	t.Setenv("EDGEX_DBNAME", conn.Database)
	// Normal builds inject this value with ldflags. Use the initial schema
	// version here so an ordinary go test exercises the real constructor.
	previousVersion := internal.SDKVersion
	internal.SDKVersion = initialVersion
	t.Cleanup(func() { internal.SDKVersion = previousVersion })
	admin, err := pgxpool.NewWithConfig(context.Background(), parsed)
	require.NoError(t, err)
	schema := "client-test-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return bootstrapconfig.Database{Host: conn.Host, Port: int(conn.Port)},
		bootstrapconfig.Credentials{Username: conn.User, Password: conn.Password}, schema
}

func TestClientReinitializesAfterRetentionFailure(t *testing.T) {
	config, credentials, schema := clientLifecycleConfig(t)
	ctx := context.Background()
	lc := logger.NewMockClient()
	first, err := NewClient(ctx, config, credentials, lc, schema)
	require.NoError(t, err)
	t.Cleanup(first.CloseSession)
	_, storeErr := first.Store(queueObject(first, "legacy reading"))
	require.NoError(t, storeErr)
	retentionErr := first.ConfigureRetention(ctx, "pipeline", func([]byte) ([]interfaces.RetainedPayload, error) {
		return nil, fmt.Errorf("temporary splitter failure")
	}, interfaces.QueueLimits{})
	require.ErrorContains(t, retentionErr, "temporary splitter failure")
	require.NoError(t, first.Disconnect()) // Same cleanup as initializeStoreClient.

	second, err := NewClient(ctx, config, credentials, lc, schema)
	require.NoError(t, err)
	t.Cleanup(second.CloseSession)
	require.NotSame(t, first.connPool, second.connPool)
	require.NoError(t, second.ConfigureRetention(ctx, "pipeline", func(data []byte) ([]interfaces.RetainedPayload, error) {
		return []interfaces.RetainedPayload{{Payload: data, RetentionKey: "resource"}}, nil
	}, interfaces.QueueLimits{}))
	require.Equal(t, []string{"legacy reading"}, queuePayloads(t, second))
	var key string
	require.NoError(t, second.connPool.QueryRow(ctx, "SELECT retention_key FROM "+second.getFullStoreTableName()).Scan(&key))
	require.Equal(t, "resource", key)
}

func TestClientUsesCorrectedCredentialsAfterFailedInitialization(t *testing.T) {
	config, credentials, schema := clientLifecycleConfig(t)
	lc := logger.NewMockClient()
	wrong := credentials
	wrong.Username = "missing-" + uuid.NewString()
	client, err := NewClient(context.Background(), config, wrong, lc, schema)
	require.Nil(t, client)
	require.Error(t, err)
	client, err = NewClient(context.Background(), config, credentials, lc, schema)
	require.NoError(t, err)
	t.Cleanup(client.CloseSession)
	require.NoError(t, client.connPool.Ping(context.Background()))
}
