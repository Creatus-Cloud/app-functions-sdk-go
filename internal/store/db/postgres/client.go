//
// Copyright (C) 2024-2025 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"net/url"
	"os"

	bootstrapConfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/postgres/embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDBName = "edgex_db"

type Client struct {
	connPool      *pgxpool.Pool
	loggingClient logger.LoggingClient
	appServiceKey string
	freeSpace     func(string) (uint64, error)
	retention     *retentionRegistry
}

// NewClient returns a pointer to the Postgres client
func NewClient(ctx context.Context, config bootstrapConfig.Database, credentials bootstrapConfig.Credentials,
	lc logger.LoggingClient, serviceKey string) (*Client, errors.EdgeX) {
	// Get the database name from the environment variable
	databaseName := os.Getenv("EDGEX_DBNAME")
	if databaseName == "" {
		databaseName = defaultDBName
	}

	// Each initialization attempt owns its pool. The SDK retains successful
	// clients; a failed attempt must not cache closed pools or old credentials.
	connectionStr := "postgres://" + fmt.Sprintf("%s:%s@%s:%d/%s", url.PathEscape(credentials.Username), url.PathEscape(credentials.Password), url.PathEscape(config.Host), config.Port, url.PathEscape(databaseName))
	dbPool, err := pgxpool.New(ctx, connectionStr)
	if err != nil {
		return nil, wrapDBError("fail to create pg connection pool", err)
	}
	initialized := false
	defer func() {
		if !initialized {
			dbPool.Close()
		}
	}()
	dc := &Client{
		connPool:      dbPool,
		loggingClient: lc,
		appServiceKey: serviceKey,
		retention:     newRetentionRegistry(),
	}

	// invoke Ping to test the connectivity to the DB
	if err := dc.connPool.Ping(ctx); err != nil {
		return nil, wrapDBError("failed to acquire a connection from database connection pool", err)
	}

	// create a new TableManager instance
	tableManager, err := NewTableManager(dc.connPool, lc, serviceKey, serviceKey, internal.SDKVersion, embed.SQLFiles)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.KindDatabaseError, "failed to create a new TableManager instance", err)
	}

	err = tableManager.RunScripts(ctx)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.KindDatabaseError, "TableManager failed to run SQL scripts", err)
	}

	lc.Info("Successfully connected to Postgres database")

	initialized = true
	return dc, nil
}

// CloseSession closes the connections to postgres
func (c Client) CloseSession() {
	c.connPool.Close()
}
