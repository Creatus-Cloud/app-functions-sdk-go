//
// Copyright (C) 2024 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/models"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/jackc/pgx/v5"
)

const (
	// constants relate to the postgres db table names
	storeTableName = "store"

	// constants relate to the storeObject fields
	appServiceKeyField = "appServiceKey"
)

// getFullStoreTableName returns the table name with schema as prefix
func (c Client) getFullStoreTableName() string {
	return pgx.Identifier{c.appServiceKey, storeTableName}.Sanitize()
}

// Store persists a stored object to the store table and returns the assigned UUID
func (c Client) Store(o interfaces.StoredObject) (string, error) {
	id, _, err := c.StoreWithLimits(o, interfaces.QueueLimits{})
	return id, err
}

// RetrieveFromStore gets an object from the table with content column contains the appServiceKey
func (c Client) RetrieveFromStore(appServiceKey string) ([]interfaces.StoredObject, error) {
	// do not satisfy requests for a blank ASK
	if appServiceKey == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "no AppServiceKey provided", nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queryObj := map[string]any{appServiceKeyField: appServiceKey}
	queryBytes, err := json.Marshal(queryObj)
	if err != nil {
		return nil, wrapDBError("failed to encode appServiceKey query obj", err)
	}

	// query from the store table with content contains {"appServiceKey": appServiceKey}
	rows, err := c.connPool.Query(ctx, sqlQueryContentByJSONField(c.getFullStoreTableName()), queryBytes)
	if err != nil {
		return nil, wrapDBError(fmt.Sprintf("failed to query app svc store record with key '%s'", appServiceKey), err)
	}

	objects, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (interfaces.StoredObject, error) {
		var obj models.StoredObject

		scanErr := row.Scan(&obj)
		if scanErr != nil {
			return interfaces.StoredObject{}, scanErr
		}

		return obj.ToContract(), nil
	})

	if err != nil {
		return nil, wrapDBError("failed to scan app svc store record to StoredObject", err)
	}

	return objects, nil
}

// Update replaces the data currently in the store table by the StoredObject ID
func (c Client) Update(o interfaces.StoredObject) error {
	_, err := c.UpdateWithLimits(o, interfaces.QueueLimits{})
	return err
}

// RemoveFromStore removes an object from the store table by StoredObject ID
func (c Client) RemoveFromStore(o interfaces.StoredObject) error {
	_, err := c.RemoveFromStoreBatch([]interfaces.StoredObject{o}, interfaces.QueueLimits{})
	return err
}

// Disconnect ends the connection.
func (c Client) Disconnect() error {
	c.connPool.Close()
	return nil
}
