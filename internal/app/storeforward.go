//
// Copyright (c) 2021 One Track Consulting
// Copyright (C) 2024-2025 IOTech Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/postgres"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/store/db/redis"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"

	bootstrapContainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/environment"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/secret"
	bootstrapConfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
)

// RegisterCustomStoreFactory allows registration of alternative storage implementation to back the Store&Forward loop
func (svc *Service) RegisterCustomStoreFactory(name string, factory func(cfg bootstrapConfig.Database, cred bootstrapConfig.Credentials) (interfaces.StoreClient, error)) error {
	if name == db.RedisDB {
		return fmt.Errorf("cannot register factory for reserved name %q", name)
	}

	if svc.customStoreClientFactories == nil {
		svc.customStoreClientFactories = make(map[string]func(db bootstrapConfig.Database, cred bootstrapConfig.Credentials) (interfaces.StoreClient, error), 1)
	}

	svc.customStoreClientFactories[strings.ToUpper(name)] = factory

	return nil
}

// RegisterStoreForwardRetention configures independent queue items before Run.
func (svc *Service) RegisterStoreForwardRetention(pipelineID string, split interfaces.RetryDataSplitter) error {
	svc.storeForwardMu.Lock()
	defer svc.storeForwardMu.Unlock()
	if svc.storeForwardReady || svc.stopHandler() != nil {
		return fmt.Errorf("StoreAndForward retention must be registered before Run")
	}
	if split == nil || svc.runtime == nil || svc.runtime.GetPipelineById(pipelineID) == nil {
		return fmt.Errorf("StoreAndForward retention requires an existing pipeline and a splitter")
	}
	if svc.storeForwardRetention == nil {
		svc.storeForwardRetention = make(map[string]interfaces.RetryDataSplitter)
	}
	if _, exists := svc.storeForwardRetention[pipelineID]; exists {
		return fmt.Errorf("StoreAndForward retention already registered for pipeline %s", pipelineID)
	}
	svc.storeForwardRetention[pipelineID] = split
	return nil
}

func (svc *Service) createStoreClient(database bootstrapConfig.Database, credentials bootstrapConfig.Credentials) (interfaces.StoreClient, error) {
	switch strings.ToLower(database.Type) {
	case db.RedisDB:
		return redis.NewClient(database, credentials)
	case db.Postgres:
		return postgres.NewClient(svc.ctx.appCtx, database, credentials, svc.lc, svc.serviceKey)
	default:
		if factory, found := svc.customStoreClientFactories[strings.ToUpper(database.Type)]; found {
			return factory(database, credentials)
		}
		return nil, db.ErrUnsupportedDatabase
	}
}

// All store initialization and loop transitions share a lock with Run. Capture
// startup settings inside it so startup cannot apply an older snapshot after
// the listener has already finished processing a newer configuration.
func (svc *Service) prepareStoreForwardStore() error {
	svc.storeForwardMu.Lock()
	defer svc.storeForwardMu.Unlock()
	// Bootstrap starts the config listener before the application registers
	// pipelines and retention. Only Run may open this initialization gate.
	svc.storeForwardReady = true
	return initializeStoreClient(svc.config.GetStoreAndForwardInfo(), svc)
}

func (svc *Service) applyCurrentStoreForwardSettings() error {
	svc.storeForwardMu.Lock()
	defer svc.storeForwardMu.Unlock()
	return svc.applyStoreForwardSettingsLocked(svc.config.GetStoreAndForwardInfo())
}

func (svc *Service) applyStoreForwardSettings(settings common.StoreAndForwardInfo, revision uint64) error {
	svc.storeForwardMu.Lock()
	defer svc.storeForwardMu.Unlock()
	err := svc.applyStoreForwardSettingsLocked(settings)
	// Roll back before releasing the lifecycle lock: startup must not recover
	// this same enable and then have its successful initialization rolled back.
	if err != nil && svc.ctx.appCtx.Err() == nil {
		if !svc.config.DisableStoreAndForwardIfRevision(revision) {
			svc.lc.Debug("Preserving newer configuration after Store and Forward initialization failed")
		}
	}
	return err
}

func (svc *Service) applyStoreForwardSettingsLocked(settings common.StoreAndForwardInfo) error {
	if err := svc.ctx.appCtx.Err(); err != nil {
		return err
	}
	if !svc.storeForwardReady {
		return nil
	}
	if !settings.Enabled {
		svc.stopStoreForwardLocked()
		return nil
	}
	// Validate reused clients on every reload, even when the retry timer stays
	// unchanged. Rejecting new limits must also stop an existing retry loop.
	if err := initializeStoreClient(settings, svc); err != nil {
		svc.stopStoreForwardLocked()
		return err
	}
	if svc.ctx.storeForwardCancelCtx != nil {
		if svc.storeForwardRetryInterval == settings.RetryInterval {
			return nil
		}
		if interval, err := time.ParseDuration(settings.RetryInterval); err != nil || interval <= 0 {
			svc.lc.Error("invalid StoreAndForward RetryInterval; keeping the current retry loop", "interval", settings.RetryInterval)
			return nil
		}
		svc.stopStoreForwardLocked()
	}
	if err := svc.ctx.appCtx.Err(); err != nil {
		return err
	}

	var storeForwardEnabledCtx context.Context
	svc.ctx.storeForwardWg = &sync.WaitGroup{}
	storeForwardEnabledCtx, svc.ctx.storeForwardCancelCtx = context.WithCancel(svc.ctx.appCtx)
	svc.storeForwardRetryInterval = settings.RetryInterval
	svc.runtime.StartStoreAndForward(svc.ctx.appWg, svc.ctx.appCtx, svc.ctx.storeForwardWg, storeForwardEnabledCtx, svc.serviceKey, settings.RetryInterval)
	return nil
}

func (svc *Service) stopStoreForward() {
	svc.storeForwardMu.Lock()
	defer svc.storeForwardMu.Unlock()
	svc.stopStoreForwardLocked()
}

func (svc *Service) stopStoreForwardLocked() {
	if svc.ctx.storeForwardCancelCtx != nil {
		svc.lc.Info("Canceling Store and Forward retry loop")
		svc.ctx.storeForwardCancelCtx()
	}
	if svc.ctx.storeForwardWg != nil {
		svc.ctx.storeForwardWg.Wait()
	}
	svc.ctx.storeForwardCancelCtx = nil
	svc.storeForwardRetryInterval = ""
}

// Caller holds storeForwardMu. Reuse a client initialized by the other startup
// path; disabling retries must not erase it from the dependency container.
func initializeStoreClient(settings common.StoreAndForwardInfo, svc *Service) error {
	if err := svc.ctx.appCtx.Err(); err != nil {
		return err
	}
	if !settings.Enabled {
		return nil
	}
	if store := container.StoreClientFrom(svc.dic.Get); store != nil {
		return settings.ValidateQueueStore(store)
	}
	if _, err := settings.QueueLimits(); err != nil {
		return err
	}

	logger := bootstrapContainer.LoggingClientFrom(svc.dic.Get)
	secretProvider := bootstrapContainer.SecretProviderFrom(svc.dic.Get)

	var err error

	database := svc.config.Database
	secrets, err := secretProvider.GetSecret(database.Type)
	if cancelErr := svc.ctx.appCtx.Err(); cancelErr != nil {
		return cancelErr
	}
	if err != nil {
		return fmt.Errorf("unable to get Database Credentials for Store and Forward: %s", err.Error())
	}

	credentials := bootstrapConfig.Credentials{
		Username: secrets[secret.UsernameKey],
		Password: secrets[secret.PasswordKey],
	}

	startup := environment.GetStartupInfo(svc.serviceKey)

	tryUntil := time.Now().Add(time.Duration(startup.Duration) * time.Second)

	var storeClient interfaces.StoreClient
	for time.Now().Before(tryUntil) {
		if cancelErr := svc.ctx.appCtx.Err(); cancelErr != nil {
			return cancelErr
		}
		if storeClient, err = svc.createStoreClient(database, credentials); err != nil {
			logger.Warnf("unable to initialize Database '%s' for Store and Forward: %s", database.Type, err.Error())
			select {
			case <-svc.ctx.appCtx.Done():
				return svc.ctx.appCtx.Err()
			case <-time.After(time.Duration(startup.Interval) * time.Second):
			}
			continue
		}
		break
	}

	if err != nil {
		return fmt.Errorf("initialize Database for Store and Forward failed: %s", err.Error())
	}
	if storeClient == nil {
		return fmt.Errorf("initialize Database for Store and Forward returned no client")
	}
	if cancelErr := svc.ctx.appCtx.Err(); cancelErr != nil {
		_ = storeClient.Disconnect()
		return cancelErr
	}

	if err := settings.ValidateQueueStore(storeClient); err != nil {
		_ = storeClient.Disconnect()
		return err
	}
	for pipelineID, split := range svc.storeForwardRetention {
		retained, ok := storeClient.(interfaces.RetentionStoreClient)
		if !ok {
			_ = storeClient.Disconnect()
			return fmt.Errorf("StoreAndForward keyed retention requires PostgreSQL")
		}
		queueLimits, err := settings.QueueLimits()
		if err == nil {
			err = retained.ConfigureRetention(svc.ctx.appCtx, pipelineID, split, queueLimits)
		}
		if err != nil {
			_ = storeClient.Disconnect()
			return fmt.Errorf("prepare StoreAndForward retention for %s: %w", pipelineID, err)
		}
	}
	if cancelErr := svc.ctx.appCtx.Err(); cancelErr != nil {
		_ = storeClient.Disconnect()
		return cancelErr
	}
	svc.dic.Update(di.ServiceConstructorMap{
		container.StoreClientName: func(get di.Get) interface{} {
			return storeClient
		},
	})
	return nil
}
