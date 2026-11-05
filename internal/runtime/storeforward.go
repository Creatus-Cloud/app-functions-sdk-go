//
// Copyright (c) 2022 Intel Corporation
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
//

package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/appfunction"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapContainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	gometrics "github.com/rcrowley/go-metrics"
)

const (
	defaultMinRetryInterval = 1 * time.Second
)

type storeForwardInfo struct {
	runtime         *FunctionsPipelineRuntime
	dic             *di.Container
	lc              logger.LoggingClient
	serviceKey      string
	dataCount       gometrics.Counter
	retryInProgress atomic.Bool
	retryCursor     interfaces.QueueCursor
	retryContext    atomic.Pointer[context.Context]
}

func newStoreAndForward(runtime *FunctionsPipelineRuntime, dic *di.Container, serviceKey string) *storeForwardInfo {
	sf := &storeForwardInfo{
		runtime:    runtime,
		dic:        dic,
		lc:         bootstrapContainer.LoggingClientFrom(dic.Get),
		serviceKey: serviceKey,
		dataCount:  gometrics.NewCounter(),
	}

	metricsManager := bootstrapContainer.MetricsManagerFrom(dic.Get)
	if metricsManager == nil {
		sf.lc.Errorf("Unable to register %s metric: MetricsManager is not available.", internal.StoreForwardQueueSizeName)
		return sf
	}

	err := metricsManager.Register(internal.StoreForwardQueueSizeName, sf.dataCount, nil)
	if err != nil {
		sf.lc.Errorf("Unable to register metric %s. Collection will continue, but metric will not be reported: %v",
			internal.StoreForwardQueueSizeName, err)
	}

	sf.lc.Infof("%s metric has been registered and will be reported (if enabled)", internal.StoreForwardQueueSizeName)

	return sf
}

func (sf *storeForwardInfo) startStoreAndForwardRetryLoop(
	appWg *sync.WaitGroup,
	appCtx context.Context,
	enabledWg *sync.WaitGroup,
	enabledCtx context.Context,
	serviceKey string,
	configuredRetryInterval string) {

	appWg.Add(1)
	enabledWg.Add(1)

	config := container.ConfigurationFrom(sf.dic.Get)
	storeClient := container.StoreClientFrom(sf.dic.Get)

	// Both disabling retries and stopping the application cancel active passes.
	retryCtx, cancelRetry := context.WithCancel(enabledCtx)
	stopAppCancel := context.AfterFunc(appCtx, cancelRetry)
	if appCtx.Err() != nil {
		cancelRetry()
	}
	sf.retryContext.Store(&retryCtx)

	count, err := sf.queueCount(storeClient, serviceKey)
	if err != nil {
		sf.lc.Errorf("Unable to initialize Store and Forward data count: Failed to load items from DB: %v", err)
	} else {
		sf.dataCount.Clear()
		sf.dataCount.Inc(count)
	}

	go func() {
		defer appWg.Done()
		defer enabledWg.Done()
		defer cancelRetry()
		defer stopAppCancel()

		retryInterval, err := time.ParseDuration(configuredRetryInterval)
		if err != nil {
			sf.lc.Warn(
				fmt.Sprintf("StoreAndForward RetryInterval failed to parse, defaulting to %s",
					defaultMinRetryInterval.String()))
			retryInterval = defaultMinRetryInterval
		} else if retryInterval < defaultMinRetryInterval {
			sf.lc.Warn(
				fmt.Sprintf("StoreAndForward RetryInterval value %s is less than the allowed minimum value, defaulting to %s",
					retryInterval.String(), defaultMinRetryInterval.String()))
			retryInterval = defaultMinRetryInterval
		}

		if config.GetStoreAndForwardInfo().MaxRetryCount < 0 {
			sf.lc.Warn("StoreAndForward MaxRetryCount can not be less than 0, defaulting to 1")
			if err := config.SetWritableInfo("StoreAndForward.MaxRetryCount", 1); err != nil {
				sf.lc.Errorf("Failed to set StoreAndForward MaxRetryCount to default 1: %v", err)
			}
		}

		sf.lc.Info(
			fmt.Sprintf("Starting StoreAndForward Retry Loop with %s RetryInterval and %d max retries. %d stored items waiting for retry.",
				retryInterval.String(),
				config.GetStoreAndForwardInfo().MaxRetryCount,
				count))

	exit:
		for {
			select {

			case <-appCtx.Done():
				// Exit the loop and function when application service is terminating.
				break exit

			case <-enabledCtx.Done():
				// Exit the loop and function when Store and Forward has been disabled.
				break exit

			case <-time.After(retryInterval):
				sf.retryStoredData(serviceKey)
			}
		}

		sf.lc.Info("Exiting StoreAndForward Retry Loop")
	}()
}

func (sf *storeForwardInfo) storeForLaterRetry(
	payload []byte,
	appContext interfaces.AppFunctionContext,
	pipeline *interfaces.FunctionPipeline,
	pipelinePosition int) {

	item := interfaces.NewStoredObject(sf.runtime.ServiceKey, payload, pipeline.Id, pipelinePosition, pipeline.Hash, appContext.GetAllValues())
	item.CorrelationID = appContext.CorrelationID()

	sf.lc.Tracef("Storing data for later retry for pipeline '%s' (%s=%s)",
		pipeline.Id,
		common.CorrelationHeader,
		appContext.CorrelationID())

	config := container.ConfigurationFrom(sf.dic.Get)
	settings := config.GetStoreAndForwardInfo()
	if !settings.Enabled {
		sf.lc.Errorf("Failed to store item for later retry for pipeline '%s': StoreAndForward not enabled", pipeline.Id)
		return
	}

	storeClient := container.StoreClientFrom(sf.dic.Get)
	if storeClient == nil {
		sf.lc.Error("Failed to store retry data: StoreAndForward store is not initialized", "pipeline", pipeline.Id)
		return
	}
	// Keeper publishes settings before the reload listener reconciles them.
	// Do not write without limits during that window on an unsupported backend.
	if err := settings.ValidateQueueStore(storeClient); err != nil {
		sf.lc.Error("Failed to store retry data", "pipeline", pipeline.Id, "error", err.Error())
		return
	}

	var err error
	var dropped int64
	var stored int64
	if retained, ok := storeClient.(interfaces.RetentionStoreClient); ok {
		limits, limitErr := settings.QueueLimits()
		if limitErr != nil {
			err = limitErr
		} else {
			stored, dropped, err = retained.StoreRetained(item, limits)
		}
	} else if bounded, ok := storeClient.(interfaces.BoundedStoreClient); ok {
		limits, limitErr := settings.QueueLimits()
		if limitErr != nil {
			err = limitErr
		} else {
			_, dropped, err = bounded.StoreWithLimits(item, limits)
			if err == nil {
				stored = 1
			}
		}
	} else {
		_, err = storeClient.Store(item)
		if err == nil {
			stored = 1
		}
	}
	// A retained payload can span several atomic batches. Earlier committed
	// batches remain queued even if a later batch fails.
	sf.reportDropped(dropped)
	sf.dataCount.Inc(stored - dropped)
	if err != nil {
		sf.lc.Error("Failed to store all retry data", "pipeline", pipeline.Id, "committedItems", stored, "error", err.Error())
		return
	}
}

func (sf *storeForwardInfo) retryStoredData(serviceKey string) {
	// Skip if another thread is already doing the retry
	if !sf.retryInProgress.CompareAndSwap(false, true) {
		return
	}

	defer sf.retryInProgress.Store(false)

	// Capture this generation's context once. A restarted loop must not revive
	// an older pass that is still finishing an export or its acknowledgments.
	parent := context.Background()
	if retryContext := sf.retryContext.Load(); retryContext != nil {
		parent = *retryContext
	}
	if parent.Err() != nil {
		return
	}

	storeClient := container.StoreClientFrom(sf.dic.Get)
	if storeClient == nil {
		sf.lc.Error("Unable to retry stored data: StoreAndForward store is not initialized")
		return
	}
	if err := container.ConfigurationFrom(sf.dic.Get).GetStoreAndForwardInfo().ValidateQueueStore(storeClient); err != nil {
		sf.lc.Errorf("Unable to retry stored data: %v", err)
		return
	}

	// Disk-sized queues must never be loaded entirely into memory. Preserve the
	// cursor across bounded retry passes, including when older rows were evicted.
	if bounded, ok := storeClient.(interfaces.BoundedStoreClient); ok {
		// Only paged stores can resume after a time-limited pass. Other stores
		// still observe cancellation, but must be allowed to reach later rows.
		ctx, cancel := context.WithTimeout(parent, 30*time.Second)
		defer cancel()
		if _, err := sf.queueCount(storeClient, serviceKey); err != nil {
			sf.lc.Errorf("Unable to enforce StoreAndForward queue limits: %v", err)
			return
		}
		for batch := 0; batch < 40; batch++ {
			if ctx.Err() != nil {
				return
			}
			limits, err := container.ConfigurationFrom(sf.dic.Get).GetStoreAndForwardInfo().QueueLimits()
			if err != nil {
				sf.lc.Errorf("Invalid StoreAndForward settings: %v", err)
				return
			}
			page, err := bounded.ReadQueuePage(sf.retryCursor, limits.MaxBatchItems)
			if err != nil {
				sf.lc.Errorf("Unable to read StoreAndForward queue page: %v", err)
				return
			}
			if len(page) == 0 {
				sf.retryCursor = interfaces.QueueCursor{}
				return
			}
			items := make([]interfaces.StoredObject, len(page))
			for i, row := range page {
				items[i] = row.StoredObject
			}
			processed := sf.retryItems(ctx, storeClient, items)
			if processed > 0 {
				sf.retryCursor = page[processed-1].Cursor
			}
			if processed < len(page) || ctx.Err() != nil {
				return
			}
		}
		return
	}

	items, err := storeClient.RetrieveFromStore(serviceKey)
	if err != nil {
		sf.lc.Errorf("Unable to load store and forward items from DB: %s", err.Error())
		return
	}

	sf.retryItems(parent, storeClient, items)
}

// retryItems acknowledges the processed prefix even after cancellation. Entries
// not attempted stay untouched, and the caller advances only past that prefix.
func (sf *storeForwardInfo) retryItems(ctx context.Context, storeClient interfaces.StoreClient, items []interfaces.StoredObject) int {

	sf.lc.Debugf("%d stored data items found for retrying", len(items))

	if len(items) > 0 {
		removed := int64(0)
		itemsToRemove, itemsToUpdate := sf.processRetryItems(ctx, items)

		sf.lc.Debugf(" %d stored data items will be removed post retry", len(itemsToRemove))
		sf.lc.Debugf(" %d stored data items will be updated post retry", len(itemsToUpdate))

		if batchStore, ok := storeClient.(interfaces.BatchStoreRemover); ok && len(itemsToRemove) > 0 {
			limits, err := container.ConfigurationFrom(sf.dic.Get).GetStoreAndForwardInfo().QueueLimits()
			if err == nil {
				removed, err = batchStore.RemoveFromStoreBatch(itemsToRemove, limits)
			}
			if err != nil {
				sf.lc.Error("Unable to remove queued retry batch", "removedItems", removed, "error", err.Error())
			}
		} else {
			for _, item := range itemsToRemove {
				if err := storeClient.RemoveFromStore(item); err != nil {
					sf.lc.Errorf("Unable to remove stored data item for pipeline '%s' from DB, objectID=%s: %s", item.PipelineId, item.ID, err.Error())
				} else {
					removed++
				}
			}
		}

		for _, item := range itemsToUpdate {
			if err := sf.updateStoredItem(storeClient, item); err != nil {
				sf.lc.Errorf("Unable to update stored data item for pipeline '%s' from DB, objectID=%s: %s",
					item.PipelineId,
					err.Error(),
					item.ID)
			}
		}

		sf.dataCount.Dec(removed)
		return len(itemsToRemove) + len(itemsToUpdate)
	}
	return 0
}

func (sf *storeForwardInfo) processRetryItems(ctx context.Context, items []interfaces.StoredObject) ([]interfaces.StoredObject, []interfaces.StoredObject) {
	config := container.ConfigurationFrom(sf.dic.Get)

	var itemsToRemove []interfaces.StoredObject
	var itemsToUpdate []interfaces.StoredObject

	// Item will be removed from store if:
	//    - successfully retried
	//    - max retries exceeded
	//    - version no longer matches current Pipeline
	// Item will not be removed if retry failed and more retries available (hit 'continue' above)
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}
		pipeline := sf.runtime.GetPipelineById(item.PipelineId)

		if pipeline == nil {
			sf.lc.Errorf("Stored data item's pipeline '%s' no longer exists. Removing item from DB", item.PipelineId)
			itemsToRemove = append(itemsToRemove, item)
			continue
		}

		if item.Version != pipeline.Hash {
			sf.lc.Error("Stored data item's pipeline Version doesn't match '%s' pipeline's Version. Removing item from DB", item.PipelineId)
			itemsToRemove = append(itemsToRemove, item)
			continue
		}

		if !sf.retryExportFunction(item, pipeline) {
			item.RetryCount++
			if config.GetStoreAndForwardInfo().MaxRetryCount == 0 ||
				item.RetryCount < config.GetStoreAndForwardInfo().MaxRetryCount {
				sf.lc.Tracef("Export retry failed for pipeline '%s'. retries=%d, Incrementing retry count (%s=%s)",
					item.PipelineId,
					item.RetryCount,
					common.CorrelationHeader,
					item.CorrelationID)
				itemsToUpdate = append(itemsToUpdate, item)
				continue
			}

			sf.lc.Tracef("Max retries exceeded for pipeline '%s'. retries=%d, Removing item from DB (%s=%s)",
				item.PipelineId,
				item.RetryCount,
				common.CorrelationHeader,
				item.CorrelationID)
			itemsToRemove = append(itemsToRemove, item)

			// Note that item will be removed for DB below.
		} else {
			sf.lc.Tracef("Retry successful for pipeline '%s'. Removing item from DB (%s=%s)",
				item.PipelineId,
				common.CorrelationHeader,
				item.CorrelationID)
			itemsToRemove = append(itemsToRemove, item)
		}
	}

	return itemsToRemove, itemsToUpdate
}

func (sf *storeForwardInfo) retryExportFunction(item interfaces.StoredObject, pipeline *interfaces.FunctionPipeline) bool {
	appContext := appfunction.NewContext(item.CorrelationID, sf.dic, "")

	for k, v := range item.ContextData {
		appContext.AddValue(strings.ToLower(k), v)
	}

	sf.lc.Tracef("Retrying stored data for pipeline '%s' (%s=%s)",
		item.PipelineId,
		common.CorrelationHeader,
		appContext.CorrelationID())

	return sf.runtime.ExecutePipeline(
		item.Payload,
		appContext,
		pipeline,
		item.PipelinePosition,
		true) == nil
}

func (sf *storeForwardInfo) triggerRetry() {
	if sf.dataCount.Count() > 0 {
		config := container.ConfigurationFrom(sf.dic.Get)
		if !config.GetStoreAndForwardInfo().Enabled {
			sf.lc.Debug("Store and Forward not enabled, skipping triggering retry of failed data")
			return
		}

		sf.lc.Debug("Triggering Store and Forward retry of failed data")
		sf.retryStoredData(sf.serviceKey)
	}
}

// Queue counts are reconciled at startup/retry; eviction can race an in-flight
// export, so metrics are observations rather than an accounting transaction.
func (sf *storeForwardInfo) queueCount(store interfaces.StoreClient, serviceKey string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("StoreAndForward store is not initialized")
	}
	if bounded, ok := store.(interfaces.BoundedStoreClient); ok {
		limits, err := container.ConfigurationFrom(sf.dic.Get).GetStoreAndForwardInfo().QueueLimits()
		if err != nil {
			return 0, err
		}
		count, dropped, err := bounded.EnforceLimits(limits)
		if err == nil {
			sf.reportDropped(dropped)
			sf.dataCount.Clear()
			sf.dataCount.Inc(count)
		}
		return count, err
	}
	items, err := store.RetrieveFromStore(serviceKey)
	return int64(len(items)), err
}

func (sf *storeForwardInfo) updateStoredItem(store interfaces.StoreClient, item interfaces.StoredObject) error {
	if bounded, ok := store.(interfaces.BoundedStoreClient); ok {
		limits, err := container.ConfigurationFrom(sf.dic.Get).GetStoreAndForwardInfo().QueueLimits()
		if err != nil {
			return err
		}
		dropped, err := bounded.UpdateWithLimits(item, limits)
		if err == nil {
			sf.reportDropped(dropped)
			sf.dataCount.Dec(dropped)
		}
		return err
	}
	return store.Update(item)
}

func (sf *storeForwardInfo) reportDropped(count int64) {
	if count > 0 {
		sf.lc.Warnf("StoreAndForward capacity limit discarded %d queued objects", count)
	}
}
