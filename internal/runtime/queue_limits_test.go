package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/appfunction"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapContainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

type retainedQueueSpy struct{ boundedQueueSpy }

func (s *retainedQueueSpy) ConfigureRetention(context.Context, string, interfaces.RetryDataSplitter, interfaces.QueueLimits) error {
	return nil
}
func (s *retainedQueueSpy) StoreRetained(o interfaces.StoredObject, l interfaces.QueueLimits) (int64, int64, error) {
	s.limits = append(s.limits, l)
	if s.fail {
		return 0, 0, fmt.Errorf("queue transaction failed")
	}
	return 3, 1, nil
}

func TestRetainedQueueMetricsCountIndividualItems(t *testing.T) {
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true, MaxStoredBytes: -1, StoragePath: "/queue-storage"}}}
	store := &retainedQueueSpy{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                   func(di.Get) any { return cfg },
		container.StoreClientName:                     func(di.Get) any { return store },
		bootstrapContainer.LoggingClientInterfaceName: func(di.Get) any { return logger.NewMockClient() },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	ctx := appfunction.NewContext("correlation", services, "")
	pipeline := &interfaces.FunctionPipeline{Id: "pipeline", Hash: "hash"}
	runtime.storeForward.dataCount.Inc(1)
	runtime.storeForward.storeForLaterRetry([]byte("batch"), ctx, pipeline, 0)
	require.Equal(t, int64(3), runtime.storeForward.dataCount.Count())
	require.Equal(t, int64(-1), store.limits[0].MaxBytes)
	store.fail = true
	runtime.storeForward.storeForLaterRetry([]byte("batch"), ctx, pipeline, 0)
	require.Equal(t, int64(3), runtime.storeForward.dataCount.Count())
}

type boundedQueueSpy struct {
	interfaces.StoreClient
	limits    []interfaces.QueueLimits
	fail      bool
	pages     int
	removed   int
	pageSizes []int
}

func (s *boundedQueueSpy) StoreWithLimits(o interfaces.StoredObject, l interfaces.QueueLimits) (string, int64, error) {
	s.limits = append(s.limits, l)
	if s.fail {
		return "", 0, fmt.Errorf("disk full")
	}
	return "id", 0, nil
}
func (s *boundedQueueSpy) UpdateWithLimits(o interfaces.StoredObject, l interfaces.QueueLimits) (int64, error) {
	return 0, nil
}
func (s *boundedQueueSpy) EnforceLimits(l interfaces.QueueLimits) (int64, int64, error) {
	return 2, 0, nil
}
func (s *boundedQueueSpy) ReadQueuePage(after interfaces.QueueCursor, n int) ([]interfaces.QueuedObject, error) {
	s.pageSizes = append(s.pageSizes, n)
	s.pages++
	if s.pages > 2 {
		return nil, nil
	}
	return []interfaces.QueuedObject{{StoredObject: interfaces.StoredObject{ID: fmt.Sprint(s.pages), PipelineId: "deleted-pipeline"}, Cursor: interfaces.QueueCursor{ID: fmt.Sprint(s.pages)}}}, nil
}
func (s *boundedQueueSpy) RemoveFromStore(interfaces.StoredObject) error { s.removed++; return nil }

func TestQueueUsesLiveKeeperSettingsAndPagedReads(t *testing.T) {
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true, StoragePath: "/queue-storage"}}}
	store := &boundedQueueSpy{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                   func(di.Get) any { return cfg },
		container.StoreClientName:                     func(di.Get) any { return store },
		bootstrapContainer.LoggingClientInterfaceName: func(di.Get) any { return logger.NewMockClient() },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	ctx := appfunction.NewContext("correlation", services, "")
	pipeline := &interfaces.FunctionPipeline{Id: "pipeline", Hash: "hash"}
	runtime.storeForward.storeForLaterRetry([]byte("payload"), ctx, pipeline, 0)
	require.Equal(t, interfaces.KeepNewest, store.limits[0].RetentionPolicy)
	require.Equal(t, int64(1536<<20), store.limits[0].MinFreeBytes)
	require.Equal(t, int64(500<<20), store.limits[0].CriticalFreeBytes)
	require.Equal(t, int64(64<<20), store.limits[0].MaxBytes)
	require.Equal(t, 100, store.limits[0].MaxBatchItems)
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MaxBatchItems", 7))
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.RetentionPolicy", interfaces.KeepOldest))
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MaxStoredBytes", int64(-1)))
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MinFreeBytes", int64(700<<20)))
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.CriticalFreeBytes", int64(600<<20)))
	runtime.storeForward.storeForLaterRetry([]byte("payload"), ctx, pipeline, 0)
	require.Equal(t, interfaces.KeepOldest, store.limits[1].RetentionPolicy)
	require.Equal(t, int64(-1), store.limits[1].MaxBytes)
	require.Equal(t, 7, store.limits[1].MaxBatchItems)
	require.Equal(t, int64(700<<20), store.limits[1].MinFreeBytes)
	require.Equal(t, int64(600<<20), store.limits[1].CriticalFreeBytes)
	store.fail = true
	runtime.storeForward.storeForLaterRetry([]byte("payload"), ctx, pipeline, 0)
	require.Equal(t, int64(2), runtime.storeForward.dataCount.Count(), "failed store increased queue metric")
	// An embedded nil legacy StoreClient would panic if retries loaded the whole
	// queue or used the old update API instead of the bounded extension.
	runtime.storeForward.retryStoredData("test")
	require.Equal(t, 3, store.pages)
	require.Equal(t, 2, store.removed)
	require.Equal(t, []int{7, 7, 7}, store.pageSizes)
	require.Empty(t, runtime.storeForward.retryCursor.ID)
}

type partialRetainedQueue struct{ retainedQueueSpy }

func (s *partialRetainedQueue) StoreRetained(interfaces.StoredObject, interfaces.QueueLimits) (int64, int64, error) {
	return 3, 1, fmt.Errorf("second batch failed")
}

func TestQueueCountsEarlierCommittedBatchesAfterFailure(t *testing.T) {
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true}}}
	store := &partialRetainedQueue{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                   func(di.Get) any { return cfg },
		container.StoreClientName:                     func(di.Get) any { return store },
		bootstrapContainer.LoggingClientInterfaceName: func(di.Get) any { return logger.NewMockClient() },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	ctx := appfunction.NewContext("correlation", services, "")
	runtime.storeForward.dataCount.Inc(5)
	runtime.storeForward.storeForLaterRetry([]byte("batch"), ctx, &interfaces.FunctionPipeline{Id: "pipeline", Hash: "hash"}, 0)
	require.Equal(t, int64(7), runtime.storeForward.dataCount.Count())
}

type batchRemovalQueue struct{ boundedQueueSpy }

func (s *batchRemovalQueue) RemoveFromStoreBatch(items []interfaces.StoredObject, l interfaces.QueueLimits) (int64, error) {
	s.limits = append(s.limits, l)
	s.removed += len(items)
	return int64(len(items)), nil
}

func TestRetryAcknowledgesBatchUsingLiveLimit(t *testing.T) {
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true, MaxBatchItems: 3}}}
	store := &batchRemovalQueue{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                   func(di.Get) any { return cfg },
		container.StoreClientName:                     func(di.Get) any { return store },
		bootstrapContainer.LoggingClientInterfaceName: func(di.Get) any { return logger.NewMockClient() },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	runtime.storeForward.retryStoredData("test")
	require.Equal(t, 2, store.removed)
	require.Len(t, store.limits, 2)
	require.Equal(t, 3, store.limits[0].MaxBatchItems)
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MaxBatchItems", 1))
	store.pages = 0
	runtime.storeForward.retryStoredData("test")
	require.Equal(t, 1, store.limits[2].MaxBatchItems)
}
