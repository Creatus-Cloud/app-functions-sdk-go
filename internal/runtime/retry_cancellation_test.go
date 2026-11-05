package runtime

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapContainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

type cancellationQueue struct {
	boundedQueueSpy
	rows         []interfaces.QueuedObject
	acknowledged []string
	updated      []interfaces.StoredObject
	reads        []interfaces.QueueCursor
	onRead       func()
}

func (s *cancellationQueue) ReadQueuePage(after interfaces.QueueCursor, n int) ([]interfaces.QueuedObject, error) {
	s.reads = append(s.reads, after)
	if s.onRead != nil {
		s.onRead()
	}
	var page []interfaces.QueuedObject
	for _, row := range s.rows {
		if row.Cursor.ID > after.ID && !slices.Contains(s.acknowledged, row.ID) && len(page) < n {
			page = append(page, row)
		}
	}
	return page, nil
}

func (s *cancellationQueue) EnforceLimits(interfaces.QueueLimits) (int64, int64, error) {
	return int64(len(s.rows) - len(s.acknowledged)), 0, nil
}

func (s *cancellationQueue) RemoveFromStoreBatch(items []interfaces.StoredObject, _ interfaces.QueueLimits) (int64, error) {
	for _, item := range items {
		s.acknowledged = append(s.acknowledged, item.ID)
	}
	return int64(len(items)), nil
}

func (s *cancellationQueue) UpdateWithLimits(item interfaces.StoredObject, _ interfaces.QueueLimits) (int64, error) {
	s.updated = append(s.updated, item)
	return 0, nil
}

func newCancellationRuntime(t *testing.T, export interfaces.AppFunction) (*FunctionsPipelineRuntime, *cancellationQueue) {
	t.Helper()
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true, MaxBatchItems: 100, RetryInterval: "1h"}}}
	store := &cancellationQueue{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                    func(di.Get) any { return cfg },
		container.StoreClientName:                      func(di.Get) any { return store },
		bootstrapContainer.LoggingClientInterfaceName:  func(di.Get) any { return logger.NewMockClient() },
		bootstrapContainer.MetricsManagerInterfaceName: func(di.Get) any { return bootstrapContainer.MetricsManagerFrom(dic.Get) },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	runtime.SetDefaultFunctionsPipeline([]interfaces.AppFunction{export})
	pipeline := runtime.GetDefaultPipeline()
	for i := 1; i <= 3; i++ {
		item := interfaces.NewStoredObject("test", []byte(fmt.Sprint(i)), pipeline.Id, 0, pipeline.Hash, nil)
		item.ID = fmt.Sprint(i)
		store.rows = append(store.rows, interfaces.QueuedObject{StoredObject: item, Cursor: interfaces.QueueCursor{ID: item.ID, Created: time.Unix(int64(i), 0)}})
	}
	return runtime, store
}

func TestRetryCancellationAcknowledgesCompletedItemAndResumesCursor(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%t", fail), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var attempted []string
			runtime, store := newCancellationRuntime(t, func(_ interfaces.AppFunctionContext, data any) (bool, any) {
				attempted = append(attempted, string(data.([]byte)))
				cancel()
				if fail {
					return false, fmt.Errorf("export failed")
				}
				return false, nil
			})
			runtime.storeForward.retryContext.Store(&ctx)
			runtime.storeForward.retryStoredData("test")
			require.Equal(t, []string{"1"}, attempted)
			require.Equal(t, store.rows[0].Cursor, runtime.storeForward.retryCursor)
			if fail {
				require.Empty(t, store.acknowledged)
				require.Len(t, store.updated, 1)
				require.Equal(t, 1, store.updated[0].RetryCount)
			} else {
				require.Equal(t, []string{"1"}, store.acknowledged)
				require.Empty(t, store.updated)
			}
			// Restart with a fresh context. Unprocessed rows must not be skipped.
			restarted := context.Background()
			runtime.storeForward.retryContext.Store(&restarted)
			runtime.storeForward.retryStoredData("test")
			require.Equal(t, []string{"1", "2", "3"}, attempted)
			require.Equal(t, store.rows[0].Cursor, store.reads[1])
			require.Empty(t, runtime.storeForward.retryCursor.ID)
			if fail {
				require.Equal(t, int64(3), runtime.storeForward.dataCount.Count())
			} else {
				require.Zero(t, runtime.storeForward.dataCount.Count())
			}
		})
	}
}

func TestRetryCancellationBeforeFirstItemPreservesCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime, store := newCancellationRuntime(t, func(interfaces.AppFunctionContext, any) (bool, any) {
		t.Error("cancelled pass started an export")
		return false, nil
	})
	runtime.storeForward.retryContext.Store(&ctx)
	runtime.storeForward.retryCursor = store.rows[0].Cursor
	store.onRead = cancel
	runtime.storeForward.retryStoredData("test")
	require.Empty(t, store.acknowledged)
	require.Empty(t, store.updated)
	require.Equal(t, store.rows[0].Cursor, runtime.storeForward.retryCursor)
}

func TestRestartDoesNotReviveCancelledRetryPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sf *storeForwardInfo
	calls := 0
	runtime, store := newCancellationRuntime(t, func(interfaces.AppFunctionContext, any) (bool, any) {
		calls++
		cancel()
		restarted := context.Background()
		sf.retryContext.Store(&restarted)
		return false, nil
	})
	sf = runtime.storeForward
	sf.retryContext.Store(&ctx)
	sf.retryStoredData("test")
	require.Equal(t, 1, calls)
	require.Equal(t, []string{"1"}, store.acknowledged)
	require.Equal(t, store.rows[0].Cursor, sf.retryCursor)
}

func TestRetryPassDeadlineStopsInsidePage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		runtime, store := newCancellationRuntime(t, func(interfaces.AppFunctionContext, any) (bool, any) {
			calls++
			// Virtual time exercises the actual 30-second pass deadline without
			// a slow test or a race against a short real-time timeout.
			time.Sleep(31 * time.Second)
			return false, nil
		})
		runtime.storeForward.retryStoredData("test")
		require.Equal(t, 1, calls)
		require.Equal(t, []string{"1"}, store.acknowledged)
		require.Equal(t, store.rows[0].Cursor, runtime.storeForward.retryCursor)
	})
}

func TestRetryLoopCancellationStopsActivePass(t *testing.T) {
	for _, stopApp := range []bool{false, true} {
		t.Run(fmt.Sprintf("application=%t", stopApp), func(t *testing.T) {
			appCtx, cancelApp := context.WithCancel(context.Background())
			enabledCtx, cancelEnabled := context.WithCancel(context.Background())
			defer cancelApp()
			defer cancelEnabled()
			var sf *storeForwardInfo
			calls := 0
			runtime, store := newCancellationRuntime(t, func(interfaces.AppFunctionContext, any) (bool, any) {
				calls++
				if stopApp {
					cancelApp()
				} else {
					cancelEnabled()
				}
				select {
				case <-(*sf.retryContext.Load()).Done():
				case <-time.After(time.Second):
					t.Error("cancellation did not reach the active retry pass")
				}
				return false, nil
			})
			sf = runtime.storeForward
			var appWg, enabledWg sync.WaitGroup
			sf.startStoreAndForwardRetryLoop(&appWg, appCtx, &enabledWg, enabledCtx, "test", "1h")
			sf.retryStoredData("test")
			stopped := make(chan struct{})
			go func() {
				appWg.Wait()
				enabledWg.Wait()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("retry loop did not stop")
			}
			require.Equal(t, 1, calls)
			require.Equal(t, []string{"1"}, store.acknowledged)
			require.Equal(t, store.rows[0].Cursor, sf.retryCursor)
		})
	}
}

type unpagedRetryQueue struct {
	interfaces.StoreClient
	items []interfaces.StoredObject
}

func (s *unpagedRetryQueue) RetrieveFromStore(string) ([]interfaces.StoredObject, error) {
	return slices.Clone(s.items), nil
}

func (s *unpagedRetryQueue) Update(item interfaces.StoredObject) error {
	for i := range s.items {
		if s.items[i].ID == item.ID {
			s.items[i] = item
		}
	}
	return nil
}

func (s *unpagedRetryQueue) RemoveFromStore(item interfaces.StoredObject) error {
	s.items = slices.DeleteFunc(s.items, func(queued interfaces.StoredObject) bool { return queued.ID == item.ID })
	return nil
}

func TestUnpagedStoreRetriesPastSlowFailingFirstItem(t *testing.T) {
	var attempted []string
	runtime, queue := newCancellationRuntime(t, func(_ interfaces.AppFunctionContext, data any) (bool, any) {
		id := string(data.([]byte))
		attempted = append(attempted, id)
		if id == "1" {
			time.Sleep(31 * time.Second)
			return false, fmt.Errorf("slow export failure")
		}
		return false, nil
	})
	store := &unpagedRetryQueue{}
	for _, row := range queue.rows {
		store.items = append(store.items, row.StoredObject)
	}
	runtime.dic.Update(di.ServiceConstructorMap{container.StoreClientName: func(di.Get) any { return store }})
	require.NoError(t, container.ConfigurationFrom(runtime.dic.Get).SetWritableInfo("StoreAndForward.MaxBatchItems", 0))
	runtime.storeForward.dataCount.Inc(3)
	synctest.Test(t, func(t *testing.T) {
		for i := 0; i < 3; i++ {
			runtime.storeForward.retryStoredData("test")
		}
		require.Equal(t, []string{"1", "2", "3", "1", "1"}, attempted)
		require.Len(t, store.items, 1)
		require.Equal(t, 3, store.items[0].RetryCount)
		require.Equal(t, int64(1), runtime.storeForward.dataCount.Count())
	})
}

func TestUnpagedStoreStillStopsBetweenItemsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	runtime, queue := newCancellationRuntime(t, func(interfaces.AppFunctionContext, any) (bool, any) {
		calls++
		cancel()
		return false, nil
	})
	store := &unpagedRetryQueue{}
	for _, row := range queue.rows {
		store.items = append(store.items, row.StoredObject)
	}
	runtime.dic.Update(di.ServiceConstructorMap{container.StoreClientName: func(di.Get) any { return store }})
	require.NoError(t, container.ConfigurationFrom(runtime.dic.Get).SetWritableInfo("StoreAndForward.MaxBatchItems", 0))
	runtime.storeForward.retryContext.Store(&ctx)
	runtime.storeForward.retryStoredData("test")
	require.Equal(t, 1, calls)
	require.Len(t, store.items, 2)
	require.Equal(t, "2", store.items[0].ID)
}
