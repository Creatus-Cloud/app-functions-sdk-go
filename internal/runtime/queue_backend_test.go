package runtime

import (
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/appfunction"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapcontainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

type unboundedWriteStore struct {
	interfaces.StoreClient
	writes int
	reads  int
}

func (s *unboundedWriteStore) Store(interfaces.StoredObject) (string, error) {
	s.writes++
	return "id", nil
}

func (s *unboundedWriteStore) RetrieveFromStore(string) ([]interfaces.StoredObject, error) {
	s.reads++
	return nil, nil
}

func TestUnsupportedLimitsBlockWritesBeforeReloadListenerRuns(t *testing.T) {
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{StoreAndForward: common.StoreAndForwardInfo{Enabled: true}}}
	store := &unboundedWriteStore{}
	services := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationName:                   func(di.Get) any { return cfg },
		container.StoreClientName:                     func(di.Get) any { return store },
		bootstrapcontainer.LoggingClientInterfaceName: func(di.Get) any { return logger.NewMockClient() },
	})
	runtime := NewFunctionPipelineRuntime("test", nil, services)
	ctx := appfunction.NewContext("correlation", services, "")
	pipeline := &interfaces.FunctionPipeline{Id: "pipeline", Hash: "hash"}
	runtime.storeForward.storeForLaterRetry([]byte("allowed"), ctx, pipeline, 0)
	require.Equal(t, 1, store.writes)

	// Publishing the settings precedes the listener's validation/rollback.
	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MaxStoredBytes", int64(1<<20)))
	runtime.storeForward.storeForLaterRetry([]byte("rejected"), ctx, pipeline, 0)
	runtime.storeForward.retryStoredData("test")
	require.Equal(t, 1, store.writes)
	require.Zero(t, store.reads)
	require.EqualValues(t, 1, runtime.storeForward.dataCount.Count())

	require.NoError(t, cfg.SetWritableInfo("StoreAndForward.MaxStoredBytes", int64(0)))
	runtime.storeForward.storeForLaterRetry([]byte("allowed again"), ctx, pipeline, 0)
	require.Equal(t, 2, store.writes)
}
