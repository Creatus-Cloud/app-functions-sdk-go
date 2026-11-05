package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/runtime"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/config"
	bootstrapcontainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	bootstrapinterfaces "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

type configUpdateStore struct {
	interfaces.StoreClient
	starts atomic.Int32
}

func (s *configUpdateStore) RetrieveFromStore(string) ([]interfaces.StoredObject, error) {
	s.starts.Add(1)
	return nil, nil
}

type configUpdateMetrics struct {
	bootstrapinterfaces.MetricsManager
}

func (m *configUpdateMetrics) Register(string, any, map[string]string) error { return nil }

type configUpdateSecrets struct {
	bootstrapinterfaces.SecretProvider
}

func (s *configUpdateSecrets) GetSecret(string, ...string) (map[string]string, error) {
	return nil, errors.New("database secret unavailable")
}

func newConfigUpdateService(t *testing.T, enabled bool) (*Service, *configUpdateStore) {
	t.Helper()
	log := logger.NewMockClient()
	cfg := &common.ConfigurationStruct{Writable: common.WritableInfo{
		StoreAndForward: common.StoreAndForwardInfo{Enabled: enabled, RetryInterval: "1h", MaxRetryCount: 5},
		Pipeline: common.PipelineInfo{ExecutionOrder: "SetResponseData", Functions: map[string]common.PipelineFunction{
			"SetResponseData": {},
		}},
	}}
	store := &configUpdateStore{}
	services := di.NewContainer(di.ServiceConstructorMap{
		bootstrapcontainer.LoggingClientInterfaceName:  func(di.Get) any { return log },
		bootstrapcontainer.MetricsManagerInterfaceName: func(di.Get) any { return &configUpdateMetrics{} },
		bootstrapcontainer.SecretProviderName:          func(di.Get) any { return &configUpdateSecrets{} },
		container.ConfigurationName:                    func(di.Get) any { return cfg },
		container.StoreClientName:                      func(di.Get) any { return store },
	})
	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		config: cfg, lc: log, dic: services, serviceKey: "config-reload-test", usingConfigurablePipeline: true, storeForwardReady: true,
		ctx:     contextGroup{appCtx: ctx, appCancelCtx: cancel, appWg: &sync.WaitGroup{}},
		runtime: runtime.NewFunctionPipelineRuntime("config-reload-test", nil, services),
	}
	pipelines, err := svc.LoadConfigurableFunctionPipelines()
	require.NoError(t, err)
	svc.runtime.SetDefaultFunctionsPipeline(pipelines[interfaces.DefaultPipelineId].Transforms)
	t.Cleanup(func() {
		cancel()
		svc.ctx.appWg.Wait()
	})
	if enabled {
		require.NoError(t, svc.applyCurrentStoreForwardSettings())
	}
	return svc, store
}

func changedConfigUpdatePipeline(current *common.WritableInfo) {
	current.Pipeline.ExecutionOrder = "FilterByDeviceName,SetResponseData"
	current.Pipeline.Functions["FilterByDeviceName"] = common.PipelineFunction{
		Parameters: map[string]string{"DeviceNames": "sensor"},
	}
}

func TestConfigReloadAppliesStoreForwardAndPipelineTogether(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wasEnabled  bool
		enabled     bool
		interval    string
		addedStarts int32
	}{
		{name: "enable with new interval", enabled: true, interval: "2h", addedStarts: 1},
		{name: "disable with new interval", wasEnabled: true, interval: "2h"},
		{name: "restart for new interval", wasEnabled: true, enabled: true, interval: "2h", addedStarts: 1},
		{name: "invalid interval still applies pipeline", wasEnabled: true, enabled: true, interval: "bad"},
		{name: "negative interval still applies pipeline", wasEnabled: true, enabled: true, interval: "-1s"},
		{name: "disabled interval change does not start loop", interval: "2h"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := newConfigUpdateService(t, tc.wasEnabled)
			before := store.starts.Load()
			previous := svc.config.GetWritableInfo()
			current := svc.config.GetWritableInfo()
			current.StoreAndForward.Enabled = tc.enabled
			current.StoreAndForward.MaxRetryCount = 8
			current.StoreAndForward.RetryInterval = tc.interval
			changedConfigUpdatePipeline(&current)
			require.True(t, svc.config.UpdateWritableFromRaw(&current))
			current, revision := svc.config.GetWritableInfoWithRevision()
			NewConfigUpdateProcessor(svc).processConfigChanges(previous, current, revision)
			require.Equal(t, before+tc.addedStarts, store.starts.Load(), "loop must transition only once per reload")
			require.Len(t, svc.runtime.GetDefaultPipeline().Transforms, 2, "StoreAndForward change must not mask pipeline update")
			if !tc.enabled && tc.wasEnabled {
				// processConfigChanges must cancel the old loop without waiting for application shutdown.
				svc.ctx.storeForwardWg.Wait()
			}
		})
	}
}

func TestConfigReloadStoreFailureDoesNotSkipPipelineOrPreventRetry(t *testing.T) {
	svc, store := newConfigUpdateService(t, false)
	svc.dic.Update(di.ServiceConstructorMap{container.StoreClientName: func(di.Get) any { return nil }})
	previous := svc.config.GetWritableInfo()
	current := svc.config.GetWritableInfo()
	current.StoreAndForward.Enabled = true
	changedConfigUpdatePipeline(&current)
	require.True(t, svc.config.UpdateWritableFromRaw(&current))
	processor := NewConfigUpdateProcessor(svc)
	current, revision := svc.config.GetWritableInfoWithRevision()
	applied := processor.processConfigChanges(previous, current, revision)
	require.False(t, svc.config.GetStoreAndForwardInfo().Enabled, "failed initialization must disable storing")
	require.False(t, applied.StoreAndForward.Enabled, "baseline must allow the next enable to retry initialization")
	require.Len(t, svc.runtime.GetDefaultPipeline().Transforms, 2)
	require.NotPanics(t, svc.stopStoreForward, "a failed enable did not create a retry waitgroup")

	svc.dic.Update(di.ServiceConstructorMap{container.StoreClientName: func(di.Get) any { return store }})
	require.True(t, svc.config.UpdateWritableFromRaw(&current))
	current, revision = svc.config.GetWritableInfoWithRevision()
	processor.processConfigChanges(applied, current, revision)
	require.EqualValues(t, 1, store.starts.Load())
}

func TestConfigListenerProcessesFirstUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, _ := newConfigUpdateService(t, false)
		notifications := make(bootstrapconfig.UpdatedStream)
		NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
		// Do not wait for the listener goroutine to run before publishing.
		current := svc.config.GetWritableInfo()
		changedConfigUpdatePipeline(&current)
		require.True(t, svc.config.UpdateWritableFromRaw(&current))
		notifications <- struct{}{}
		synctest.Wait()
		require.Len(t, svc.runtime.GetDefaultPipeline().Transforms, 2)
		close(notifications)
		svc.ctx.appWg.Wait() // Closing the stream must also stop the listener.
	})
}

func TestConfigListenerDoesNotSwallowUpdatePublishedDuringProcessing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, store := newConfigUpdateService(t, false)
		log := &configUpdateBarrierLogger{LoggingClient: svc.lc, entered: make(chan struct{}), release: make(chan struct{})}
		svc.lc = log
		notifications := make(bootstrapconfig.UpdatedStream)
		NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
		first := svc.config.GetWritableInfo()
		first.StoreAndForward.MaxRetryCount = 9
		require.True(t, svc.config.UpdateWritableFromRaw(&first))
		notifications <- struct{}{}
		<-log.entered
		second := svc.config.GetWritableInfo()
		second.StoreAndForward.Enabled = true
		require.True(t, svc.config.UpdateWritableFromRaw(&second))
		close(log.release)
		notifications <- struct{}{}
		synctest.Wait()
		require.EqualValues(t, 1, store.starts.Load(), "later published state must not become the baseline before processing its signal")
		close(notifications)
		svc.stopStoreForward()
		svc.ctx.appWg.Wait()
	})
}

type configUpdateBarrierLogger struct {
	logger.LoggingClient
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func TestConfigListenerDoesNotOrphanRetryLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, _ := newConfigUpdateService(t, true)
		originalLoop := svc.ctx.storeForwardWg
		log := &configUpdateBarrierLogger{LoggingClient: svc.lc, entered: make(chan struct{}), release: make(chan struct{})}
		svc.lc = log
		notifications := make(bootstrapconfig.UpdatedStream)
		NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
		first := svc.config.GetWritableInfo()
		first.StoreAndForward.MaxRetryCount = 9
		first.StoreAndForward.Enabled = false
		require.True(t, svc.config.UpdateWritableFromRaw(&first))
		notifications <- struct{}{}
		<-log.entered
		second := svc.config.GetWritableInfo()
		second.StoreAndForward.Enabled = true
		require.True(t, svc.config.UpdateWritableFromRaw(&second))
		close(log.release)
		notifications <- struct{}{}
		synctest.Wait()
		svc.stopStoreForward()
		stopped := make(chan struct{})
		go func() { originalLoop.Wait(); close(stopped) }()
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Error("overlapping disable and enable orphaned the original retry loop")
		}
		close(notifications)
	})
}

func (l *configUpdateBarrierLogger) Infof(format string, args ...any) {
	if format == "StoreAndForward MaxRetryCount changed to %d" {
		l.once.Do(func() { close(l.entered); <-l.release })
	}
	l.LoggingClient.Infof(format, args...)
}
