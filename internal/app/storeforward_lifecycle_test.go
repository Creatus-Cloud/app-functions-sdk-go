package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/webserver"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/config"
	bootstrapcontainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	bootstrapmocks "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces/mocks"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lifecycleTrigger struct {
	initialize func()
	err        error
}

func (tr *lifecycleTrigger) Initialize(*sync.WaitGroup, context.Context, <-chan interfaces.BackgroundMessage) (bootstrap.Deferred, error) {
	if tr.initialize != nil {
		tr.initialize()
	}
	return func() {}, tr.err
}

type lifecycleStateLogger struct {
	logger.LoggingClient
	applied chan bool
}

func (l *lifecycleStateLogger) Infof(format string, args ...any) {
	l.LoggingClient.Infof(format, args...)
	if format == "StoreAndForward Enabled changed to %v" {
		l.applied <- args[0].(bool)
	}
}

func configureLifecycleRun(t *testing.T, svc *Service, trigger *lifecycleTrigger) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(svc.ctx.appCtx)
	svc.ctx.appCtx, svc.ctx.appCancelCtx = ctx, cancel
	svc.config.Service.RequestTimeout = "invalid-timeout"
	svc.config.Trigger.Type = "lifecycle"
	require.NoError(t, svc.RegisterCustomTriggerFactory("lifecycle", func(interfaces.TriggerConfig) (interfaces.Trigger, error) { return trigger, nil }))
	svc.webserver = webserver.NewWebServer(svc.dic, echo.New(), svc.serviceKey)
	return cancel
}

func TestShutdownDuringStoreInitialization(t *testing.T) {
	svc, _ := newConfigUpdateService(t, false)
	cancel := configureLifecycleRun(t, svc, &lifecycleTrigger{})
	defer cancel()
	secrets := &reloadInitializationSecrets{entered: make(chan struct{}), release: make(chan struct{})}
	svc.dic.Update(di.ServiceConstructorMap{
		container.StoreClientName:             func(di.Get) any { return nil },
		bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
	})
	notifications := make(bootstrapconfig.UpdatedStream)
	NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
	svc.startupHooks = append(svc.startupHooks, func() error {
		next := svc.config.GetWritableInfo()
		next.StoreAndForward.Enabled = true
		require.True(t, svc.config.UpdateWritableFromRaw(&next))
		notifications <- struct{}{}
		<-secrets.entered
		// A correctly coordinated shutdown cancels initialization before waiting.
		go func() { <-svc.ctx.appCtx.Done(); close(secrets.release) }()
		return nil
	})
	assert.NotPanics(t, func() { _ = svc.Run() }, "shutdown must tolerate an enable whose store is still initializing")
	cancel()
	svc.ctx.appWg.Wait()
}

func TestStartupDoesNotDuplicateRetryLoop(t *testing.T) {
	svc, store := newConfigUpdateService(t, false)
	applied := make(chan bool, 2)
	svc.lc = &lifecycleStateLogger{LoggingClient: svc.lc, applied: applied}
	trigger := &lifecycleTrigger{}
	cancel := configureLifecycleRun(t, svc, trigger)
	defer cancel()
	svc.config.Database.Type = "lifecycle-store"
	require.NoError(t, svc.RegisterCustomStoreFactory("lifecycle-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) { return store, nil }))
	secrets := &bootstrapmocks.SecretProvider{}
	secrets.On("GetSecret", "lifecycle-store").Return(map[string]string{"username": "test", "password": "test"}, nil)
	svc.dic.Update(di.ServiceConstructorMap{bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets }})
	notifications := make(bootstrapconfig.UpdatedStream)
	NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
	var firstLoop *sync.WaitGroup
	trigger.initialize = func() {
		next := svc.config.GetWritableInfo()
		next.StoreAndForward.Enabled = true
		require.True(t, svc.config.UpdateWritableFromRaw(&next))
		notifications <- struct{}{}
		require.True(t, <-applied)
		firstLoop = svc.ctx.storeForwardWg
	}
	svc.startupHooks = append(svc.startupHooks, func() error {
		assert.EqualValues(t, 1, store.starts.Load(), "Run must reuse the loop already started by the config listener")
		next := svc.config.GetWritableInfo()
		next.StoreAndForward.Enabled = false
		require.True(t, svc.config.UpdateWritableFromRaw(&next))
		notifications <- struct{}{}
		require.False(t, <-applied)
		stopped := make(chan struct{})
		go func() { firstLoop.Wait(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(100 * time.Millisecond):
			t.Error("the listener's original retry loop remains running after disable")
		}
		return nil
	})
	assert.ErrorContains(t, svc.Run(), "failed to parse Service.RequestTimeout")
}

type lifecycleStore struct {
	configUpdateStore
	disconnects atomic.Int32
}

func (s *lifecycleStore) Disconnect() error {
	s.disconnects.Add(1)
	return nil
}

func TestStoreInitializationCancellation(t *testing.T) {
	for _, outcome := range []string{"retrying", "late success"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc, _ := newConfigUpdateService(t, false)
				store := &lifecycleStore{}
				entered, release := make(chan struct{}), make(chan struct{})
				calls := 0
				svc.config.Database.Type = "lifecycle-store"
				require.NoError(t, svc.RegisterCustomStoreFactory("lifecycle-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) {
					calls++
					if calls == 1 {
						close(entered)
					}
					if outcome == "retrying" {
						return nil, errors.New("database unavailable")
					}
					<-release
					return store, nil
				}))
				secrets := &bootstrapmocks.SecretProvider{}
				secrets.On("GetSecret", "lifecycle-store").Return(map[string]string{"username": "test", "password": "test"}, nil)
				svc.dic.Update(di.ServiceConstructorMap{
					container.StoreClientName:             func(di.Get) any { return nil },
					bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
				})
				require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", true))
				result := make(chan error, 1)
				go func() { result <- svc.applyCurrentStoreForwardSettings() }()
				<-entered
				synctest.Wait()
				svc.ctx.appCancelCtx()
				close(release)
				require.ErrorIs(t, <-result, context.Canceled)
				require.Equal(t, 1, calls, "cancellation must interrupt the initialization retry wait")
				require.Nil(t, container.StoreClientFrom(svc.dic.Get), "canceled initialization must not publish a client")
				require.Zero(t, store.starts.Load(), "canceled initialization must not start a loop")
				if outcome == "late success" {
					require.EqualValues(t, 1, store.disconnects.Load())
				}
			})
		})
	}
}

func TestReloadReconcilesStartupStateEvenWhenSnapshotIsUnchanged(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disable", true: "enable"}[enabled], func(t *testing.T) {
			svc, _ := newConfigUpdateService(t, enabled)
			previous := svc.config.GetWritableInfo()
			require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", !enabled))
			// Run applies a state the listener has not observed yet.
			require.NoError(t, svc.applyCurrentStoreForwardSettings())
			require.True(t, svc.config.UpdateWritableFromRaw(&previous))
			current, revision := svc.config.GetWritableInfoWithRevision()
			NewConfigUpdateProcessor(svc).processConfigChanges(previous, current, revision)
			require.Equal(t, enabled, svc.ctx.storeForwardCancelCtx != nil,
				"listener must reconcile the actual loop when coalesced notifications match its baseline")
		})
	}
}

func TestStartupFailureStopsRetryLoop(t *testing.T) {
	for _, phase := range []string{"trigger", "hook"} {
		t.Run(phase, func(t *testing.T) {
			svc, _ := newConfigUpdateService(t, true)
			originalLoop := svc.ctx.storeForwardWg
			trigger := &lifecycleTrigger{}
			cancel := configureLifecycleRun(t, svc, trigger)
			defer cancel()
			if phase == "trigger" {
				trigger.err = errors.New("trigger failure")
			} else {
				svc.startupHooks = append(svc.startupHooks, func() error { return errors.New("hook failure") })
			}
			require.Error(t, svc.Run())
			require.ErrorIs(t, svc.ctx.appCtx.Err(), context.Canceled)
			require.Nil(t, svc.ctx.storeForwardCancelCtx)
			originalLoop.Wait()
		})
	}
}

func TestStartupAndReloadShareStoreInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, store := newConfigUpdateService(t, false)
		svc.storeForwardReady = false
		entered, release := make(chan struct{}), make(chan struct{})
		calls := 0
		svc.config.Database.Type = "lifecycle-store"
		require.NoError(t, svc.RegisterCustomStoreFactory("lifecycle-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) {
			calls++
			if calls == 1 {
				close(entered)
			}
			<-release
			return store, nil
		}))
		secrets := &bootstrapmocks.SecretProvider{}
		secrets.On("GetSecret", "lifecycle-store").Return(map[string]string{"username": "test", "password": "test"}, nil)
		svc.dic.Update(di.ServiceConstructorMap{
			container.StoreClientName:             func(di.Get) any { return nil },
			bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
		})
		require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", true))
		require.NoError(t, svc.applyCurrentStoreForwardSettings())
		require.Zero(t, calls, "a reload before Run must leave time for pipeline and retention registration")
		prepared := make(chan error, 1)
		go func() { prepared <- svc.prepareStoreForwardStore() }()
		<-entered
		updated := make(chan error, 1)
		go func() { updated <- svc.applyCurrentStoreForwardSettings() }()
		close(release)
		require.NoError(t, <-prepared)
		require.NoError(t, <-updated)
		require.NoError(t, svc.applyCurrentStoreForwardSettings()) // Run reaches its start step.
		require.Equal(t, 1, calls, "startup and reload must reuse the same client")
		require.EqualValues(t, 1, store.starts.Load(), "startup and reload must share one retry loop")
		require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", false))
		require.NoError(t, svc.applyCurrentStoreForwardSettings())
		require.Same(t, store, container.StoreClientFrom(svc.dic.Get), "disable must retain the client for a later enable")
	})
}
