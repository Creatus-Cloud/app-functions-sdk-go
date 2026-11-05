package app

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/config"
	bootstrapcontainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	bootstrapmocks "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces/mocks"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopCancelsInitializationWhileStartupWaits(t *testing.T) {
	for _, phase := range []string{"initial store", "configuration reload"} {
		t.Run(phase, func(t *testing.T) {
			svc, _ := newConfigUpdateService(t, false)
			svc.storeForwardReady = false
			trigger := &lifecycleTrigger{}
			cancel := configureLifecycleRun(t, svc, trigger)
			defer cancel()
			entered := make(chan struct{})
			triggerFinished := make(chan struct{})
			svc.config.Database.Type = "stop-test-store"
			require.NoError(t, svc.RegisterCustomStoreFactory("stop-test-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) {
				close(entered)
				// Emulate a context-aware connection attempt, as Postgres receives appCtx.
				<-svc.ctx.appCtx.Done()
				return nil, svc.ctx.appCtx.Err()
			}))
			secrets := &bootstrapmocks.SecretProvider{}
			secrets.On("GetSecret", "stop-test-store").Return(map[string]string{"username": "test", "password": "test"}, nil)
			svc.dic.Update(di.ServiceConstructorMap{
				container.StoreClientName:             func(di.Get) any { return nil },
				bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
			})
			if phase == "initial store" {
				require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", true))
				triggerFinished = entered
			} else {
				notifications := make(bootstrapconfig.UpdatedStream)
				NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
				trigger.initialize = func() {
					next := svc.config.GetWritableInfo()
					next.StoreAndForward.Enabled = true
					svc.config.UpdateWritableFromRaw(&next)
					notifications <- struct{}{}
					<-entered
					close(triggerFinished)
				}
			}
			finished := make(chan error, 1)
			go func() { finished <- svc.Run() }()
			<-triggerFinished
			svc.Stop()
			select {
			case <-svc.ctx.appCtx.Done():
			case <-time.After(250 * time.Millisecond):
				t.Error("Stop did not cancel the connection attempt; Run is blocked waiting for the initialization lock")
			}
			cancel() // Release the reproduction so the test itself cannot leak.
			select {
			case err := <-finished:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not exit after direct application cancellation")
			}
		})
	}
}

func TestConcurrentStopAndRunCleanup(t *testing.T) {
	svc, _ := newConfigUpdateService(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	trigger := &lifecycleTrigger{
		initialize: func() { close(entered); <-release },
		err:        errors.New("trigger initialization failed"),
	}
	cancel := configureLifecycleRun(t, svc, trigger)
	defer cancel()
	cleanupStarted, releaseCleanup := make(chan struct{}), make(chan struct{})
	svc.addDeferred(func() { close(cleanupStarted); <-releaseCleanup })
	runFinished := make(chan error, 1)
	go func() { runFinished <- svc.Run() }()
	<-entered // stop callback has been installed by Run.
	firstStop := make(chan struct{})
	stopFinished := make(chan struct{})
	done := make(chan struct{})
	var first sync.Once
	var calls atomic.Int32
	go func() {
		defer close(stopFinished)
		assert.NotPanics(t, func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				svc.Stop()
				calls.Add(1)
				first.Do(func() { close(firstStop) })
				runtime.Gosched()
			}
		})
	}()
	<-firstStop
	close(release)
	<-cleanupStarted
	before := calls.Load()
	assert.Eventually(t, func() bool { return calls.Load() >= before+2 }, 2*time.Second, time.Millisecond,
		"Stop must remain callable while Run is cleaning up")
	close(done)
	<-stopFinished
	close(releaseCleanup)
	<-runFinished
}
