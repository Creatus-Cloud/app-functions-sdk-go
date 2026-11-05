package app

import (
	"errors"
	"testing"
	"testing/synctest"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/appfunction"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/config"
	bootstrapcontainer "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	bootstrapinterfaces "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	bootstrapmocks "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces/mocks"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reloadInitializationSecrets struct {
	bootstrapinterfaces.SecretProvider
	entered chan struct{}
	release chan struct{}
	calls   int
}

type initializationStore struct {
	configUpdateStore
	items []interfaces.StoredObject
}

func (s *initializationStore) Store(item interfaces.StoredObject) (string, error) {
	s.items = append(s.items, item)
	return "stored", nil
}

func TestExportDuringStoreInitializationRecoversWhenReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, _ := newConfigUpdateService(t, false)
		svc.usingConfigurablePipeline = false
		svc.runtime.SetDefaultFunctionsPipeline([]interfaces.AppFunction{
			func(ctx interfaces.AppFunctionContext, data any) (bool, any) {
				ctx.SetRetryData([]byte("offline-reading"))
				return false, errors.New("MQTT offline")
			},
		})
		store := &initializationStore{}
		entered, release := make(chan struct{}), make(chan struct{})
		svc.config.Database.Type = "reload-store"
		require.NoError(t, svc.RegisterCustomStoreFactory("reload-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) {
			close(entered)
			<-release
			return store, nil
		}))
		secrets := &bootstrapmocks.SecretProvider{}
		secrets.On("GetSecret", "reload-store").Return(map[string]string{"username": "test", "password": "test"}, nil)
		svc.dic.Update(di.ServiceConstructorMap{
			container.StoreClientName:             func(di.Get) any { return nil },
			bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
		})
		notifications := make(bootstrapconfig.UpdatedStream)
		NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
		next := svc.config.GetWritableInfo()
		next.StoreAndForward.Enabled = true
		require.True(t, svc.config.UpdateWritableFromRaw(&next))
		notifications <- struct{}{}
		<-entered
		process := func() {
			ctx := appfunction.NewContext("initialization-test", svc.dic, "application/json")
			err := svc.runtime.ProcessMessage(ctx, []byte("reading"), svc.runtime.GetDefaultPipeline())
			assert.ErrorContains(t, err, "MQTT offline")
		}
		assert.NotPanics(t, process, "failed export must tolerate a store that is still initializing")
		assert.Empty(t, store.items, "no store is available to persist the first message")
		close(release)
		synctest.Wait()
		require.Same(t, store, container.StoreClientFrom(svc.dic.Get))
		process()
		require.Len(t, store.items, 1, "failed exports must be stored after initialization succeeds")
		require.Equal(t, []byte("offline-reading"), store.items[0].Payload)
		close(notifications)
	})
}

func (s *reloadInitializationSecrets) GetSecret(string, ...string) (map[string]string, error) {
	s.calls++
	if s.calls == 1 {
		close(s.entered)
		<-s.release
		return nil, errors.New("old credentials failed")
	}
	return map[string]string{"username": "test", "password": "test"}, nil
}

func TestInitializationFailureMustNotOverwriteNewerEnable(t *testing.T) {
	for _, change := range []string{"corrected credentials", "identical re-enable", "disable"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc, store := newConfigUpdateService(t, false)
				svc.config.Database.Type = "reload-store"
				require.NoError(t, svc.RegisterCustomStoreFactory("reload-store", func(config.Database, config.Credentials) (interfaces.StoreClient, error) {
					return store, nil
				}))
				secrets := &reloadInitializationSecrets{entered: make(chan struct{}), release: make(chan struct{})}
				svc.dic.Update(di.ServiceConstructorMap{
					container.StoreClientName:             func(di.Get) any { return nil },
					bootstrapcontainer.SecretProviderName: func(di.Get) any { return secrets },
				})
				notifications := make(bootstrapconfig.UpdatedStream)
				NewConfigUpdateProcessor(svc).WaitForConfigUpdates(notifications)
				first := svc.config.GetWritableInfo()
				first.StoreAndForward.Enabled = true
				require.True(t, svc.config.UpdateWritableFromRaw(&first))
				notifications <- struct{}{}
				<-secrets.entered
				second := svc.config.GetWritableInfo()
				switch change {
				case "corrected credentials":
					second.InsecureSecrets = config.InsecureSecrets{"db": {SecretName: "reload-store", SecretData: map[string]string{"password": "corrected"}}}
				case "disable":
					second.StoreAndForward.Enabled = false
				}
				require.True(t, svc.config.UpdateWritableFromRaw(&second))
				close(secrets.release)
				notifications <- struct{}{}
				synctest.Wait()
				require.Equal(t, second.StoreAndForward.Enabled, svc.config.GetStoreAndForwardInfo().Enabled,
					"older initialization failure must not overwrite the newer configuration")
				if second.StoreAndForward.Enabled {
					require.EqualValues(t, 1, store.starts.Load(), "newer enable must start a retry loop")
					require.Equal(t, 2, secrets.calls)
				} else {
					require.Zero(t, store.starts.Load())
					require.Equal(t, 1, secrets.calls)
				}
				close(notifications)
			})
		})
	}
}
