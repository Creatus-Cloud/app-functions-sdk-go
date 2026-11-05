package app

import (
	"fmt"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/bootstrap/container"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/stretchr/testify/require"
)

func TestReloadRejectsLimitsOnExistingUnboundedStore(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			change func(*common.StoreAndForwardInfo)
		}{
			{"bytes", func(s *common.StoreAndForwardInfo) { s.MaxStoredBytes = 1 << 20 }},
			{"batch", func(s *common.StoreAndForwardInfo) { s.MaxBatchItems = 7 }},
			{"policy", func(s *common.StoreAndForwardInfo) { s.RetentionPolicy = interfaces.KeepNewest }},
			{"path", func(s *common.StoreAndForwardInfo) { s.StoragePath = "/queue-storage" }},
			{"free space", func(s *common.StoreAndForwardInfo) { s.MinFreeBytes = 2 << 30 }},
			{"critical space", func(s *common.StoreAndForwardInfo) { s.CriticalFreeBytes = 600 << 20 }},
			{"invalid value", func(s *common.StoreAndForwardInfo) { s.MaxBatchItems = -1 }},
		} {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, tc.name), func(t *testing.T) {
				svc, store := newConfigUpdateService(t, enabled)
				next := svc.config.GetWritableInfo()
				next.StoreAndForward.Enabled = true
				tc.change(&next.StoreAndForward)
				require.True(t, svc.config.UpdateWritableFromRaw(&next))
				current, revision := svc.config.GetWritableInfoWithRevision()
				err := svc.applyStoreForwardSettings(current.StoreAndForward, revision)
				require.Error(t, err)
				if tc.name != "invalid value" {
					require.ErrorContains(t, err, "bounded store")
				}
				require.False(t, svc.config.GetStoreAndForwardInfo().Enabled)
				require.Nil(t, svc.ctx.storeForwardCancelCtx, "rejected settings must stop the old retry loop")
				require.Same(t, store, container.StoreClientFrom(svc.dic.Get))

				// Correcting the settings must reuse the still-open client and
				// start one retry loop without restarting the application.
				next.StoreAndForward = common.StoreAndForwardInfo{Enabled: true, RetryInterval: "1h"}
				require.True(t, svc.config.UpdateWritableFromRaw(&next))
				current, revision = svc.config.GetWritableInfoWithRevision()
				before := store.starts.Load()
				require.NoError(t, svc.applyStoreForwardSettings(current.StoreAndForward, revision))
				require.Equal(t, before+1, store.starts.Load())
				require.True(t, svc.config.GetStoreAndForwardInfo().Enabled)
			})
		}
	}
}

type boundedReloadStore struct {
	configUpdateStore
	interfaces.BoundedStoreClient
}

func (s *boundedReloadStore) EnforceLimits(interfaces.QueueLimits) (int64, int64, error) {
	s.starts.Add(1)
	return 0, 0, nil
}

func TestReloadAcceptsLimitsOnExistingBoundedStoreWithoutRestarting(t *testing.T) {
	svc, _ := newConfigUpdateService(t, false)
	store := &boundedReloadStore{}
	svc.dic.Update(di.ServiceConstructorMap{container.StoreClientName: func(di.Get) any { return store }})
	require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.Enabled", true))
	require.NoError(t, svc.applyCurrentStoreForwardSettings())
	loop := svc.ctx.storeForwardWg
	require.NoError(t, svc.config.SetWritableInfo("StoreAndForward.MaxStoredBytes", int64(1<<20)))
	current, revision := svc.config.GetWritableInfoWithRevision()
	require.NoError(t, svc.applyStoreForwardSettings(current.StoreAndForward, revision))
	require.Same(t, loop, svc.ctx.storeForwardWg)
	require.EqualValues(t, 1, store.starts.Load())
	require.True(t, svc.config.GetStoreAndForwardInfo().Enabled)
}
