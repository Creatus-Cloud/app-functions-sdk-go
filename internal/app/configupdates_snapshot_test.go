package app

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/stretchr/testify/require"
)

// Bootstrap can publish B while the listener processes its captured snapshot A.
// If C is published before the listener takes its next snapshot, both pending
// notifications see C. A's side effects must match the baseline it returns.
func TestCoalescedReloadAppliesCapturedSettings(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wasEnabled bool
		change     func(*common.WritableInfo)
		check      func(*testing.T, *Service, *configUpdateStore, int32)
	}{
		{
			name: "enable",
			change: func(w *common.WritableInfo) {
				w.StoreAndForward.Enabled = true
			},
			check: func(t *testing.T, svc *Service, store *configUpdateStore, before int32) {
				require.Equal(t, before+1, store.starts.Load(), "captured enable must start exactly one loop")
				time.Sleep(time.Hour)
				synctest.Wait()
				require.Equal(t, before+2, store.starts.Load(), "final enabled state must perform retries")
			},
		},
		{
			name: "disable", wasEnabled: true,
			change: func(w *common.WritableInfo) {
				w.StoreAndForward.Enabled = false
			},
			check: func(t *testing.T, svc *Service, store *configUpdateStore, before int32) {
				time.Sleep(time.Hour)
				synctest.Wait()
				require.Equal(t, before, store.starts.Load(), "final disabled state must stop retries")
			},
		},
		{
			name: "retry interval", wasEnabled: true,
			change: func(w *common.WritableInfo) {
				w.StoreAndForward.RetryInterval = "10s"
			},
			check: func(t *testing.T, svc *Service, store *configUpdateStore, before int32) {
				require.Equal(t, before+1, store.starts.Load(), "interval change must restart exactly once")
				time.Sleep(10 * time.Second)
				synctest.Wait()
				require.Equal(t, before+2, store.starts.Load(), "retry timer must use the captured interval")
			},
		},
		{
			name:   "pipeline",
			change: changedConfigUpdatePipeline,
			check: func(t *testing.T, svc *Service, store *configUpdateStore, before int32) {
				require.Len(t, svc.runtime.GetDefaultPipeline().Transforms, 2, "final pipeline must match the captured configuration")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc, store := newConfigUpdateService(t, tc.wasEnabled)
				processor := NewConfigUpdateProcessor(svc)
				before := store.starts.Load()
				previous := svc.config.GetWritableInfo()

				a := svc.config.GetWritableInfo()
				tc.change(&a)
				require.True(t, svc.config.UpdateWritableFromRaw(&a))
				a, revisionA := svc.config.GetWritableInfoWithRevision()
				// B reverses A before A's side effects run.
				require.True(t, svc.config.UpdateWritableFromRaw(&previous))
				applied := processor.processConfigChanges(previous, a, revisionA)
				synctest.Wait() // Let the loop start while live configuration is B.

				// C restores A. B and C's notifications both observe C.
				require.True(t, svc.config.UpdateWritableFromRaw(&a))
				c, revisionC := svc.config.GetWritableInfoWithRevision()
				applied = processor.processConfigChanges(applied, c, revisionC)
				processor.processConfigChanges(applied, c, revisionC)
				synctest.Wait()
				tc.check(t, svc, store, before)
			})
		})
	}
}
