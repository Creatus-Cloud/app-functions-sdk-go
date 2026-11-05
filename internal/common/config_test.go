package common

import (
	"sync"
	"testing"

	bootstrapConfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/stretchr/testify/require"
)

func TestRawConfigurationUpdatesSynchronizeWithReaders(t *testing.T) {
	for _, full := range []bool{true, false} {
		name := "writable"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			initial := WritableInfo{StoreAndForward: StoreAndForwardInfo{MaxBatchItems: 100, MinFreeBytes: 1000}}
			updated := WritableInfo{StoreAndForward: StoreAndForwardInfo{MaxBatchItems: 7, MinFreeBytes: 7000}}
			config := &ConfigurationStruct{Writable: initial}
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 1000; i++ {
					next := initial
					if i%2 == 0 {
						next = updated
					}
					if full {
						config.UpdateFromRaw(&ConfigurationStruct{Writable: next})
					} else {
						config.UpdateWritableFromRaw(&next)
					}
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 1000; i++ {
					got := config.GetWritableInfo().StoreAndForward
					if got != initial.StoreAndForward && got != updated.StoreAndForward {
						t.Errorf("reader received a partially updated queue configuration: %+v", got)
					}
					_ = config.CopyWritableInfo()
				}
			}()
			close(start)
			wg.Wait()
		})
	}
}

func snapshotTestWritable() WritableInfo {
	return WritableInfo{
		LogLevel: "INFO",
		Pipeline: PipelineInfo{
			PerTopicPipelines: map[string]TopicPipeline{"main": {Topics: "events/#"}},
			Functions:         map[string]PipelineFunction{"Export": {Parameters: map[string]string{"Topic": "events"}}},
		},
		InsecureSecrets: bootstrapConfig.InsecureSecrets{"test": {SecretName: "test", SecretData: map[string]string{"value": "test-fixture"}}},
		Telemetry:       bootstrapConfig.TelemetryInfo{Interval: "30s", Metrics: map[string]bool{"Queue": true}, Tags: map[string]string{"site": "original"}},
	}
}

func TestWritableSnapshotsDoNotExposeMutableMaps(t *testing.T) {
	source := snapshotTestWritable()
	cfg := &ConfigurationStruct{}
	require.True(t, cfg.UpdateWritableFromRaw(&source))
	source.Pipeline.Functions["Export"].Parameters["Topic"] = "source-changed"
	source.Telemetry.Tags["site"] = "source-changed"
	source.InsecureSecrets["test"].SecretData["value"] = "source-changed"
	require.Equal(t, snapshotTestWritable(), cfg.GetWritableInfo())

	writable := cfg.GetWritablePtr().(*WritableInfo)
	writable.Pipeline.Functions["Export"].Parameters["Topic"] = "reader-changed"
	writable.Pipeline.PerTopicPipelines["main"] = TopicPipeline{}
	cfg.GetTelemetryInfo().Metrics["Queue"] = false
	cfg.GetTelemetryInfo().Tags["site"] = "reader-changed"
	cfg.GetInsecureSecrets()["test"].SecretData["value"] = "reader-changed"
	require.Equal(t, snapshotTestWritable(), cfg.GetWritableInfo())

	parameters := map[string]PipelineFunction{"Export": {Parameters: map[string]string{"Topic": "new"}}}
	require.NoError(t, cfg.SetWritableInfo("Pipeline.Functions", parameters))
	parameters["Export"].Parameters["Topic"] = "setter-source-changed"
	require.Equal(t, "new", cfg.GetWritableInfo().Pipeline.Functions["Export"].Parameters["Topic"])
}

func TestConfigurationReadersUseIndependentSnapshotsDuringUpdates(t *testing.T) {
	cfg := &ConfigurationStruct{Writable: snapshotTestWritable()}
	retainedTelemetry := cfg.GetTelemetryInfo()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			next := snapshotTestWritable()
			next.Telemetry.Metrics["Queue"] = i%2 == 0
			cfg.UpdateWritableFromRaw(&next)
			cfg.SetPipelineFunctionParameterNamesLowercase()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			_ = cfg.GetLogLevel()
			_ = cfg.GetRegistryInfo()
			_ = cfg.GetStoreAndForwardInfo()
			_ = cfg.GetInsecureSecrets()["test"].SecretData["value"]
			_, _ = cfg.GetTelemetryInfo().GetEnabledMetricName("Queue")
			_, _ = retainedTelemetry.GetEnabledMetricName("Queue")
			_ = cfg.GetWritableInfo().Pipeline.Functions["Export"].Parameters["topic"]
			_ = cfg.GetWritablePtr().(*WritableInfo).Telemetry.Tags["site"]
			if _, err := cfg.GetConfigurationSnapshot(); err != nil {
				t.Error(err)
			}
		}
	}()
	close(start)
	wg.Wait()
	require.True(t, retainedTelemetry.Metrics["Queue"], "a previous snapshot was changed by a live update")
}

func TestRawConfigurationUpdatesRejectNilAndWrongType(t *testing.T) {
	config := &ConfigurationStruct{Writable: WritableInfo{StoreAndForward: StoreAndForwardInfo{MaxBatchItems: 7}}}
	require.False(t, config.UpdateFromRaw((*ConfigurationStruct)(nil)))
	require.False(t, config.UpdateFromRaw(&WritableInfo{}))
	require.False(t, config.UpdateWritableFromRaw((*WritableInfo)(nil)))
	require.False(t, config.UpdateWritableFromRaw(&ConfigurationStruct{}))
	require.Equal(t, 7, config.GetWritableInfo().StoreAndForward.MaxBatchItems)
}

func TestFailedEnableRollbackRequiresCurrentRevision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update func(*testing.T, *ConfigurationStruct)
	}{
		{name: "writable publication", update: func(t *testing.T, cfg *ConfigurationStruct) {
			next := cfg.GetWritableInfo()
			require.True(t, cfg.UpdateWritableFromRaw(&next))
		}},
		{name: "full publication", update: func(t *testing.T, cfg *ConfigurationStruct) {
			next, err := cfg.GetConfigurationSnapshot()
			require.NoError(t, err)
			require.True(t, cfg.UpdateFromRaw(next))
		}},
		{name: "field update", update: func(t *testing.T, cfg *ConfigurationStruct) {
			require.NoError(t, cfg.SetWritableInfo("StoreAndForward.Enabled", true))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ConfigurationStruct{Writable: WritableInfo{StoreAndForward: StoreAndForwardInfo{Enabled: true}}}
			before, revision := cfg.GetWritableInfoWithRevision()
			tc.update(t, cfg)
			after, currentRevision := cfg.GetWritableInfoWithRevision()
			require.Equal(t, before, after, "a repeated enable still represents a new attempt")
			require.Greater(t, currentRevision, revision)
			require.False(t, cfg.DisableStoreAndForwardIfRevision(revision))
			require.True(t, cfg.GetStoreAndForwardInfo().Enabled)
			require.True(t, cfg.DisableStoreAndForwardIfRevision(currentRevision))
			require.False(t, cfg.GetStoreAndForwardInfo().Enabled)
		})
	}
}
