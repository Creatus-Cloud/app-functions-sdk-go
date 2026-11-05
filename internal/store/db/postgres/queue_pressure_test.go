package postgres

import (
	"fmt"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/stretchr/testify/require"
)

func TestQueuePressureThresholds(t *testing.T) {
	const size = 100
	const critical = 500 << 20
	const headroom = size*4 + (1 << 20)
	for _, tc := range []struct {
		name                                           string
		available                                      uint64
		readingPressure, notificationPressure, blocked bool
	}{
		{"growth", 3 << 30, false, false, false},
		{"readings threshold", 2 << 30, true, false, false},
		{"notification headroom", 1792 << 20, true, false, false},
		{"notifications threshold", 1536 << 20, true, true, false},
		{"recycle below old cutoff", 700 << 20, true, true, false},
		{"last safe write", critical + headroom, true, true, false},
		{"write reaches emergency reserve", critical + headroom - 1, false, false, true},
		{"at emergency reserve", critical, false, false, true},
		{"below emergency reserve", critical - 1, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Client{freeSpace: func(path string) (uint64, error) { require.Equal(t, "/queue-storage", path); return tc.available, nil }}
			for _, pair := range []struct {
				threshold int64
				pressure  bool
			}{{2 << 30, tc.readingPressure}, {1536 << 20, tc.notificationPressure}} {
				limits, err := (interfaces.QueueLimits{StoragePath: "/queue-storage", MinFreeBytes: pair.threshold}).Normalize()
				require.NoError(t, err)
				pressure, err := c.queueStoragePressure(limits, size)
				if tc.blocked {
					require.ErrorContains(t, err, "emergency floor")
				} else {
					require.NoError(t, err)
					require.Equal(t, pair.pressure, pressure)
				}
			}
		})
	}
}

func TestQueuePressureRejectsFailedProbeAndOverflow(t *testing.T) {
	limits, err := (interfaces.QueueLimits{StoragePath: "/queue-storage"}).Normalize()
	require.NoError(t, err)
	c := Client{freeSpace: func(string) (uint64, error) { return 0, fmt.Errorf("unavailable") }}
	_, err = c.queueStoragePressure(limits, 100)
	require.ErrorContains(t, err, "unavailable")
	c.freeSpace = func(string) (uint64, error) { return ^uint64(0), nil }
	_, err = c.queueStoragePressure(limits, ^uint64(0))
	require.ErrorContains(t, err, "overflows")
}
