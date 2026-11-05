package interfaces

import "testing"

func TestQueueLimits(t *testing.T) {
	defaults, err := (QueueLimits{}).Normalize()
	if err != nil || defaults.MaxBatchItems != 100 || defaults.MaxItems != DefaultQueueItems || defaults.MaxBytes != 64<<20 || defaults.RetentionPolicy != KeepNewest || defaults.MinFreeBytes != 1536<<20 || defaults.CriticalFreeBytes != 500<<20 {
		t.Fatal(defaults, err)
	}
	for _, bad := range []QueueLimits{{MaxItems: -1}, {MaxBytes: -1}, {RetentionPolicy: "unknown"}, {StoragePath: "relative"}, {MinFreeBytes: -1}, {CriticalFreeBytes: -1}, {MinFreeBytes: 500 << 20}, {MinFreeBytes: 1 << 30, CriticalFreeBytes: 1 << 30}, {MinFreeBytes: 1 << 30, CriticalFreeBytes: 2 << 30}} {
		if _, err := bad.Normalize(); err == nil {
			t.Fatalf("accepted %#v", bad)
		}
	}
	for _, policy := range []string{KeepNewest, KeepOldest} {
		l := QueueLimits{MaxItems: 1, MaxBytes: 1, RetentionPolicy: policy}
		got, err := l.Normalize()
		if err != nil || got.MaxItems != l.MaxItems || got.MaxBytes != l.MaxBytes || got.RetentionPolicy != l.RetentionPolicy {
			t.Fatal(got, err)
		}
	}
}

func TestQueueBatchLimit(t *testing.T) {
	for _, n := range []int{-1, 1001} {
		if _, err := (QueueLimits{MaxBatchItems: n}).Normalize(); err == nil {
			t.Fatalf("accepted invalid MaxBatchItems=%d", n)
		}
	}
	for _, n := range []int{1, 100, 1000} {
		l, err := (QueueLimits{MaxBatchItems: n}).Normalize()
		if err != nil || l.MaxBatchItems != n {
			t.Fatalf("batch size %d: %#v, %v", n, l, err)
		}
	}
}

func TestQueueByteLimitModes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limits  QueueLimits
		want    int64
		invalid bool
	}{
		{name: "default without path", want: 64 << 20},
		{name: "default with path", limits: QueueLimits{StoragePath: "/queue-storage"}, want: 64 << 20},
		{name: "disabled with path", limits: QueueLimits{MaxBytes: -1, StoragePath: "/queue-storage"}, want: -1},
		{name: "explicit without path", limits: QueueLimits{MaxBytes: 1 << 30}, want: 1 << 30},
		{name: "explicit with path", limits: QueueLimits{MaxBytes: 1 << 30, StoragePath: "/queue-storage"}, want: 1 << 30},
		{name: "disabled without path", limits: QueueLimits{MaxBytes: -1}, invalid: true},
		{name: "disabled with relative path", limits: QueueLimits{MaxBytes: -1, StoragePath: "relative"}, invalid: true},
		{name: "invalid negative", limits: QueueLimits{MaxBytes: -2, StoragePath: "/queue-storage"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.limits.Normalize()
			if tc.invalid {
				if err == nil {
					t.Fatalf("accepted invalid limits: %#v", tc.limits)
				}
				return
			}
			if err != nil || got.MaxBytes != tc.want {
				t.Fatalf("got %#v, %v; want MaxBytes=%d", got, err, tc.want)
			}
			// Runtime and database layers both normalize. A disabled cap must stay disabled.
			again, err := got.Normalize()
			if err != nil || again != got {
				t.Fatalf("second normalization changed limits: %#v, %v", again, err)
			}
		})
	}
}
