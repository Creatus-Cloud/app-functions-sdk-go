package app

import (
	"fmt"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/configvalidation"
	"github.com/stretchr/testify/require"
)

func TestConfigPreflightTelemetryInterval(t *testing.T) {
	sdk, _ := createSdkAndRouter()
	sdk.serviceKey = "test-service"
	require.NoError(t, sdk.initializeConfigValidation())

	for _, test := range []struct {
		name     string
		interval string
		valid    bool
	}{
		{"disabled by empty interval", "", true},
		{"disabled by zero", "0", true},
		{"disabled by zero duration", "0s", true},
		{"positive", "30s", true},
		{"malformed", "not-a-duration", false},
		{"negative", "-1s", false},
		{"overflow", "999999999999999999999h", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := sdk.configValidation.Validate(configvalidation.Request{
				ServiceID: "test-service",
				Configuration: map[string]string{
					"Service/Host":                "localhost",
					"Service/Port":                "59701",
					"Service/RequestTimeout":      "5s",
					"Writable/LogLevel":           "INFO",
					"Writable/Telemetry/Interval": test.interval,
				},
			})
			require.Equal(t, test.valid, result.Valid, "%+v", result.Errors)
			if !test.valid {
				require.Contains(t, fmt.Sprint(result.Errors), "Writable.Telemetry.Interval")
			}
		})
	}
}
