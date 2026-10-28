package app

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	triggerMocks "github.com/edgexfoundry/app-functions-sdk-go/v4/internal/trigger/mocks"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/trigger/mqtt"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/configvalidation"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
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
					"Trigger/Type":                "http",
				},
			})
			require.Equal(t, test.valid, result.Valid, "%+v", result.Errors)
			if !test.valid {
				require.Contains(t, fmt.Sprint(result.Errors), "Writable.Telemetry.Interval")
			}
		})
	}
}

func TestConfigPreflightMQTTConnectTimeout(t *testing.T) {
	sdk, _ := createSdkAndRouter()
	require.NoError(t, sdk.RegisterCustomTriggerFactory("custom", func(interfaces.TriggerConfig) (interfaces.Trigger, error) {
		t.Error("preflight must not execute the custom trigger factory")
		return nil, nil
	}))
	sdk.serviceKey = "test-service"
	require.NoError(t, sdk.initializeConfigValidation())
	for _, test := range []struct {
		name, trigger, timeout string
		valid                  bool
	}{
		{"default timeout", "external-mqtt", "", true},
		{"positive timeout", "external-mqtt", "5s", true},
		{"zero timeout", "external-mqtt", "0s", true},
		{"malformed", "external-mqtt", "invalid", false},
		{"overflow", "external-mqtt", "999999999999999999999h", false},
		{"case insensitive trigger", "EXTERNAL-MQTT", "invalid", false},
		{"unused MQTT settings", "edgex-messagebus", "invalid", true},
		{"custom trigger", "custom", "invalid", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := sdk.configValidation.Validate(configvalidation.Request{
				ServiceID: "test-service",
				Configuration: map[string]string{
					"Service/Host": "localhost", "Service/Port": "59701",
					"Service/RequestTimeout": "5s", "Writable/LogLevel": "INFO",
					"Trigger/Type":                        test.trigger,
					"Trigger/SubscribeTopics":             "requests/#",
					"Trigger/ExternalMqtt/Url":            "tcp://localhost:1883",
					"Trigger/ExternalMqtt/ConnectTimeout": test.timeout,
				},
			})
			require.Equal(t, test.valid, result.Valid, "%+v", result.Errors)
			if !test.valid {
				require.Contains(t, fmt.Sprint(result.Errors), "Trigger.ExternalMqtt.ConnectTimeout")
			}
		})
	}
}

func TestConfigPreflightMQTTTimeoutMatchesStartup(t *testing.T) {
	sdk, _ := createSdkAndRouter()
	sdk.serviceKey = "test-service"
	require.NoError(t, sdk.initializeConfigValidation())
	result := sdk.configValidation.Validate(configvalidation.Request{ServiceID: "test-service", Configuration: map[string]string{
		"Service/Host": "localhost", "Service/Port": "59701", "Service/RequestTimeout": "5s", "Writable/LogLevel": "INFO",
		"Trigger/Type": "external-mqtt", "Trigger/SubscribeTopics": "requests/#", "Trigger/ExternalMqtt/Url": "tcp://localhost:1883", "Trigger/ExternalMqtt/ConnectTimeout": "invalid",
	}})
	cfg := &common.ConfigurationStruct{}
	cfg.Trigger.Type = "external-mqtt"
	cfg.Trigger.SubscribeTopics = "requests/#"
	cfg.Trigger.ExternalMqtt.Url = "tcp://localhost:1883"
	cfg.Trigger.ExternalMqtt.ConnectTimeout = "invalid"
	binding := &triggerMocks.ServiceBinding{}
	binding.On("Config").Return(cfg)
	binding.On("LoggingClient").Return(logger.NewMockClient())
	trigger := mqtt.NewTrigger(binding, &triggerMocks.MessageProcessor{})
	_, startupErr := trigger.Initialize(&sync.WaitGroup{}, context.Background(), nil)
	require.ErrorContains(t, startupErr, "invalid MQTT ConnectTimeout")
	require.False(t, result.Valid, "preflight accepts MQTT timeout rejected by normal startup")
}
