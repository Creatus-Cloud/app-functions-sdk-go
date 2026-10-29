package app

import (
	"context"
	"strconv"
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

func triggerCandidate(config common.TriggerInfo) configvalidation.Request {
	return configvalidation.Request{ServiceID: "test-service", Configuration: map[string]any{
		"Service/Host": "localhost", "Service/Port": "59701",
		"Service/RequestTimeout": "5s", "Writable/LogLevel": "INFO",
		"Trigger/Type":                        config.Type,
		"Trigger/SubscribeTopics":             config.SubscribeTopics,
		"Trigger/ExternalMqtt/Url":            config.ExternalMqtt.Url,
		"Trigger/ExternalMqtt/ConnectTimeout": config.ExternalMqtt.ConnectTimeout,
		"Trigger/ExternalMqtt/QoS":            strconv.Itoa(int(config.ExternalMqtt.QoS)),
		"Trigger/ExternalMqtt/AuthMode":       config.ExternalMqtt.AuthMode,
	}}
}

func TestPreflightRejectsTriggerSettingsRejectedByStartup(t *testing.T) {
	for _, test := range []struct{ name, trigger, topics, url, timeout, field string }{
		{"unknown trigger", "typo", "requests/#", "tcp://localhost:1883", "5s", "Trigger.Type"},
		{"missing trigger", "", "requests/#", "tcp://localhost:1883", "5s", "Trigger.Type"},
		{"missing topics", "external-mqtt", "", "tcp://localhost:1883", "5s", "Trigger.SubscribeTopics"},
		{"empty topic list", "external-mqtt", " , , ", "tcp://localhost:1883", "5s", "Trigger.SubscribeTopics"},
		{"invalid URL", "external-mqtt", "requests/#", "://invalid", "5s", "Trigger.ExternalMqtt.Url"},
		{"empty URL", "external-mqtt", "requests/#", "", "5s", "Trigger.ExternalMqtt.Url"},
		{"unsupported scheme", "external-mqtt", "requests/#", "ftp://localhost:1883", "5s", "Trigger.ExternalMqtt.Url"},
		{"missing host", "external-mqtt", "requests/#", "tcp://:1883", "5s", "Trigger.ExternalMqtt.Url"},
		{"negative timeout", "external-mqtt", "requests/#", "tcp://localhost:1883", "-1s", "Trigger.ExternalMqtt.ConnectTimeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sdk, _ := createSdkAndRouter()
			sdk.serviceKey = "test-service"
			sdk.lc = logger.NewMockClient()
			require.NoError(t, sdk.initializeConfigValidation())
			cfg := &common.ConfigurationStruct{}
			cfg.Trigger.Type = test.trigger
			cfg.Trigger.SubscribeTopics = test.topics
			cfg.Trigger.ExternalMqtt.Url = test.url
			cfg.Trigger.ExternalMqtt.ConnectTimeout = test.timeout
			result := sdk.configValidation.Validate(triggerCandidate(cfg.Trigger))
			require.False(t, result.Valid, "%+v", result)
			require.NotEmpty(t, result.Errors)
			require.Contains(t, result.Errors[0].Message, test.field)
			if test.field == "Trigger.Type" {
				sdk.config = cfg
				require.Nil(t, sdk.setupTrigger(cfg))
			} else {
				binding := &triggerMocks.ServiceBinding{}
				binding.On("Config").Return(cfg)
				binding.On("LoggingClient").Return(logger.NewMockClient())
				trigger := mqtt.NewTrigger(binding, &triggerMocks.MessageProcessor{})
				_, err := trigger.Initialize(&sync.WaitGroup{}, context.Background(), nil)
				require.EqualError(t, err, result.Errors[0].Message)
			}
		})
	}
}

func TestCustomTriggerRegistrationDuringPreflight(t *testing.T) {
	sdk, _ := createSdkAndRouter()
	sdk.serviceKey = "test-service"
	require.NoError(t, sdk.initializeConfigValidation())
	factory := func(interfaces.TriggerConfig) (interfaces.Trigger, error) {
		t.Error("preflight executed a custom trigger factory")
		return nil, nil
	}
	require.NoError(t, sdk.RegisterCustomTriggerFactory("custom", factory))
	request := triggerCandidate(common.TriggerInfo{Type: "CUSTOM"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			if err := sdk.RegisterCustomTriggerFactory("custom", factory); err != nil {
				t.Errorf("registration failed: %v", err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			if result := sdk.configValidation.Validate(request); !result.Valid {
				t.Errorf("registered custom trigger rejected: %+v", result.Errors)
			}
		}
	}()
	wg.Wait()
	require.Error(t, sdk.RegisterCustomTriggerFactory("missing-factory", nil))
}

func TestPreflightRejectsInvalidMQTTSettingsBeforeStartupIO(t *testing.T) {
	for _, test := range []struct {
		name, address, topics, auth, field string
		qos                                byte
	}{
		{"missing TCP port", "tcp://localhost", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"empty TCP port", "tcp://localhost:", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"missing TLS port", "tls://localhost", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"out of range TCP port", "tcp://localhost:65536", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"zero TCP port", "tcp://localhost:0", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"out of range websocket port", "wss://localhost:65536/mqtt", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"unbracketed IPv6", "tcp://::1:1883", "requests/#", "none", "Trigger.ExternalMqtt.Url", 0},
		{"invalid QoS", "tcp://localhost:1883", "requests/#", "none", "Trigger.ExternalMqtt.QoS", 3},
		{"overflow QoS", "tcp://localhost:1883", "requests/#", "none", "Trigger.ExternalMqtt.QoS", 255},
		{"invalid multilevel filter", "tcp://localhost:1883", "requests/#/after", "none", "Trigger.SubscribeTopics", 0},
		{"inline multilevel filter", "tcp://localhost:1883", "requests/prefix#", "none", "Trigger.SubscribeTopics", 0},
		{"inline single level filter", "tcp://localhost:1883", "requests/pre+fix", "none", "Trigger.SubscribeTopics", 0},
		{"unknown auth mode", "tcp://localhost:1883", "requests/#", "typo", "Trigger.ExternalMqtt.AuthMode", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			sdk, _ := createSdkAndRouter()
			sdk.serviceKey = "test-service"
			require.NoError(t, sdk.initializeConfigValidation())
			cfg := &common.ConfigurationStruct{Trigger: common.TriggerInfo{
				Type: "external-mqtt", SubscribeTopics: test.topics,
				ExternalMqtt: common.ExternalMqttConfig{Url: test.address, QoS: test.qos, AuthMode: test.auth},
			}}
			result := sdk.configValidation.Validate(triggerCandidate(cfg.Trigger))
			require.False(t, result.Valid)
			require.NotEmpty(t, result.Errors)
			require.Contains(t, result.Errors[0].Message, test.field)
			binding := &triggerMocks.ServiceBinding{}
			binding.On("Config").Return(cfg)
			binding.On("LoggingClient").Return(logger.NewMockClient())
			// There is no SecretProvider expectation: static rejection must precede IO.
			trigger := mqtt.NewTrigger(binding, &triggerMocks.MessageProcessor{})
			_, err := trigger.Initialize(&sync.WaitGroup{}, context.Background(), nil)
			require.EqualError(t, err, result.Errors[0].Message)
		})
	}
}
