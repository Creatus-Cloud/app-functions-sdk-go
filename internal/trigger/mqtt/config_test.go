package mqtt

import (
	"strings"
	"testing"
	"time"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/stretchr/testify/require"
)

func TestParseConfigPreservesSupportedTransports(t *testing.T) {
	for _, address := range []string{
		"mqtt://localhost:1883", "tcp://localhost:1883", "tls://localhost:8883",
		"ssl://localhost:8883", "mqtts://localhost:8883", "mqtt+ssl://localhost:8883",
		"tcps://localhost:8883", "ws://localhost/mqtt", "wss://localhost/mqtt",
		"unix:///tmp/mqtt.sock", "unix://mqtt.sock",
		"tcp://[::1]:1883", "tls://[::1]:8883", "tcp://localhost:65535",
		"ws://[::1]/mqtt", "wss://localhost:443/mqtt",
	} {
		t.Run(address, func(t *testing.T) {
			config := common.TriggerInfo{SubscribeTopics: "one/#, two/+", ExternalMqtt: common.ExternalMqttConfig{Url: address}}
			parsed, timeout, err := parseConfig(config)
			require.NoError(t, err)
			require.Equal(t, address, parsed.String())
			require.Equal(t, pahoMqtt.NewClientOptions().ConnectTimeout, timeout)
			require.NoError(t, ValidateConfig(config))
			config.ExternalMqtt.ConnectTimeout = "0s"
			_, timeout, err = parseConfig(config)
			require.NoError(t, err)
			require.Zero(t, timeout)
			config.ExternalMqtt.ConnectTimeout = "5s"
			_, timeout, err = parseConfig(config)
			require.NoError(t, err)
			require.Equal(t, 5*time.Second, timeout)
		})
	}
}

func TestValidateConfigPreservesSupportedAuthenticationAndQoS(t *testing.T) {
	for _, mode := range []string{"", "none", "usernamepassword", "clientcert", "cacert"} {
		for qos := byte(0); qos <= 2; qos++ {
			cfg := common.TriggerInfo{SubscribeTopics: "requests/#", ExternalMqtt: common.ExternalMqttConfig{
				Url: "tcp://localhost:1883", AuthMode: mode, QoS: qos,
			}}
			require.NoError(t, ValidateConfig(cfg), "auth=%s qos=%d", mode, qos)
		}
	}
}

func TestValidateTopicFilters(t *testing.T) {
	for _, topic := range []string{"#", "+", "/", "one//two", "one/+/three/#", "$share/group/requests/#", "$queue/requests/+", "温度/+", strings.Repeat("a", 65535)} {
		cfg := common.TriggerInfo{SubscribeTopics: topic, ExternalMqtt: common.ExternalMqttConfig{Url: "tcp://localhost:1883"}}
		require.NoError(t, ValidateConfig(cfg))
	}
	for _, topic := range []string{"one/#/two", "one/partial#", "one/part+ial", "one/\x00", "one/\xff", strings.Repeat("a", 65536)} {
		cfg := common.TriggerInfo{SubscribeTopics: topic, ExternalMqtt: common.ExternalMqttConfig{Url: "tcp://localhost:1883"}}
		require.ErrorContains(t, ValidateConfig(cfg), "Trigger.SubscribeTopics")
	}
}
