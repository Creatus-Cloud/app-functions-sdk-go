package mqtt

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/util"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/messaging"
)

// ValidateConfig checks static MQTT settings without connecting or loading secrets.
func ValidateConfig(config common.TriggerInfo) error {
	_, _, err := parseConfig(config)
	return err
}

func parseConfig(config common.TriggerInfo) (*url.URL, time.Duration, error) {
	topics := util.DeleteEmptyAndTrim(strings.FieldsFunc(config.SubscribeTopics, util.SplitComma))
	if len(topics) == 0 {
		return nil, 0, fmt.Errorf("Trigger.SubscribeTopics: missing SubscribeTopics for MQTT Trigger")
	}
	for _, topic := range topics {
		if err := validateTopicFilter(topic); err != nil {
			return nil, 0, fmt.Errorf("Trigger.SubscribeTopics: %w", err)
		}
	}
	if config.ExternalMqtt.QoS > 2 {
		return nil, 0, fmt.Errorf("Trigger.ExternalMqtt.QoS must be 0, 1, or 2")
	}
	switch config.ExternalMqtt.AuthMode {
	case "", messaging.AuthModeNone, messaging.AuthModeUsernamePassword, messaging.AuthModeCert, messaging.AuthModeCA:
	default:
		return nil, 0, fmt.Errorf("Trigger.ExternalMqtt.AuthMode is unsupported")
	}
	brokerURL, err := url.Parse(config.ExternalMqtt.Url)
	if err != nil {
		return nil, 0, fmt.Errorf("Trigger.ExternalMqtt.Url: invalid MQTT Broker Url")
	}
	if err := validateBrokerURL(brokerURL); err != nil {
		return nil, 0, fmt.Errorf("Trigger.ExternalMqtt.Url: %w", err)
	}
	timeout := pahoMqtt.NewClientOptions().ConnectTimeout
	if config.ExternalMqtt.ConnectTimeout != "" {
		timeout, err = time.ParseDuration(config.ExternalMqtt.ConnectTimeout)
		if err != nil || timeout < 0 {
			return nil, 0, fmt.Errorf("Trigger.ExternalMqtt.ConnectTimeout: invalid MQTT ConnectTimeout")
		}
	}
	return brokerURL, timeout, nil
}

func validateBrokerURL(brokerURL *url.URL) error {
	switch brokerURL.Scheme {
	case "mqtt", "tcp", "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
		// Paho uses this address directly; these transports have no default port.
		if _, port, err := net.SplitHostPort(brokerURL.Host); err != nil || port == "" {
			return fmt.Errorf("must include a host and port")
		}
	case "ws", "wss":
		// WebSocket transports supply the HTTP/HTTPS default port when omitted.
	case "unix":
		if brokerURL.Host == "" && brokerURL.Path == "" {
			return fmt.Errorf("must include a socket path")
		}
		return nil
	default:
		return fmt.Errorf("unsupported scheme")
	}
	if brokerURL.Hostname() == "" {
		return fmt.Errorf("must include a host")
	}
	if port := brokerURL.Port(); port != "" {
		value, err := strconv.ParseUint(port, 10, 16)
		if err != nil || value == 0 {
			return fmt.Errorf("port must be between 1 and 65535")
		}
	}
	return nil
}

func validateTopicFilter(topic string) error {
	if len(topic) == 0 || len(topic) > 65535 || !utf8.ValidString(topic) || strings.ContainsRune(topic, '\x00') {
		return fmt.Errorf("topic filters must contain 1 to 65535 UTF-8 bytes without null characters")
	}
	levels := strings.Split(topic, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return fmt.Errorf("multi-level wildcard must occupy the final topic level")
		}
		if strings.Contains(level, "+") && level != "+" {
			return fmt.Errorf("single-level wildcard must occupy an entire topic level")
		}
	}
	return nil
}
