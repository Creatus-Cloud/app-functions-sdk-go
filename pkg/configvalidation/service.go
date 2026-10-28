package configvalidation

import (
	"fmt"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
)

// ValidateService checks common settings consumed by both SDKs. Decoding also
// checks field types; custom semantic rules remain with their owning service.
func ValidateService(service config.ServiceInfo, logLevel string) error {
	if strings.TrimSpace(service.Host) == "" {
		return fmt.Errorf("Service.Host is required")
	}
	if service.Port < 1 || service.Port > 65535 {
		return fmt.Errorf("Service.Port must be between 1 and 65535")
	}
	if timeout, err := time.ParseDuration(service.RequestTimeout); err != nil || timeout <= 0 {
		return fmt.Errorf("Service.RequestTimeout must be a positive duration")
	}
	switch strings.ToUpper(logLevel) {
	case "TRACE", "DEBUG", "INFO", "WARN", "ERROR":
	default:
		return fmt.Errorf("Writable.LogLevel is invalid")
	}
	return nil
}

// ValidateTelemetry checks the duration used by the metrics ticker.
// Empty and zero intervals disable telemetry reporting.
func ValidateTelemetry(telemetry config.TelemetryInfo) error {
	if telemetry.Interval == "" {
		return nil
	}
	if interval, err := time.ParseDuration(telemetry.Interval); err != nil || interval < 0 {
		return fmt.Errorf("Writable.Telemetry.Interval must be a non-negative duration")
	}
	return nil
}
