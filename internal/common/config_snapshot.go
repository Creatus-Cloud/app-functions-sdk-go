package common

import (
	"maps"

	bootstrapConfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
)

func cloneWritableInfo(value WritableInfo) WritableInfo {
	value.Pipeline.PerTopicPipelines = maps.Clone(value.Pipeline.PerTopicPipelines)
	value.Pipeline.Functions = maps.Clone(value.Pipeline.Functions)
	for name, function := range value.Pipeline.Functions {
		function.Parameters = maps.Clone(function.Parameters)
		value.Pipeline.Functions[name] = function
	}
	value.InsecureSecrets = cloneInsecureSecrets(value.InsecureSecrets)
	value.Telemetry = cloneTelemetry(value.Telemetry)
	return value
}

func cloneInsecureSecrets(value bootstrapConfig.InsecureSecrets) bootstrapConfig.InsecureSecrets {
	value = maps.Clone(value)
	for name, secret := range value {
		secret.SecretData = maps.Clone(secret.SecretData)
		value[name] = secret
	}
	return value
}

func cloneTelemetry(value bootstrapConfig.TelemetryInfo) bootstrapConfig.TelemetryInfo {
	value.Metrics = maps.Clone(value.Metrics)
	value.Tags = maps.Clone(value.Tags)
	return value
}
