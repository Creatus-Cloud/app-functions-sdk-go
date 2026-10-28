package app

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/trigger/mqtt"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/configvalidation"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/labstack/echo/v4"
)

func (svc *Service) initializeConfigValidation() error {
	svc.configValidation = configvalidation.New(svc.serviceKey)
	if err := svc.configValidation.Register(func() any { return &common.ConfigurationStruct{} }, func(raw any) error {
		c := raw.(*common.ConfigurationStruct)
		if err := configvalidation.ValidateService(c.Service, c.Writable.LogLevel); err != nil {
			return err
		}
		if err := configvalidation.ValidateTelemetry(c.Writable.Telemetry); err != nil {
			return err
		}
		if err := svc.validateTriggerType(c.Trigger.Type); err != nil {
			return err
		}
		if strings.EqualFold(c.Trigger.Type, TriggerTypeMQTT) {
			if err := mqtt.ValidateConfig(c.Trigger); err != nil {
				return err
			}
		}
		if c.Writable.StoreAndForward.Enabled {
			d, err := time.ParseDuration(c.Writable.StoreAndForward.RetryInterval)
			if err != nil || d <= 0 {
				return fmt.Errorf("Writable.StoreAndForward.RetryInterval must be a positive duration")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return svc.AddCustomRoute(configvalidation.Route, interfaces.Authenticated, echo.WrapHandler(svc.configValidation), http.MethodGet, http.MethodPost)
}

// RegisterConfigValidator adds an isolated configuration factory and validator.
// It is an optional extension and does not change ApplicationService.
func (svc *Service) RegisterConfigValidator(factory func() any, validate func(any) error) error {
	if svc.configValidation == nil {
		return fmt.Errorf("configuration validation is not initialized")
	}
	return svc.configValidation.Register(factory, validate)
}

var _ interfaces.ConfigValidationRegistrar = (*Service)(nil)

// RegisterCustomConfigValidator registers an existing validator for the named
// custom section. The prototype has the same outer shape as LoadCustomConfig.
// Validation receives a fresh configuration and must not change live state.
func (svc *Service) RegisterCustomConfigValidator(prototype any, sectionName string, validate func(any) error) error {
	if svc.configValidation == nil {
		return fmt.Errorf("configuration validation is not initialized")
	}
	return svc.configValidation.RegisterCustom(prototype, sectionName, validate)
}
