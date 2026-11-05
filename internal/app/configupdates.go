//
// Copyright (c) 2021 Intel Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/internal/common"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/config"
)

// ConfigUpdateProcessor contains the data need to process configuration updates
type ConfigUpdateProcessor struct {
	svc *Service
}

// NewConfigUpdateProcessor creates a new ConfigUpdateProcessor which processes configuration updates triggered from
// the Configuration Provider
func NewConfigUpdateProcessor(svc *Service) *ConfigUpdateProcessor {
	return &ConfigUpdateProcessor{svc: svc}
}

// WaitForConfigUpdates waits for signal that configuration has been updated (triggered from by Configuration Provider)
// and then determines what was updated and does any special processing, if needed, for the updates.
func (processor *ConfigUpdateProcessor) WaitForConfigUpdates(configUpdated config.UpdatedStream) {
	svc := processor.svc
	svc.ctx.appWg.Add(1)
	// Capture the baseline before returning to bootstrap so the first update
	// cannot become the baseline before the listener goroutine starts.
	svc.config.SetPipelineFunctionParameterNamesLowercase()
	previousWritable := svc.config.CopyWritableInfo()

	go func() {
		defer svc.ctx.appWg.Done()
		lc := svc.LoggingClient()
		lc.Info("Waiting for App Service configuration updates...")

		for {
			select {
			case <-svc.ctx.appCtx.Done():
				lc.Info("Exiting waiting for App Service configuration updates")
				return

			case _, ok := <-configUpdated:
				if !ok {
					return
				}
				// set pipeline function parameter names to lowercase to avoid casing issues from what is in source configuration
				svc.config.SetPipelineFunctionParameterNamesLowercase()
				currentWritable, revision := svc.config.GetWritableInfoWithRevision()
				lc.Info("Processing App Service configuration updates")

				currentWritable = processor.processConfigChanges(previousWritable, currentWritable, revision)

				// A later reload may already be published. Compare the next signal
				// against the snapshot processed here, not that newer configuration.
				previousWritable = currentWritable
			}
		}
	}()
}

func (processor *ConfigUpdateProcessor) processConfigChanges(previous, current common.WritableInfo, revision uint64) common.WritableInfo {
	sdk := processor.svc
	lc := sdk.LoggingClient()
	if previous.StoreAndForward.MaxRetryCount != current.StoreAndForward.MaxRetryCount {
		lc.Infof("StoreAndForward MaxRetryCount changed to %d", current.StoreAndForward.MaxRetryCount)
	}

	// Startup can also apply settings, so reconcile against the actual loop on
	// every notification, not just changes from the listener's own baseline.
	enabledChanged := previous.StoreAndForward.Enabled != current.StoreAndForward.Enabled
	if err := sdk.applyStoreForwardSettings(current.StoreAndForward, revision); err != nil {
		if sdk.ctx.appCtx.Err() != nil {
			return current
		}
		lc.Errorf("Initializing Database for Store and Forward failed: %v", err)
		current.StoreAndForward.Enabled = false
	}
	if enabledChanged {
		lc.Infof("StoreAndForward Enabled changed to %v", current.StoreAndForward.Enabled)
	} else if previous.StoreAndForward.RetryInterval != current.StoreAndForward.RetryInterval {
		if interval, err := time.ParseDuration(current.StoreAndForward.RetryInterval); err == nil && interval > 0 {
			lc.Infof("StoreAndForward RetryInterval changed to %s", current.StoreAndForward.RetryInterval)
		}
	}

	if !reflect.DeepEqual(previous.Pipeline, current.Pipeline) {
		processor.processConfigChangedPipeline(current.Pipeline)
	}
	return current
}

func (processor *ConfigUpdateProcessor) processConfigChangedPipeline(settings common.PipelineInfo) {
	sdk := processor.svc

	if sdk.usingConfigurablePipeline {
		pipelines, err := sdk.loadConfigurableFunctionPipelines(settings)
		if err != nil {
			sdk.LoggingClient().Errorf("unable to reload Configurable Pipeline(s) from new configuration: %s: all pipelines have been disabled", err.Error())
			// Clear the pipeline transforms so error occurs when attempting to execute the pipeline(s).
			sdk.runtime.ClearAllFunctionsPipelineTransforms()
			return
		}

		sdk.runtime.TargetType = sdk.targetType

		// Update the pipelines with their new transforms
		for _, pipeline := range pipelines {
			// TODO: Look at better way to apply pipeline updates
			sdk.runtime.SetFunctionsPipelineTransforms(pipeline.Id, pipeline.Transforms)
			sdk.runtime.SetFunctionsPipelineTopics(pipeline.Id, pipeline.Topics)
		}

		sdk.LoggingClient().Info("Configurable Pipeline successfully reloaded from new configuration")
	}
}

func (svc *Service) findMatchingFunction(configurable reflect.Value, functionName string) (reflect.Value, reflect.Type, error) {
	// Find if there is a method with name identical to the functionName
	functionValue := configurable.MethodByName(functionName)

	if !functionValue.IsValid() {
		longestMatchedNameLength := 0
		count := configurable.Type().NumMethod()
		// Iterate over all method names to find a match
		for index := 0; index < count; index++ {
			method := configurable.Type().Method(index)
			// If the target configuration function name starts with actual method name then it is a match
			// If there are multiple matches then pick the one with the longest name
			if strings.Index(functionName, method.Name) == 0 && len(method.Name) > longestMatchedNameLength {
				longestMatchedNameLength = len(method.Name)
				functionValue = configurable.MethodByName(method.Name)
			}
		}
	}

	if functionValue.Kind() == reflect.Invalid {
		return functionValue, nil, fmt.Errorf("function %s is not a built in SDK function", functionName)
	} else if functionValue.IsNil() {
		return functionValue, nil, fmt.Errorf("invalid/missing configuration for %s", functionName)
	}

	functionType := functionValue.Type()
	return functionValue, functionType, nil
}
