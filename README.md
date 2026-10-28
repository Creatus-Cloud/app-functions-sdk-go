
# Application Functions SDK
[![Build Status](https://jenkins.edgexfoundry.org/view/EdgeX%20Foundry%20Project/job/edgexfoundry/job/app-functions-sdk-go/job/main/badge/icon)](https://jenkins.edgexfoundry.org/view/EdgeX%20Foundry%20Project/job/edgexfoundry/job/app-functions-sdk-go/job/main/) [![Code Coverage](https://codecov.io/gh/edgexfoundry/app-functions-sdk-go/branch/main/graph/badge.svg?token=E4uzIvukPu)](https://codecov.io/gh/edgexfoundry/app-functions-sdk-go) [![Go Report Card](https://goreportcard.com/badge/github.com/edgexfoundry/app-functions-sdk-go)](https://goreportcard.com/report/github.com/edgexfoundry/app-functions-sdk-go) [![GitHub Latest Dev Tag)](https://img.shields.io/github/v/tag/edgexfoundry/app-functions-sdk-go?include_prereleases&sort=semver&label=latest-dev)](https://github.com/edgexfoundry/app-functions-sdk-go/tags) ![GitHub Latest Stable Tag)](https://img.shields.io/github/v/tag/edgexfoundry/app-functions-sdk-go?sort=semver&label=latest-stable) [![GitHub License](https://img.shields.io/github/license/edgexfoundry/app-functions-sdk-go)](https://choosealicense.com/licenses/apache-2.0/) ![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/edgexfoundry/app-functions-sdk-go) [![GitHub Pull Requests](https://img.shields.io/github/issues-pr-raw/edgexfoundry/app-functions-sdk-go)](https://github.com/edgexfoundry/app-functions-sdk-go/pulls) [![GitHub Contributors](https://img.shields.io/github/contributors/edgexfoundry/app-functions-sdk-go)](https://github.com/edgexfoundry/app-functions-sdk-go/contributors) [![GitHub Committers](https://img.shields.io/badge/team-committers-green)](https://github.com/orgs/edgexfoundry/teams/app-functions-sdk-go-committers/members) [![GitHub Commit Activity](https://img.shields.io/github/commit-activity/m/edgexfoundry/app-functions-sdk-go)](https://github.com/edgexfoundry/app-functions-sdk-go/commits) [![Go](https://github.com/edgexfoundry/app-functions-sdk-go/actions/workflows/go.yml/badge.svg)](https://github.com/edgexfoundry/app-functions-sdk-go/actions/workflows/go.yml)

Welcome the App Functions SDK for EdgeX. This sdk is meant to provide all the plumbing necessary for developers to get started in processing/transforming/exporting data out of EdgeX.

For latest documentation please visit 
  - https://docs.edgexfoundry.org/latest/microservices/application/ApplicationServices/
  - https://docs.edgexfoundry.org/latest/microservices/application/sdk/Purpose/

For a list of examples please visit https://docs.edgexfoundry.org/latest/examples/AppServiceExamples/

## Build Prerequisites

Please see the [edgex-go README](https://github.com/edgexfoundry/edgex-go/blob/master/README.md#prerequisites).

## Versioning

Please refer to the EdgeX Foundry [versioning policy](https://wiki.edgexfoundry.org/pages/viewpage.action?pageId=21823969) for information on how EdgeX services are released and how EdgeX services are compatible with one another.  Specifically, device services (and the associated SDK), application services (and the associated app functions SDK), and client tools (like the EdgeX CLI and UI) can have independent minor releases, but these services must be compatible with the latest major release of EdgeX.

## Long Term Support

Please refer to the EdgeX Foundry [LTS policy](https://wiki.edgexfoundry.org/display/FA/Long+Term+Support) for information on support of EdgeX releases. The EdgeX community does not offer support on any non-LTS release outside of the latest release.

## Configuration preflight (Creatus)

The SDK exposes authenticated `POST /api/v3/config/validate`. Authenticated `GET` on the same route lists
`serviceID` and known configuration `sections` (names only). app-config uses this to
exclude shared defaults the receiving service does not use. The request has a
`serviceID` matching this service and a `configuration` object whose values are
strings and whose keys are relative Keeper paths, for example
`AppCustom/Limit`. Send the complete proposed service configuration, including
applicable common layers, rather than a patch or the entire gateway snapshot.

Register the existing validator directly during application startup or driver
`Initialize`, using the SDK's `RegisterCustomConfigValidator` method:

```go
if err := app.service.RegisterCustomConfigValidator(
    &config.ServiceConfig{}, appCustomConfigName, app.validateConfig,
); err != nil {
    return err
}
```

The prototype is the same outer struct used with `LoadCustomConfig`, containing
only the named top-level section (for example `AppCustom`). The callback has
signature `func(any) error` and receives that outer struct. Reuse the validation
method called by startup and Core Keeper updates, including any combined checks.
Runtime application callbacks remain separate.

The SDK retains only the prototype's type and creates a fresh zero-valued object
for every request. Prototype values and live settings are never copied into a
candidate, so deleting a required setting cannot be hidden by an old value.
Keeper's mapstructure conventions apply. The validator may prepare or normalize
the isolated candidate, but must not perform I/O or mutate live state.
`LoadCustomConfig` only records which sections need validation; it never discovers
methods. A loaded section without an explicit validator blocks preflight,
including when the candidate omits it.

`ApplicationService` and `DeviceServiceSDK` now expose this registration API.
Consumers require an SDK release containing it. Existing factory-based
`ConfigValidationRegistrar` registration remains available for configuration
groups requiring a custom factory.

Validation rejects wrong service IDs, unknown sections/fields, invalid value
types, and failures returned by custom validators. Standard configuration is
type checked and common service settings are checked; this does not invent
protocol-specific rules absent from a driver. Success is HTTP 200 with
`{"serviceID":"...","valid":true}`. Invalid configuration is HTTP 422 with
`errors` containing paths and messages; malformed requests are HTTP 400.
Request bodies are limited to 4 MiB. The device SDK additionally accepts
`devices` belonging to this service and calls its existing `ValidateDevice`
hook. Validation does not write to Core Keeper or replace active settings.
