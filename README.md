
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

## PostgreSQL queue storage protection

`Writable/StoreAndForward` accepts these live Core Keeper settings:

- `RetentionPolicy`: `keep-newest` (default) or `keep-oldest`.
- `MaxBatchItems`: maximum incoming rows per write transaction and maximum
  rows per retry page/acknowledgment transaction. Default `100`; range `1`–`1000`.
  Omission or `0` selects `100` for existing configurations. Changes apply on the
  next write/retry operation. Batches are processed immediately without waiting
  to fill. New write transactions also have a fixed 4 MiB encoded-JSON limit,
  including SDK metadata; an individual new row above that limit is rejected.
- `MinFreeBytes`: filesystem rotation threshold; default `1610612736` (1.5 GiB).
- `CriticalFreeBytes`: independent emergency floor; default `524288000`
  (500 MiB). It must be lower than `MinFreeBytes`. Zero selects the default;
  negative limits and invalid threshold ordering are rejected.
- `StoragePath`: absolute path on the **PostgreSQL data filesystem**. Mount the
  database volume read-only in the application container; do not point this at
  the application's overlay filesystem or use it for a remote database.
- `MaxStoredBytes`: total serialized-JSON byte cap. `0` always selects the
  default 64 MiB queue budget; a positive value sets an explicit byte limit.
  `-1` disables this additional cap and requires `StoragePath` so free-space
  protection stays active. Values below `-1` are rejected. The gateway services
  explicitly use `-1`. There is no configured entry-count limit.

Application services can opt into per-resource retention through the optional
`interfaces.StoreForwardRetentionConfigurer` API. Register a deterministic
`RetryDataSplitter` for an existing pipeline before `Run`. The splitter returns
individual payloads and stable opaque keys; the SDK never decodes application
reading fields. Export uses a JSON tuple of device ID and resource name as its key.

Above the pressure threshold, queues can grow. Under pressure, `keep-newest`
replaces one oldest queued item in the same pipeline and retention key for each
incoming keyed item. Queue order uses the existing `created` timestamp and ID;
original observation times remain in the payload. A resource without an existing
queued item cannot replace another resource's history: the write is rejected.
This is approximately size-stable for similarly sized readings, not a byte quota.
Services without a registered splitter, including notifications, retain global
oldest-first eviction of enough serialized bytes for the incoming retry object.
`keep-oldest` rejects new items under pressure for either type of queue.

Large incoming payloads are split into bounded database batches. Each batch
deletes its selected old rows and inserts its replacements together, with one
DELETE and one INSERT statement for keyed rotation. An error rolls back that
database batch; previously committed batches remain queued and are counted.
If a resource has fewer queued rows than incoming rows in a pressure batch,
only its newest inputs up to that existing count survive. Quiet resources remain
untouched. Reported discarded counts include inputs superseded within the batch.
The complete batch's write headroom is checked before any eviction. Missing
paths and failed space checks reject new writes, while counting and acknowledging
existing records remain available. Notification payloads stay opaque single rows;
their byte eviction deletes candidate IDs in bounded SQL groups.

The table has a nullable `retention_key` and an index on
`(retention_key, created, id)`. There is no `stored_bytes` column. Optional explicit
byte caps, unkeyed eviction and bounded retry pages calculate serialized sizes on
demand. With `MaxStoredBytes=-1`, keyed pressure rotation does not sum existing
queue payload sizes; incoming batch sizes are measured in one database call.

Before the input trigger and retry loop start, registration migrates existing
unkeyed batches for that pipeline into individual rows. Each batch is migrated
atomically and keeps its original enqueue time, payload timestamps, pipeline hash,
retry count, correlation ID and context. New IDs prevent a stale retry from
restoring an old batch. Completed batches are resumable after interruption;
malformed batches or insufficient emergency headroom leave that batch intact and
return a startup error. Migration does not evict data to meet configured caps.
Startup time and transient storage overhead depend on backlog size. The schema
upgrade removes any earlier `stored_bytes` column; stop the old service instance
before upgrading its schema. Do not run old and new SDK versions on the same queue.

PostgreSQL deletion makes space reusable inside its tables after vacuuming; it
does not normally increase filesystem free space immediately. Queue writes no
longer execute synchronous VACUUM. Background autovacuum is enabled per queue
table with a 1000-tuple threshold, 0.01 scale factor, 5 ms cost delay, and cost
limit 200. Table/TOAST truncation is disabled to avoid its exclusive table lock.
These starting values require load testing on the target storage. PostgreSQL's
global autovacuum must remain enabled. Queue mutations use a transaction advisory
lock shared by updated SDK instances, which permits concurrent normal VACUUM.
Neither rotation policy deletes the whole queue waiting for OS free space to rise.
New queue writes and retry updates are rejected at the independent
`CriticalFreeBytes` floor, or when estimated tuple/index/WAL write overhead would
consume that reserve. Successful delivery can still remove queued entries.

For the gateway, readings use a 2 GiB rotation threshold and notifications use
1.5 GiB, both with a 500 MiB emergency floor. This gives notifications additional
room to grow after readings start rotating. With a typical 300–400 MiB firmware
bundle, the lower threshold leaves about 600 MiB of working room above the
emergency floor after downloading a 400 MiB bundle. The agent checks the actual
download size; its 2 GiB maximum is a size limit, not reserved disk space. Docker
layers, WAL growth and concurrent writes can require more; these defaults must
fit the deployed partition and update sizes. This is a priority policy, not a
reservation between services or a guaranteed update budget.

Existing Core Keeper values take precedence over service file defaults. Update
both rotation thresholds during rollout; an old 500 MiB rotation setting must be
raised above the new 500 MiB emergency floor. The two settings are independently
runtime-writable and validated on every queue operation and in config preflight.
Set `MaxStoredBytes=-1` for filesystem-based retention; an existing value of `0`
selects the 64 MiB cap even when `StoragePath` is configured.

This is a storage-pressure policy, **not a hard filesystem quota**. WAL, indexes,
other services and database maintenance also consume space. Retain host disk
alerts and sufficient headroom; this policy cannot guarantee 500 MiB remains
available to every writer. Keep PostgreSQL autovacuum enabled. Existing database
files are not shrunk automatically, and `VACUUM FULL` is never used here.

Limits and policy are read on every write/retry. Lowering a byte cap also trims
existing entries at the next retry pass. Retries read at most `MaxBatchItems`
entries / about 4 MiB per page (one larger legacy entry is allowed), using a deletion-safe cursor and a
bounded pass. This prevents a disk-sized backlog being loaded entirely into RAM.
For paged stores, the 30-second pass budget and cancellation are checked before every retry item.
An in-flight export still finishes within its own timeout, and its result is
acknowledged before the pass stops. The cursor advances only through processed
items, so cancellation or a retry-loop restart does not skip the rest of a page.
Stores without paging check cancellation between items without a pass deadline;
otherwise a slow failing first item could prevent later items from ever retrying.

Other stores keep their existing behavior unless they implement
`interfaces.BoundedStoreClient`; explicitly configured limits reject an unsupported
backend at startup and on live reload. An incompatible reload disables retries
and queue writes until the settings are corrected and StoreAndForward is enabled
again. A release of this SDK is required before adding the new keys
to applications pinned to an older release.

## Live configuration snapshots

Writable accessors return independent snapshots, including nested maps. Use
`SetWritableInfo` or `UpdateWritableFromRaw` to change values; modifying a returned
snapshot does not update the service. Queue workers use `GetStoreAndForwardInfo`
to read only their scalar settings without copying pipeline or secret maps.

These changes require the matching `go-mod-bootstrap` patch: live reloads decode
into a fresh object, update only Writable, and telemetry obtains a fresh snapshot
for every report. Startup host overrides and other non-writable settings remain
in place during a writable reload.

The local SDK module uses `../go-mod-bootstrap` for development. Downstream Go
modules ignore dependency replacements, so their local test module/workspace must
also select that checkout. Publish the bootstrap fix first, replace this local
override with its released version, then release the SDK and update service pins.
