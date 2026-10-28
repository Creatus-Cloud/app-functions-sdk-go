// Package configvalidation validates isolated candidate configurations without applying them.
package configvalidation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/mitchellh/mapstructure"
)

const Route = "/api/v3/config/validate"
const MaxRequestBytes = 4 << 20

// Request contains only relative keys belonging to ServiceID. Configuration must
// be the complete candidate, including applicable common configuration layers.
type Request struct {
	ServiceID     string            `json:"serviceID"`
	Configuration map[string]string `json:"configuration"`
	Devices       []dtos.Device     `json:"devices,omitempty"`
}
type Issue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// Capabilities lists sections used by this service without exposing values.
// Known custom sections without a validator remain listed so shared defaults
// cannot hide a missing validator by being discarded during projection.
type Capabilities struct {
	ServiceID string   `json:"serviceID"`
	Sections  []string `json:"sections"`
}

type Response struct {
	ServiceID string  `json:"serviceID"`
	Valid     bool    `json:"valid"`
	Errors    []Issue `json:"errors,omitempty"`
}

type registration struct {
	sections []string
	factory  func() any
	validate func(any) error
}

// Registry owns immutable factories, never live service configuration pointers.
type Registry struct {
	serviceID     string
	mu            sync.RWMutex
	entries       []registration
	sections      map[string]bool
	knownSections map[string]bool
	devices       func([]dtos.Device) error
}

func New(serviceID string) *Registry {
	return &Registry{serviceID: serviceID, sections: make(map[string]bool), knownSections: make(map[string]bool)}
}

// Register adds a configuration group. The factory must return a fresh pointer
// to a struct on every call. Top-level struct fields are its allowed sections.
func (r *Registry) Register(factory func() any, validate func(any) error) error {
	if factory == nil || validate == nil {
		return fmt.Errorf("config validator factory and function are required")
	}
	sections, err := configurationSections(factory())
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range sections {
		if r.sections[name] {
			return fmt.Errorf("config section %s already registered", name)
		}
	}
	for _, name := range sections {
		r.sections[name] = true
		r.knownSections[name] = true
	}
	r.entries = append(r.entries, registration{sections, factory, validate})
	return nil
}

// RegisterCustom registers a single custom configuration section. Like
// LoadCustomConfig, prototype is an outer struct containing the named section.
// Only its type is retained; each request gets a new zero-valued configuration.
func (r *Registry) RegisterCustom(prototype any, sectionName string, validate func(any) error) error {
	sections, err := configurationSections(prototype)
	if err != nil {
		return err
	}
	if sectionName == "" || strings.Contains(sectionName, "/") || len(sections) != 1 || sections[0] != sectionName {
		return fmt.Errorf("configuration prototype must contain only section %q", sectionName)
	}
	typ := reflect.TypeOf(prototype).Elem()
	return r.Register(func() any { return reflect.New(typ).Interface() }, validate)
}

// RequireValidator records the sections loaded by a service. It never discovers
// or invokes methods on config, and does not retain the live object. Until a
// validator is explicitly registered for these sections, preflight rejects them.
func (r *Registry) RequireValidator(config any) error {
	sections, err := configurationSections(config)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range sections {
		r.knownSections[name] = true
	}
	return nil
}

func configurationSections(config any) ([]string, error) {
	typ := reflect.TypeOf(config)
	if typ == nil || typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct || reflect.ValueOf(config).IsNil() {
		return nil, fmt.Errorf("configuration must be a non-nil struct pointer")
	}
	sections := []string{}
	for i := 0; i < typ.Elem().NumField(); i++ {
		field := typ.Elem().Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("mapstructure"), ",")[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		sections = append(sections, name)
	}
	if len(sections) == 0 {
		return nil, fmt.Errorf("configuration has no sections")
	}
	return sections, nil
}

// SetDeviceValidator installs the protocol driver's existing read-only hook.
func (r *Registry) SetDeviceValidator(validate func([]dtos.Device) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.devices = validate
}

func (r *Registry) Validate(request Request) Response {
	result := Response{ServiceID: r.serviceID}
	fail := func(path, message string) { result.Errors = append(result.Errors, Issue{path, message}) }
	if request.ServiceID != r.serviceID {
		fail("serviceID", "target service does not match")
		return result
	}
	if request.Configuration == nil {
		fail("configuration", "complete candidate configuration is required")
		return result
	}
	r.mu.RLock()
	entries := append([]registration(nil), r.entries...)
	sections := make(map[string]bool, len(r.sections))
	for key, value := range r.sections {
		sections[key] = value
	}
	unsupported := []string{}
	for name := range r.knownSections {
		if !r.sections[name] {
			unsupported = append(unsupported, name)
		}
	}
	validateDevices := r.devices
	r.mu.RUnlock()
	sort.Strings(unsupported)
	for _, name := range unsupported {
		fail(name, "configuration validator is not registered")
	}
	if len(result.Errors) > 0 {
		return result
	}
	keys := make([]string, 0, len(request.Configuration))
	for key := range request.Configuration {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.Split(key, "/")
		valid := true
		for _, part := range parts {
			if part == "" || part == "." || part == ".." {
				valid = false
			}
		}
		if !valid || !sections[parts[0]] {
			fail(key, "unknown or unregistered configuration section")
		}
	}
	if len(result.Errors) > 0 {
		return result
	}
	for _, entry := range entries {
		raw := make(map[string]any)
		for _, key := range keys {
			top := strings.SplitN(key, "/", 2)[0]
			owned := false
			for _, section := range entry.sections {
				if section == top {
					owned = true
					break
				}
			}
			if !owned {
				continue
			}
			if err := insert(raw, key, request.Configuration[key]); err != nil {
				fail(key, err.Error())
			}
		}
		if len(result.Errors) > 0 {
			return result
		}
		candidate := entry.factory()
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: candidate, WeaklyTypedInput: true, ErrorUnused: true})
		if err == nil {
			err = decoder.Decode(raw)
		}
		if err != nil {
			fail(strings.Join(entry.sections, ","), "unknown field or invalid configuration value type")
			continue
		}
		if err = entry.validate(candidate); err != nil {
			fail(strings.Join(entry.sections, ","), err.Error())
		}
	}
	if len(request.Devices) > 0 {
		for _, device := range request.Devices {
			if device.ServiceName != r.serviceID {
				fail("devices", "device belongs to another service")
				return result
			}
		}
		if validateDevices == nil {
			fail("devices", "device validation is unavailable")
		} else if err := validateDevices(request.Devices); err != nil {
			fail("devices", err.Error())
		}
	}
	result.Valid = len(result.Errors) == 0
	return result
}

func insert(raw map[string]any, path, value string) error {
	parts := strings.Split(path, "/")
	current := raw
	for _, part := range parts[:len(parts)-1] {
		existing, ok := current[part]
		if !ok {
			existing = make(map[string]any)
			current[part] = existing
		}
		next, ok := existing.(map[string]any)
		if !ok {
			return fmt.Errorf("configuration key is both a value and a section")
		}
		current = next
	}
	key := parts[len(parts)-1]
	if _, exists := current[key]; exists {
		return fmt.Errorf("conflicting configuration keys")
	}
	current[key] = value
	return nil
}

// ServeHTTP provides a bounded, read-only validation endpoint. SDK route
// registration applies the usual EdgeX authentication middleware.
func (r *Registry) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if request.Method == http.MethodGet {
		r.mu.RLock()
		sections := make([]string, 0, len(r.knownSections))
		for section := range r.knownSections {
			sections = append(sections, section)
		}
		r.mu.RUnlock()
		sort.Strings(sections)
		if err := json.NewEncoder(w).Encode(Capabilities{ServiceID: r.serviceID, Sections: sections}); err != nil {
			return
		}
		return
	}
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, MaxRequestBytes))
	decoder.DisallowUnknownFields()
	var input Request
	err := decoder.Decode(&input)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = fmt.Errorf("expected one JSON object")
		}
	}
	result := Response{ServiceID: r.serviceID}
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadRequest
		result.Errors = []Issue{{"request", "invalid validation request"}}
	} else {
		result = r.Validate(input)
		if !result.Valid {
			status = http.StatusUnprocessableEntity
		}
	}
	w.WriteHeader(status)
	// The response contains only strings and booleans; an encoding failure can
	// only be a disconnected caller. There is no state to roll back or retry.
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}
