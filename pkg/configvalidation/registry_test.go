package configvalidation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/stretchr/testify/require"
)

type customConfig struct{ AppCustom customSettings }
type customSettings struct {
	Limit   int
	Enabled bool
	Entries map[string]int `mapstructure:"LegacyEntries"`
}

func (c *customConfig) Validate() error {
	if c.AppCustom.Limit < 1 {
		return fmt.Errorf("Limit must be positive")
	}
	// Some production validators prepare maps. They must receive fresh objects.
	for key := range c.AppCustom.Entries {
		c.AppCustom.Entries[key]++
	}
	return nil
}
func testRegistry(t *testing.T) *Registry {
	t.Helper()
	r := New("app-one")
	require.NoError(t, r.Register(func() any { return &customConfig{} }, func(raw any) error { return raw.(*customConfig).Validate() }))
	return r
}
func TestFreshCandidateAndKeeperDecoding(t *testing.T) {
	live := &customConfig{AppCustom: customSettings{Limit: 99, Entries: map[string]int{"live": 7}}}
	r := New("app-one")
	require.NoError(t, r.RequireValidator(live))
	require.NoError(t, r.Register(func() any { return &customConfig{} }, func(raw any) error { return raw.(*customConfig).Validate() }))
	request := Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "2", "AppCustom/Enabled": "true", "AppCustom/LegacyEntries/example": "4"}}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := r.Validate(request)
			if !result.Valid {
				t.Errorf("unexpected failure: %+v", result)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 99, live.AppCustom.Limit)
	require.Equal(t, map[string]int{"live": 7}, live.AppCustom.Entries)
	require.Equal(t, "4", request.Configuration["AppCustom/LegacyEntries/example"])
}
func TestRejectInvalidOrForeignConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		request Request
	}{
		{"wrong service", Request{ServiceID: "app-two", Configuration: map[string]string{"AppCustom/Limit": "2"}}},
		{"missing configuration", Request{ServiceID: "app-one"}},
		{"full gateway path", Request{ServiceID: "app-one", Configuration: map[string]string{"edgex/v4/app-one/AppCustom/Limit": "2"}}},
		{"unknown section", Request{ServiceID: "app-one", Configuration: map[string]string{"Other/Limit": "2"}}},
		{"unknown nested field", Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "2", "AppCustom/Typo": "1"}}},
		{"invalid type", Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "secret-value"}}},
		{"deleted required value", Request{ServiceID: "app-one", Configuration: map[string]string{}}},
		{"value and directory", Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom": "x", "AppCustom/Limit": "2"}}},
		{"foreign device", Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "2"}, Devices: []dtos.Device{{Name: "foreign", ServiceName: "device-two"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := testRegistry(t).Validate(test.request)
			require.False(t, response.Valid)
			require.NotEmpty(t, response.Errors)
			data, err := json.Marshal(response)
			require.NoError(t, err)
			require.NotContains(t, string(data), "secret-value")
		})
	}
}
func TestLoadedConfigRequiresExplicitRegistration(t *testing.T) {
	r := New("app-one")
	require.NoError(t, r.RequireValidator(&customConfig{}))
	require.False(t, r.Validate(Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "1"}}).Valid)
	require.False(t, r.Validate(Request{ServiceID: "app-one", Configuration: map[string]string{}}).Valid, "missing validators must not be hidden by deleting their section")
	response := httptest.NewRecorder()
	r.ServeHTTP(response, httptest.NewRequest(http.MethodGet, Route, nil))
	require.Contains(t, response.Body.String(), "AppCustom")
}
func TestDuplicateSectionsAreRejected(t *testing.T) {
	r := testRegistry(t)
	require.Error(t, r.Register(func() any { return &customConfig{} }, func(raw any) error { return raw.(*customConfig).Validate() }))
}
func TestHTTPValidation(t *testing.T) {
	r := testRegistry(t)
	tests := []struct {
		body   string
		status int
	}{
		{`{"serviceID":"app-one","configuration":{"AppCustom/Limit":"2"}}`, http.StatusOK},
		{`{"serviceID":"app-two","configuration":{"AppCustom/Limit":"2"}}`, http.StatusUnprocessableEntity},
		{`{"serviceID":"app-one","configuration":{}}`, http.StatusUnprocessableEntity},
		{`{"serviceID":"app-one","configuration":{"AppCustom/Limit":"2"},"unknown":true}`, http.StatusBadRequest},
		{`{"serviceID":"app-one","configuration":{"AppCustom/Limit":"2"}} {}`, http.StatusBadRequest},
		{strings.Repeat(" ", MaxRequestBytes+1) + `{}`, http.StatusBadRequest},
	}
	for _, test := range tests {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, Route, bytes.NewBufferString(test.body)))
		require.Equal(t, test.status, rec.Code)
	}
}
func TestDeviceValidatorOnlyReceivesOwnedDevices(t *testing.T) {
	r := New("device-one")
	calls := 0
	r.SetDeviceValidator(func(devices []dtos.Device) error {
		calls++
		require.Equal(t, "device-one", devices[0].ServiceName)
		return fmt.Errorf("invalid protocol")
	})
	result := r.Validate(Request{ServiceID: "device-one", Configuration: map[string]string{}, Devices: []dtos.Device{{Name: "one", ServiceName: "device-one"}}})
	require.False(t, result.Valid)
	require.Equal(t, 1, calls)
	result = r.Validate(Request{ServiceID: "device-one", Configuration: map[string]string{}, Devices: []dtos.Device{{Name: "two", ServiceName: "device-two"}}})
	require.False(t, result.Valid)
	require.Equal(t, 1, calls)
}

func TestCapabilitiesExposeOnlySectionNames(t *testing.T) {
	r := testRegistry(t)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, httptest.NewRequest(http.MethodGet, Route, nil))
	require.Equal(t, http.StatusOK, response.Code)
	var capabilities Capabilities
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &capabilities))
	require.Equal(t, "app-one", capabilities.ServiceID)
	require.Equal(t, []string{"AppCustom"}, capabilities.Sections)
	require.NotContains(t, response.Body.String(), "Limit")
}

func TestExplicitRegistrationUsesExistingValidator(t *testing.T) {
	r := New("app-one")
	live := &customConfig{AppCustom: customSettings{Limit: 99}}
	require.NoError(t, r.RequireValidator(live))
	request := Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "2"}}
	require.False(t, r.Validate(request).Valid, "loading a type with Validate must not register it")
	calls := 0
	require.NoError(t, r.Register(func() any { return &customConfig{} }, func(candidate any) error {
		calls++
		return candidate.(*customConfig).Validate()
	}))
	require.True(t, r.Validate(request).Valid)
	request.Configuration["AppCustom/Limit"] = "0"
	rejected := r.Validate(request)
	require.False(t, rejected.Valid)
	require.Equal(t, "Limit must be positive", rejected.Errors[0].Message)
	require.Equal(t, 2, calls)
	require.Equal(t, 99, live.AppCustom.Limit)
}

func TestRegisterCustomCreatesFreshCandidates(t *testing.T) {
	r := New("app-one")
	prototype := &customConfig{AppCustom: customSettings{Limit: 99, Entries: map[string]int{"live": 7}}}
	require.NoError(t, r.RequireValidator(prototype))
	var candidates []*customConfig
	require.NoError(t, r.RegisterCustom(prototype, "AppCustom", func(raw any) error {
		candidate := raw.(*customConfig)
		candidates = append(candidates, candidate)
		return candidate.Validate()
	}))
	request := Request{ServiceID: "app-one", Configuration: map[string]string{"AppCustom/Limit": "2", "AppCustom/LegacyEntries/one": "4"}}
	require.True(t, r.Validate(request).Valid)
	require.True(t, r.Validate(request).Valid)
	require.Len(t, candidates, 2)
	require.NotSame(t, prototype, candidates[0])
	require.NotSame(t, candidates[0], candidates[1])
	require.Equal(t, map[string]int{"one": 5}, candidates[0].AppCustom.Entries)
	require.Equal(t, map[string]int{"one": 5}, candidates[1].AppCustom.Entries)
	require.Equal(t, 99, prototype.AppCustom.Limit)
	require.Equal(t, map[string]int{"live": 7}, prototype.AppCustom.Entries)
	// Deleting required settings must not inherit values from the prototype.
	require.False(t, r.Validate(Request{ServiceID: "app-one", Configuration: map[string]string{}}).Valid)
}

func TestRegisterCustomRejectsInvalidRegistration(t *testing.T) {
	validate := func(raw any) error { return raw.(*customConfig).Validate() }
	for _, test := range []struct {
		name      string
		prototype any
		section   string
		validate  func(any) error
	}{
		{"nil prototype", nil, "AppCustom", validate},
		{"typed nil", (*customConfig)(nil), "AppCustom", validate},
		{"value", customConfig{}, "AppCustom", validate},
		{"section value instead of outer config", &customSettings{}, "AppCustom", validate},
		{"wrong section", &customConfig{}, "Other", validate},
		{"missing section", &customConfig{}, "", validate},
		{"nested section", &customConfig{}, "AppCustom/Limit", validate},
		{"nil callback", &customConfig{}, "AppCustom", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := New("app-one")
			require.Error(t, r.RegisterCustom(test.prototype, test.section, test.validate))
			require.Empty(t, r.entries)
		})
	}
	r := New("app-one")
	require.NoError(t, r.RegisterCustom(&customConfig{}, "AppCustom", validate))
	require.Error(t, r.RegisterCustom(&customConfig{}, "AppCustom", validate))
}
