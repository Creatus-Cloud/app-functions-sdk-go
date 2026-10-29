package configvalidation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/edgexfoundry/go-mod-configuration/v4/configuration"
	"github.com/edgexfoundry/go-mod-configuration/v4/pkg/types"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

type testKeeperAuth struct{}

func (testKeeperAuth) AddAuthenticationData(*http.Request) error { return nil }
func (testKeeperAuth) RoundTripper() http.RoundTripper           { return http.DefaultTransport }

func TestTypedHTTPConfigurationMatchesKeeperRuntime(t *testing.T) {
	for _, literal := range []string{"1000000", "1e6", "1000000.0", "-1e6", "0", "1000000000000000128", `"9007199254740993"`, `"9223372036854775807"`} {
		t.Run(literal, func(t *testing.T) {
			const prefix = "edgex/v4/app-one"
			leaves := map[string]json.RawMessage{
				"AppCustom/Limit":   json.RawMessage(literal),
				"AppCustom/Rate":    json.RawMessage("1.25e-7"),
				"AppCustom/Enabled": json.RawMessage("true"),
				"AppCustom/Label":   json.RawMessage("true"),
			}
			keeper := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				response := map[string]any{"apiVersion": "v3", "statusCode": 200}
				keys := []string{}
				entries := []map[string]any{}
				for key, value := range leaves {
					keys = append(keys, prefix+"/"+key)
					var scalar any
					if err := json.Unmarshal(value, &scalar); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					// Keeper persists cast.ToString after JSON decoding and
					// its plaintext API returns the stored string.
					entries = append(entries, map[string]any{"key": prefix + "/" + key, "value": cast.ToString(scalar)})
				}
				response["response"] = entries
				if r.URL.Query().Get("keyOnly") == "true" {
					response["response"] = keys
				}
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Error(err)
				}
			}))
			defer keeper.Close()
			u, err := url.Parse(keeper.URL)
			require.NoError(t, err)
			port, err := strconv.Atoi(u.Port())
			require.NoError(t, err)
			client, err := configuration.NewConfigurationClient(types.ServiceConfig{
				Type: "keeper", Host: u.Hostname(), Port: port, BasePath: prefix, AuthInjector: testKeeperAuth{},
			})
			require.NoError(t, err)
			type settings struct {
				AppCustom struct {
					Limit   int64
					Rate    float64
					Enabled bool
					Label   string
				}
			}
			live := &settings{}
			_, err = client.GetConfiguration(live)
			require.NoError(t, err)
			require.Equal(t, "true", live.AppCustom.Label)
			registry := New("app-one")
			calls := 0
			require.NoError(t, registry.Register(func() any { return &settings{} }, func(candidate any) error {
				calls++
				require.Equal(t, live, candidate)
				return nil
			}))
			payload, err := json.Marshal(map[string]any{"serviceID": "app-one", "configuration": leaves})
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			registry.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, Route, bytes.NewReader(payload)))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, 1, calls)
		})
	}
}

func TestTypedConfigurationRejectsNonScalarLeaves(t *testing.T) {
	for _, value := range []any{nil, []any{1}, map[string]any{"Limit": 2}, math.Inf(1), math.NaN(), json.Number("1e9999"), json.Number("secret-invalid")} {
		t.Run(fmt.Sprintf("%T/%v", value, value), func(t *testing.T) {
			registry := New("app-one")
			calls := 0
			require.NoError(t, registry.Register(func() any { return &customConfig{} }, func(any) error { calls++; return nil }))
			result := registry.Validate(Request{ServiceID: "app-one", Configuration: map[string]any{"AppCustom/Limit": value}})
			require.False(t, result.Valid)
			require.Zero(t, calls)
			require.NotContains(t, fmt.Sprint(result.Errors), "secret-invalid")
		})
	}
	for _, literal := range []string{"null", "[]", `{"Limit":2}`} {
		recorder := httptest.NewRecorder()
		testRegistry(t).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, Route,
			io.NopCloser(bytes.NewBufferString(`{"serviceID":"app-one","configuration":{"AppCustom":`+literal+`}}`))))
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code, recorder.Body.String())
	}
}

func TestNativeNumbersUseHTTPDecodingRules(t *testing.T) {
	for _, value := range []any{int64(9007199254740993), json.Number("9007199254740993"), uint64(9007199254740993)} {
		registry := New("app-one")
		type settings struct{ Limit int64 }
		calls := 0
		require.NoError(t, registry.Register(func() any { return &settings{} }, func(candidate any) error {
			calls++
			require.Equal(t, int64(9007199254740992), candidate.(*settings).Limit)
			return nil
		}))
		input := map[string]any{"Limit": value}
		result := registry.Validate(Request{ServiceID: "app-one", Configuration: input})
		require.True(t, result.Valid, "%+v", result.Errors)
		require.Equal(t, 1, calls)
		require.Equal(t, value, input["Limit"], "normalization must not mutate the caller's map")
	}
}

func TestTypedHTTPRejectsValuesKeeperCannotDecodeAsIntegers(t *testing.T) {
	for _, value := range []string{"1.5", "true", "false"} {
		recorder := httptest.NewRecorder()
		testRegistry(t).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, Route,
			bytes.NewBufferString(`{"serviceID":"app-one","configuration":{"AppCustom/Limit":`+value+`}}`)))
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code, recorder.Body.String())
	}
}
