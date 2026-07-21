package config

import (
	"reflect"
	"testing"
)

func TestParseSwaggerBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		host     string
		basePath string
		schemes  []string
	}{
		{
			name:     "public HTTPS URL with proxy path",
			value:    "https://hack.eferzo.xyz/api/",
			host:     "hack.eferzo.xyz",
			basePath: "/api",
			schemes:  []string{"https"},
		},
		{
			name:     "local URL",
			value:    "http://localhost:8080",
			host:     "localhost:8080",
			basePath: "/",
			schemes:  []string{"http"},
		},
		{
			name:     "legacy host-only value",
			value:    "localhost:8080",
			host:     "localhost:8080",
			basePath: "/",
			schemes:  []string{"http"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			host, basePath, schemes, err := parseSwaggerBaseURL(tt.value)
			if err != nil {
				t.Fatalf("parseSwaggerBaseURL() error = %v", err)
			}
			if host != tt.host {
				t.Errorf("host = %q, want %q", host, tt.host)
			}
			if basePath != tt.basePath {
				t.Errorf("basePath = %q, want %q", basePath, tt.basePath)
			}
			if !reflect.DeepEqual(schemes, tt.schemes) {
				t.Errorf("schemes = %v, want %v", schemes, tt.schemes)
			}
		})
	}
}

func TestParseSwaggerBaseURLRejectsUnsafeValues(t *testing.T) {
	t.Parallel()

	values := []string{
		"",
		"ftp://example.com/api",
		"https:///api",
		"https://user:password@example.com/api",
		"https://example.com/api?token=secret",
		"https://example.com/api#fragment",
	}

	for _, value := range values {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			if _, _, _, err := parseSwaggerBaseURL(value); err == nil {
				t.Fatalf("parseSwaggerBaseURL(%q) expected an error", value)
			}
		})
	}
}
