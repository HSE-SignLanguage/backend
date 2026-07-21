package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"streaming/logger"
)

func TestSecurityHeadersAreAppliedCentrally(t *testing.T) {
	handler := securityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil))

	assertSecurityHeaders(t, recorder)
}

func TestRouterProtectsHealthAndKeepsSwaggerUsable(t *testing.T) {
	t.Setenv("ML_API_URL", "http://127.0.0.1:8085/process")
	t.Setenv("USE_MOCK", "true")
	t.Setenv("USE_OPENROUTER", "false")
	router, handlers := NewRouterWithHandlers(&logger.MultiLogger{})
	defer handlers.Shutdown(context.Background())

	for _, path := range []string{"/health", "/swagger/index.html"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, http.StatusOK)
		}
		assertSecurityHeaders(t, recorder)
		if path == "/swagger/index.html" && !strings.Contains(recorder.Body.String(), "SwaggerUIBundle") {
			t.Fatal("Swagger UI bootstrap is missing")
		}
	}
}

func assertSecurityHeaders(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	want := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
		"Permissions-Policy":     "camera=(), microphone=(), geolocation=()",
	}
	for name, expected := range want {
		if got := recorder.Header().Get(name); got != expected {
			t.Errorf("%s = %q, want %q", name, got, expected)
		}
	}
}
