package api

import "net/http"

// securityHeaders applies browser-facing response protections centrally so
// API errors, health checks, Swagger assets and WebSocket handshakes share the
// same safe defaults. A global CSP is intentionally omitted: Swagger UI uses
// an inline bootstrap script, and allowing unsafe-inline would provide little
// useful protection. CSP can be added separately if that UI is self-hosted
// with hashes or nonces.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}
