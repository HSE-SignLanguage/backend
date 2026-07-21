package api

import (
	"streaming/logger"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httprate"
	httpSwagger "github.com/swaggo/http-swagger"
)

func NewRouter(log *logger.MultiLogger) *chi.Mux {
	router, _ := NewRouterWithHandlers(log)
	return router
}

// NewRouterWithHandlers returns both the HTTP router and the lifecycle owner
// required to drain upgraded sockets and detached workers during shutdown.
func NewRouterWithHandlers(log *logger.MultiLogger) (*chi.Mux, *HandlersConfig) {
	handlers := NewHandlersConfig(log)
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(securityHeaders)
	r.Use(trustedRealIPMiddleware(log))
	r.Use(middleware.Logger)
	r.Use(httprate.Limit(
		20,
		5*time.Second,
		httprate.WithKeyFuncs(httprate.KeyByIP),
	))

	r.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))
	r.Get("/swagger-dev/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	r.Get("/health", handlers.HealthCheck)
	r.Get("/socket", handlers.VideoSocketHandler)
	r.Post("/upload", handlers.VideoUploadHandler)
	r.Get("/job/{id}", handlers.GetJobStatus)

	return r, handlers
}
