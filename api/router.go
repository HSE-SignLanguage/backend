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
	handlers := NewHandlersConfig(log)
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(httprate.Limit(
		20,
		5*time.Second,
		httprate.WithKeyFuncs(httprate.KeyByIP, httprate.KeyByEndpoint),
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
	r.Get("/jobs", handlers.ListJobs)

	return r
}
