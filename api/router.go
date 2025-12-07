package api

import (
	"net/http"
	"streaming/logger"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httprate"
)

func NewRouter(log *logger.MultiLogger) *chi.Mux {
	handlers := NewHandlersConfig(log)
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(httprate.Limit(
		20,
		5*time.Second,
		httprate.WithKeyFuncs(httprate.KeyByIP, httprate.KeyByEndpoint),
	))

	r.Route("/api", func(r chi.Router) {
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		r.Get("/socket", handlers.VideoSocketHandler)
	})

	return r
}
