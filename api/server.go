package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"streaming/logger"
	"time"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	Port   int
	log    *logger.MultiLogger
	Router *chi.Mux
	serv   *http.Server
}

func NewServer(port int, logger *logger.MultiLogger, router *chi.Mux) *Server {
	return &Server{
		Port:   port,
		log:    logger,
		Router: router,
		serv: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           router,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       maxUploadReadTime,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 20,
		},
	}
}

func (s *Server) Start() {
	s.log.Info("Starting server", "port", s.Port)
	err := s.serv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Fatal("Server failed to start", "error", err)
	}
}

func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("stopping server")
	err := s.serv.Shutdown(ctx)
	return err
}
