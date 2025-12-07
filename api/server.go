package api

import (
	"context"
	"fmt"
	"net/http"
	"streaming/logger"

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
			Addr:    fmt.Sprintf(":%d", port),
			Handler: router,
		},
	}
}

func (s *Server) Start() {
	s.log.Info("Starting server", "port", s.Port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", s.Port), s.Router)
	if err != nil {
		s.log.Fatal("Server failed to start", "error", err)
	}
}

func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("stopping server")
	err := s.serv.Shutdown(ctx)
	return err
}
