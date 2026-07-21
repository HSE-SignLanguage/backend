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
	Port      int
	log       *logger.MultiLogger
	Router    *chi.Mux
	serv      *http.Server
	lifecycle shutdownLifecycle
}

type shutdownLifecycle interface {
	BeginShutdown()
	Shutdown(context.Context) error
}

func NewServer(port int, logger *logger.MultiLogger, router *chi.Mux, lifecycles ...shutdownLifecycle) *Server {
	server := &Server{
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
	if len(lifecycles) > 0 {
		server.lifecycle = lifecycles[0]
	}
	return server
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
	if ctx == nil {
		ctx = context.Background()
	}
	if s.lifecycle != nil {
		s.lifecycle.BeginShutdown()
	}

	httpDone := make(chan error, 1)
	go func() {
		httpDone <- s.serv.Shutdown(ctx)
	}()

	workDone := make(chan error, 1)
	if s.lifecycle == nil {
		workDone <- nil
	} else {
		go func() {
			workDone <- s.lifecycle.Shutdown(ctx)
		}()
	}

	httpErr := <-httpDone
	workErr := <-workDone
	shutdownErr := errors.Join(httpErr, workErr)
	if shutdownErr == nil {
		return nil
	}

	// Shutdown leaves connections open when its context expires. Close regular
	// HTTP connections explicitly; upgraded sockets are owned by the lifecycle.
	closeErr := s.serv.Close()
	return errors.Join(shutdownErr, closeErr)
}
