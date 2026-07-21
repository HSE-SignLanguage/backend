package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"streaming/logger"
)

func TestHandlerLifecycleStopsAdmissionAndDrainsActiveWork(t *testing.T) {
	var lifecycle handlerLifecycle
	if !lifecycle.tryBeginWork() {
		t.Fatal("initial work must be admitted")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- lifecycle.shutdown(shutdownCtx)
	}()

	deadline := time.Now().Add(time.Second)
	for !lifecycle.isDraining() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !lifecycle.isDraining() {
		t.Fatal("shutdown did not close admission")
	}
	if lifecycle.tryBeginWork() {
		lifecycle.endWork()
		t.Fatal("work was admitted after shutdown started")
	}

	lifecycle.endWork()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("graceful shutdown failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after work drained")
	}
}

func TestHandlerLifecycleCancelsWorkBeforeCallerDeadline(t *testing.T) {
	var lifecycle handlerLifecycle
	if !lifecycle.tryBeginWork() {
		t.Fatal("initial work must be admitted")
	}

	workCancelled := make(chan struct{})
	go func() {
		<-lifecycle.context().Done()
		close(workCancelled)
		lifecycle.endWork()
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := lifecycle.shutdown(shutdownCtx); err != nil {
		t.Fatalf("work should clean up inside the reserved deadline: %v", err)
	}
	select {
	case <-workCancelled:
	case <-time.After(time.Second):
		t.Fatal("active work did not receive forced cancellation")
	}
}

func TestHandlerLifecycleStopsRegisteredWebSocketsImmediately(t *testing.T) {
	var lifecycle handlerLifecycle
	stopped := make(chan struct{})
	sessionID := lifecycle.registerWebSocket(func() { close(stopped) })
	defer lifecycle.unregisterWebSocket(sessionID)

	lifecycle.beginShutdown()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("registered WebSocket did not receive shutdown signal")
	}
}

func TestHealthCheckReportsDrainingState(t *testing.T) {
	handlers := &HandlersConfig{}
	handlers.BeginShutdown()

	recorder := httptest.NewRecorder()
	handlers.HealthCheck(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("Retry-After = %q, want 1", retryAfter)
	}
}

func TestLongRunningHandlersRejectNewWorkWhileDraining(t *testing.T) {
	handlers := &HandlersConfig{}
	handlers.BeginShutdown()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		path    string
	}{
		{name: "websocket", handler: handlers.VideoSocketHandler, method: http.MethodGet, path: "/socket"},
		{name: "upload", handler: handlers.VideoUploadHandler, method: http.MethodPost, path: "/upload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			tt.handler(recorder, httptest.NewRequest(tt.method, tt.path, nil))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
			}
			if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
				t.Fatalf("Retry-After = %q, want 1", retryAfter)
			}
		})
	}
}

func TestShutdownSendsGoingAwayToActiveWebSocket(t *testing.T) {
	handlers := &HandlersConfig{
		log:            &logger.MultiLogger{},
		webSocketSlots: make(chan struct{}, maxConcurrentSockets),
		webSocketsByIP: make(map[string]int),
	}
	server := httptest.NewServer(http.HandlerFunc(handlers.VideoSocketHandler))
	defer server.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Second)
	defer cancelDial()
	connection, _, err := websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer connection.CloseNow()

	handlers.BeginShutdown()
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	_, _, err = connection.Read(readCtx)
	if status := websocket.CloseStatus(err); status != websocket.StatusGoingAway {
		t.Fatalf("close status = %d, want %d (error: %v)", status, websocket.StatusGoingAway, err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if err := handlers.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown after WebSocket close: %v", err)
	}
}
