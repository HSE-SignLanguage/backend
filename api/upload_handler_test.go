package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"streaming/logger"
	"streaming/utils"
)

func TestSanitizeFilenameRemovesPathsAndUnsafeCharacters(t *testing.T) {
	got := sanitizeFilename(`../../folder\\my video?.mp4`)
	if got != "my_video_.mp4" {
		t.Fatalf("unexpected sanitized filename: %q", got)
	}
}

func TestJobSlotsRejectConcurrentVideoProcessing(t *testing.T) {
	handlers := &HandlersConfig{jobSlots: make(chan struct{}, maxConcurrentVideoJobs)}
	if !handlers.tryAcquireJobSlot() {
		t.Fatal("first video job must acquire the worker slot")
	}
	if handlers.tryAcquireJobSlot() {
		t.Fatal("concurrent video job must be rejected")
	}
	handlers.releaseJobSlot()
	if !handlers.tryAcquireJobSlot() {
		t.Fatal("released video job slot must become available")
	}
}

func TestVideoValidationErrorClassification(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		draining   bool
		status     int
		retryAfter string
	}{
		{name: "invalid media", err: fmt.Errorf("probe: %w", utils.ErrInvalidVideo), status: http.StatusUnsupportedMediaType},
		{name: "cancelled", err: fmt.Errorf("probe: %w", context.Canceled), status: http.StatusServiceUnavailable, retryAfter: "1"},
		{name: "timed out", err: fmt.Errorf("probe: %w", context.DeadlineExceeded), status: http.StatusServiceUnavailable, retryAfter: "1"},
		{name: "draining", err: errors.New("probe failed"), draining: true, status: http.StatusServiceUnavailable, retryAfter: "1"},
		{name: "operational failure", err: errors.New("ffprobe unavailable"), status: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, retryAfter := classifyVideoValidationError(tt.err, tt.draining)
			if status != tt.status || retryAfter != tt.retryAfter {
				t.Fatalf("classification = (%d, %q), want (%d, %q)", status, retryAfter, tt.status, tt.retryAfter)
			}
		})
	}
}

func TestVideoUploadShutdownDuringValidationReturnsServiceUnavailable(t *testing.T) {
	validationStarted := make(chan struct{})
	handlers := &HandlersConfig{
		log:           &logger.MultiLogger{},
		jobManager:    NewJobManager(),
		uploadSlots:   make(chan struct{}, maxConcurrentUploads),
		jobSlots:      make(chan struct{}, maxConcurrentVideoJobs),
		uploadTempDir: t.TempDir(),
		createExtractor: func(ctx context.Context, _ string) (*utils.VideoFrameExtractor, error) {
			close(validationStarted)
			<-ctx.Done()
			return nil, fmt.Errorf("validation cancelled: %w", ctx.Err())
		},
	}

	recorder := httptest.NewRecorder()
	request := newVideoUploadRequest(t)
	handlerDone := make(chan struct{})
	go func() {
		handlers.VideoUploadHandler(recorder, request)
		close(handlerDone)
	}()

	select {
	case <-validationStarted:
	case <-time.After(time.Second):
		t.Fatal("video validation did not start")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelShutdown()
	if err := handlers.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown did not drain cancelled validation: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("upload handler did not stop after validation cancellation")
	}

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("Retry-After = %q, want 1", retryAfter)
	}
}

func TestVideoUploadRequestCancellationDuringValidationReturnsServiceUnavailable(t *testing.T) {
	validationStarted := make(chan struct{})
	handlers := &HandlersConfig{
		log:           &logger.MultiLogger{},
		jobManager:    NewJobManager(),
		uploadSlots:   make(chan struct{}, maxConcurrentUploads),
		jobSlots:      make(chan struct{}, maxConcurrentVideoJobs),
		uploadTempDir: t.TempDir(),
		createExtractor: func(ctx context.Context, _ string) (*utils.VideoFrameExtractor, error) {
			close(validationStarted)
			<-ctx.Done()
			return nil, fmt.Errorf("validation cancelled: %w", ctx.Err())
		},
	}
	defer handlers.Shutdown(context.Background())

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request := newVideoUploadRequest(t).WithContext(requestCtx)
	recorder := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		handlers.VideoUploadHandler(recorder, request)
		close(handlerDone)
	}()

	select {
	case <-validationStarted:
	case <-time.After(time.Second):
		t.Fatal("video validation did not start")
	}
	cancelRequest()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("upload handler did not stop after request cancellation")
	}

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("Retry-After = %q, want 1", retryAfter)
	}
}

func newVideoUploadRequest(t *testing.T) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("video", "sample.mp4")
	if err != nil {
		t.Fatalf("create multipart video: %v", err)
	}
	if _, err := part.Write([]byte("fake video payload")); err != nil {
		t.Fatalf("write multipart video: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart body: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/upload", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}

func TestUploadTranscriptSuccessfulEmptyDeltaLeavesTranscriptUnchanged(t *testing.T) {
	server := newTranscriptUpdateServer(t, utils.TranscriptUpdate{
		FullText: "готовый текст",
		Delta:    "",
	})
	defer server.Close()
	restore := utils.SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	handlers := &HandlersConfig{useOpenRouter: true}
	updated, segment, err := handlers.updateUploadTranscript(context.Background(), "готовый текст", "сомнительный жест")
	if err != nil {
		t.Fatalf("valid no-op response returned error: %v", err)
	}
	if updated != "готовый текст" || segment != "" {
		t.Fatalf("successful empty delta changed upload transcript: full=%q delta=%q", updated, segment)
	}
}

func TestUploadTranscriptFallsBackToLiteralOnlyForInvalidResponse(t *testing.T) {
	server := newTranscriptUpdateServer(t, utils.TranscriptUpdate{
		FullText: "переписанный текст",
		Delta:    "",
	})
	defer server.Close()
	restore := utils.SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	handlers := &HandlersConfig{useOpenRouter: true}
	updated, segment, err := handlers.updateUploadTranscript(context.Background(), "готовый текст", "дом")
	if err == nil {
		t.Fatal("invalid formatter response must return an error")
	}
	if updated != "готовый текст дом" || segment != "дом" {
		t.Fatalf("invalid response did not use literal fallback: full=%q delta=%q", updated, segment)
	}
}

func newTranscriptUpdateServer(t *testing.T, update utils.TranscriptUpdate) *httptest.Server {
	t.Helper()
	content, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("encode transcript fixture: %v", err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]any{"role": "assistant", "content": string(content)},
				"finish_reason": "stop",
			}},
		})
	}))
}
