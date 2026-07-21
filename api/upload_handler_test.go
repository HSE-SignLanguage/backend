package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
