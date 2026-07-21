package api

import "testing"

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
