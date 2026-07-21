package api

import "testing"

func TestAppendTranscriptDeltaUsesOpenRouterDelta(t *testing.T) {
	fullText, delta := appendTranscriptDelta("привет", "  как дела  ")
	if fullText != "привет как дела" || delta != "как дела" {
		t.Fatalf("unexpected transcript update: full=%q delta=%q", fullText, delta)
	}
}

func TestAppendTranscriptDeltaPreservesTranscriptForEmptySuccessfulDelta(t *testing.T) {
	fullText, delta := appendTranscriptDelta("привет", "   ")
	if fullText != "привет" || delta != "" {
		t.Fatalf("unexpected no-op update: full=%q delta=%q", fullText, delta)
	}
}
