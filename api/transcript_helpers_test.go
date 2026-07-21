package api

import "testing"

func TestAppendTranscriptDeltaUsesOpenRouterDelta(t *testing.T) {
	fullText, delta := appendTranscriptDelta("привет", "буквальный жест", "  как дела  ")
	if fullText != "привет как дела" || delta != "как дела" {
		t.Fatalf("unexpected transcript update: full=%q delta=%q", fullText, delta)
	}
}

func TestAppendTranscriptDeltaFallsBackToAcceptedLiteral(t *testing.T) {
	fullText, delta := appendTranscriptDelta("привет", "  дом  ", "   ")
	if fullText != "привет дом" || delta != "дом" {
		t.Fatalf("unexpected literal fallback: full=%q delta=%q", fullText, delta)
	}
}
