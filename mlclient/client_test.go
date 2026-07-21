package mlclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProcessFramesSupportsLegacyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"text": " привет "})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	prediction, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")})
	if err != nil {
		t.Fatalf("process frames: %v", err)
	}
	if prediction.Text != "привет" || !prediction.Accepted {
		t.Fatalf("unexpected legacy prediction: %#v", prediction)
	}
}

func TestProcessFramesRetriesTransientServiceUnavailable(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"text":       "дом",
			"confidence": 0.9,
			"accepted":   true,
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	prediction, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")})
	if err != nil {
		t.Fatalf("expected retry to recover: %v", err)
	}
	if prediction.Text != "дом" || calls.Load() != 3 {
		t.Fatalf("unexpected result after retry: prediction=%#v calls=%d", prediction, calls.Load())
	}
}

func TestProcessFramesDoesNotRetryTerminalClientError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")}); err == nil {
		t.Fatal("expected terminal ML error")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one request, got %d", calls.Load())
	}
}

func TestProcessFramesReadsExtendedPrediction(t *testing.T) {
	classID := 42
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"text":       "дом",
			"confidence": 0.87,
			"accepted":   false,
			"class_id":   classID,
			"reason":     "ambiguous",
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	prediction, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")})
	if err != nil {
		t.Fatalf("process frames: %v", err)
	}
	if prediction.Accepted || prediction.Confidence != 0.87 || prediction.ClassID == nil || *prediction.ClassID != classID || prediction.Reason != "ambiguous" {
		t.Fatalf("unexpected extended prediction: %#v", prediction)
	}
}

func TestProcessFramesRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"text":"` + strings.Repeat("x", maxResponseBodyLen) + `"}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if _, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")}); err == nil {
		t.Fatal("expected oversized response error")
	}
}

func TestProcessFramesRejectsInvalidConfidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"text":       "дом",
			"confidence": 1.5,
			"accepted":   true,
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if _, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")}); err == nil || !strings.Contains(err.Error(), "confidence") {
		t.Fatalf("expected invalid confidence error, got %v", err)
	}
}

func TestProcessFramesRejectsOversizedTranscriptToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"text":       strings.Repeat("я", 129),
			"confidence": 0.9,
			"accepted":   true,
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if _, err := client.ProcessFrames(context.Background(), [][]byte{[]byte("frame")}); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected oversized token error, got %v", err)
	}
}
