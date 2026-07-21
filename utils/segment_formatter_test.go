package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFormatTranscriptSegmentSendsStrictLowLatencyRequest(t *testing.T) {
	tokens := []TranscriptSegmentToken{
		{Text: "я", Confidence: 0.91, Sequence: 7},
		{Text: "работать", Confidence: 0.82, Sequence: 8},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("unexpected authorization header: %q", got)
		}

		var payload map[string]any
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload["model"] != "test-model" || payload["temperature"] != float64(0) {
			t.Fatalf("unexpected model request: %#v", payload)
		}

		provider, ok := payload["provider"].(map[string]any)
		if !ok || provider["require_parameters"] != true || provider["sort"] != "latency" || provider["allow_fallbacks"] != true {
			t.Fatalf("unexpected provider preferences: %#v", payload["provider"])
		}
		reasoning, ok := payload["reasoning"].(map[string]any)
		if !ok || reasoning["effort"] != "minimal" {
			t.Fatalf("unexpected reasoning preferences: %#v", payload["reasoning"])
		}

		format, ok := payload["response_format"].(map[string]any)
		if !ok || format["type"] != "json_schema" {
			t.Fatalf("unexpected response format: %#v", payload["response_format"])
		}
		schemaContainer, ok := format["json_schema"].(map[string]any)
		if !ok || schemaContainer["name"] != "transcript_segment" || schemaContainer["strict"] != true {
			t.Fatalf("unexpected JSON schema container: %#v", format["json_schema"])
		}
		schema, ok := schemaContainer["schema"].(map[string]any)
		if !ok || schema["additionalProperties"] != false {
			t.Fatalf("schema must reject extra fields: %#v", schemaContainer["schema"])
		}
		required, ok := schema["required"].([]any)
		if !ok || len(required) != 4 {
			t.Fatalf("unexpected required schema fields: %#v", schema["required"])
		}

		messages, ok := payload["messages"].([]any)
		if !ok || len(messages) != 2 {
			t.Fatalf("unexpected messages: %#v", payload["messages"])
		}
		userMessage, ok := messages[1].(map[string]any)
		if !ok {
			t.Fatalf("unexpected user message: %#v", messages[1])
		}
		var input transcriptSegmentInput
		if err := json.Unmarshal([]byte(userMessage["content"].(string)), &input); err != nil {
			t.Fatalf("decode segment input: %v", err)
		}
		if input.PriorContext != "мы обсуждали проект" || len(input.Tokens) != 2 || input.Tokens[0].Sequence != 7 || input.Tokens[1].Sequence != 8 {
			t.Fatalf("unexpected segment input: %#v", input)
		}

		content, err := json.Marshal(TranscriptSegment{
			SegmentText:     "Я работаю.",
			FirstSequence:   7,
			LastSequence:    8,
			SourceSequences: []uint64{7, 8},
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]any{"role": "assistant", "content": string(content)},
				"finish_reason": "stop",
			}},
		})
	}))
	defer server.Close()

	restore := SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	result, err := FormatTranscriptSegment(context.Background(), "  мы обсуждали проект  ", tokens)
	if err != nil {
		t.Fatalf("format segment: %v", err)
	}
	if result.SegmentText != "Я работаю." || result.FirstSequence != 7 || result.LastSequence != 8 || len(result.SourceSequences) != 2 {
		t.Fatalf("unexpected segment result: %#v", result)
	}
}

func TestFormatTranscriptSegmentRejectsMismatchedSequence(t *testing.T) {
	server := newSegmentResponseServer(t, `{"segment_text":"Я работаю.","first_sequence":7,"last_sequence":9,"source_sequences":[7,8]}`)
	defer server.Close()
	restore := SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	_, err := FormatTranscriptSegment(context.Background(), "", []TranscriptSegmentToken{
		{Text: "я", Confidence: 0.9, Sequence: 7},
		{Text: "работать", Confidence: 0.8, Sequence: 8},
	})
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("expected sequence validation error, got %v", err)
	}
}

func TestFormatTranscriptSegmentRejectsTrailingJSON(t *testing.T) {
	server := newSegmentResponseServer(t, `{"segment_text":"Дом.","first_sequence":1,"last_sequence":1,"source_sequences":[1]} {}`)
	defer server.Close()
	restore := SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	_, err := FormatTranscriptSegment(context.Background(), "", []TranscriptSegmentToken{{Text: "дом", Confidence: 1, Sequence: 1}})
	if err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("expected trailing JSON error, got %v", err)
	}
}

func TestFormatTranscriptSegmentRejectsMismatchedSourceSequences(t *testing.T) {
	server := newSegmentResponseServer(t, `{"segment_text":"Я работаю.","first_sequence":7,"last_sequence":8,"source_sequences":[7,9]}`)
	defer server.Close()
	restore := SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	_, err := FormatTranscriptSegment(context.Background(), "", []TranscriptSegmentToken{
		{Text: "я", Confidence: 0.9, Sequence: 7},
		{Text: "работать", Confidence: 0.8, Sequence: 8},
	})
	if err == nil || !strings.Contains(err.Error(), "source sequences") {
		t.Fatalf("expected source sequence validation error, got %v", err)
	}
}

func TestFormatTranscriptSegmentRejectsExcessiveExpansion(t *testing.T) {
	response := `{"segment_text":"` + strings.Repeat("очень ", 20) + `дом","first_sequence":1,"last_sequence":1,"source_sequences":[1]}`
	server := newSegmentResponseServer(t, response)
	defer server.Close()
	restore := SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	_, err := FormatTranscriptSegment(context.Background(), "", []TranscriptSegmentToken{{Text: "дом", Confidence: 1, Sequence: 1}})
	if err == nil || (!strings.Contains(err.Error(), "too large") && !strings.Contains(err.Error(), "too many words")) {
		t.Fatalf("expected expansion validation error, got %v", err)
	}
}

func TestFormatTranscriptSegmentValidatesInputBeforeRequest(t *testing.T) {
	tokens := make([]TranscriptSegmentToken, MaxTranscriptSegmentTokens+1)
	for index := range tokens {
		tokens[index] = TranscriptSegmentToken{Text: "жест", Confidence: 1, Sequence: uint64(index + 1)}
	}
	if _, err := FormatTranscriptSegment(context.Background(), "", tokens); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("expected token count error, got %v", err)
	}

	if _, err := FormatTranscriptSegment(context.Background(), "", []TranscriptSegmentToken{
		{Text: "один", Confidence: 1, Sequence: 2},
		{Text: "два", Confidence: 1, Sequence: 2},
	}); err == nil || !strings.Contains(err.Error(), "strictly increasing") {
		t.Fatalf("expected sequence ordering error, got %v", err)
	}
}

func TestLiteralSegmentTextIsDeterministic(t *testing.T) {
	tokens := []TranscriptSegmentToken{
		{Text: "  я ", Confidence: 0.9, Sequence: 1},
		{Text: "", Confidence: 0.1, Sequence: 2},
		{Text: "работать", Confidence: 0.8, Sequence: 3},
	}
	if got := LiteralSegmentText(tokens); got != "я работать" {
		t.Fatalf("unexpected literal fallback: %q", got)
	}
}

func TestNormalizeTranscriptTokenText(t *testing.T) {
	if got, err := NormalizeTranscriptTokenText("  дом  "); err != nil || got != "дом" {
		t.Fatalf("unexpected normalized token %q, %v", got, err)
	}
	if _, err := NormalizeTranscriptTokenText(strings.Repeat("я", MaxTranscriptTokenRunes+1)); err == nil {
		t.Fatal("expected oversized token to be rejected")
	}
}

func newSegmentResponseServer(t *testing.T, content string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]any{"role": "assistant", "content": content},
				"finish_reason": "stop",
			}},
		})
	}))
}
