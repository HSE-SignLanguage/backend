package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"streaming/utils"
)

func TestUpdateTranscriptReturnsCurrentWhenNewLiteralEmpty(t *testing.T) {
	got, err := utils.UpdateTranscript(context.Background(), "  Hello world  ", "   ")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got.FullText != "Hello world" || got.Delta != "" {
		t.Fatalf("expected existing transcript and empty delta, got %#v", got)
	}
}

func TestUpdateTranscriptMissingEnv(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "")
	t.Setenv("OPENROUTER_MODEL", "")

	if _, err := utils.UpdateTranscript(context.Background(), "context", "new"); err == nil {
		t.Fatalf("expected error when env vars are missing")
	}
}

func TestUpdateTranscriptSuccess(t *testing.T) {
	expectedDelta := "обновлённый фрагмент"
	expectedFullText := "prev context " + expectedDelta
	structuredContent, err := json.Marshal(utils.TranscriptUpdate{
		FullText: expectedFullText,
		Delta:    expectedDelta,
	})
	if err != nil {
		t.Fatalf("failed to encode fixture: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}

		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("expected authorization header to be set, got %q", got)
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		if payload["model"] != "test-model" {
			t.Fatalf("expected model test-model, got %v", payload["model"])
		}
		if payload["temperature"] != float64(0) {
			t.Fatalf("expected zero temperature, got %v", payload["temperature"])
		}
		if payload["max_tokens"] != float64(256) {
			t.Fatalf("expected max_tokens 256, got %v", payload["max_tokens"])
		}
		provider, ok := payload["provider"].(map[string]any)
		if !ok || provider["require_parameters"] != true {
			t.Fatalf("expected provider.require_parameters=true, got %v", payload["provider"])
		}

		messages, ok := payload["messages"].([]any)
		if !ok || len(messages) != 2 {
			t.Fatalf("expected 2 messages, got %v", payload["messages"])
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{
					"role":    "assistant",
					"content": string(structuredContent),
				},
				"finish_reason": "stop",
			}},
		})
	}))
	defer server.Close()

	restore := utils.SetOpenRouterURLForTest(server.URL)
	defer restore()

	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	result, err := utils.UpdateTranscript(context.Background(), "prev context", "new literal chunk")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result.FullText != expectedFullText || result.Delta != expectedDelta {
		t.Fatalf("unexpected transcript result: %#v", result)
	}
}

func TestUpdateTranscriptRejectsNonAppendOnlyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		content, err := json.Marshal(utils.TranscriptUpdate{
			FullText: "переписанный старый текст новый фрагмент",
			Delta:    "новый фрагмент",
		})
		if err != nil {
			t.Fatalf("failed to encode fixture: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]any{"role": "assistant", "content": string(content)},
				"finish_reason": "stop",
			}},
		})
	}))
	defer server.Close()

	restore := utils.SetOpenRouterURLForTest(server.URL)
	defer restore()
	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")

	if _, err := utils.UpdateTranscript(context.Background(), "исходный текст", "новый literal"); err == nil {
		t.Fatal("expected a non-append-only response to be rejected")
	}
}

func TestUpdateTranscriptIntegrationOpenRouter(t *testing.T) {
	// Load environment from project root if present.
	_ = godotenv.Load("../.env")

	if _, ok := os.LookupEnv("OPENROUTER_API_KEY"); !ok {
		t.Skip("OPENROUTER_API_KEY not set; skipping integration test")
	}

	if _, ok := os.LookupEnv("OPENROUTER_MODEL"); !ok {
		t.Skip("OPENROUTER_MODEL not set; skipping integration test")
	}

	current := "Вечером, когда солнце уже спряталось за крышами домов, Лера вышла на балкон полить свои цветы. Она делала это каждый день, но сегодня заметила кое-что необычное: на перилах лежал маленький блестящий ключ. Он был тёплым, будто его кто-то только что держал в руках. Лера огляделась — во дворе никого. Она взяла ключ, и в этот момент снизу тихо щёлкнуло. Лестничная клетка, где уже много лет был заколочен старый чердак, вдруг оказалась открытой. Дверь приоткрылась ровно настолько, чтобы пройти внутрь. Лера вдохнула поглубже. Внутри пахло пылью, но где-то вдали мерцал мягкий золотистый свет. Она шагнула внутрь. С каждой ступенькой свет становился ярче, и когда Лера добралась до самого верха, увидела на полу маленькую музыкальную шкатулку. Она была раскрыта, и из неё лилась тихая мелодия — та самая, которую мама пела ей в детстве. Лера опустилась на колени. На крышке было выгравировано: «Добро пожаловать домой». Шкатулка закрылась, свет погас, но ключ всё ещё светился в её ладони — как обещание, что чудеса всё ещё рядом. Как"
	newChunk := "ты думать о рассказ?"

	fmt.Printf("current context: %q\n", current)
	fmt.Printf("new literal chunk: %q\n", newChunk)

	result, err := utils.UpdateTranscript(context.Background(), current, newChunk)
	if err != nil {
		t.Fatalf("OpenRouter request failed: %v", err)
	}

	if result.FullText == "" {
		t.Fatalf("expected non-empty transcript result")
	}

	fmt.Printf("updated transcript: %q\n", result.FullText)
}
