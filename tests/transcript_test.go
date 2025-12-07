package tests

import (
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
	got, err := utils.UpdateTranscript("  Hello world  ", "   ")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got != "Hello world" {
		t.Fatalf("expected existing transcript when literal empty, got %q", got)
	}
}

func TestUpdateTranscriptMissingEnv(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "")
	t.Setenv("OPENROUTER_MODEL", "")
	t.Setenv("OPENROUTER_SYSTEM_PROMPT", "")

	if _, err := utils.UpdateTranscript("context", "new"); err == nil {
		t.Fatalf("expected error when env vars are missing")
	}
}

func TestUpdateTranscriptSuccess(t *testing.T) {
	expectedResponse := "Updated transcript result."

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

		messages, ok := payload["messages"].([]any)
		if !ok || len(messages) != 2 {
			t.Fatalf("expected 2 messages, got %v", payload["messages"])
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{
					"role":    "assistant",
					"content": expectedResponse,
				},
			}},
		})
	}))
	defer server.Close()

	restore := utils.SetOpenRouterURLForTest(server.URL)
	defer restore()

	t.Setenv("OPENROUTER_API_KEY", "test-key")
	t.Setenv("OPENROUTER_MODEL", "test-model")
	t.Setenv("OPENROUTER_SYSTEM_PROMPT", "You polish transcripts")

	result, err := utils.UpdateTranscript("prev context", "new literal chunk")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != expectedResponse {
		t.Fatalf("unexpected transcript result: %q", result)
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

	if _, ok := os.LookupEnv("OPENROUTER_SYSTEM_PROMPT"); !ok {
		t.Skip("OPENROUTER_SYSTEM_PROMPT not set; skipping integration test")
	}

	current := "Вечером, когда солнце уже спряталось за крышами домов, Лера вышла на балкон полить свои цветы. Она делала это каждый день, но сегодня заметила кое-что необычное: на перилах лежал маленький блестящий ключ. Он был тёплым, будто его кто-то только что держал в руках. Лера огляделась — во дворе никого. Она взяла ключ, и в этот момент снизу тихо щёлкнуло. Лестничная клетка, где уже много лет был заколочен старый чердак, вдруг оказалась открытой. Дверь приоткрылась ровно настолько, чтобы пройти внутрь. Лера вдохнула поглубже. Внутри пахло пылью, но где-то вдали мерцал мягкий золотистый свет. Она шагнула внутрь. С каждой ступенькой свет становился ярче, и когда Лера добралась до самого верха, увидела на полу маленькую музыкальную шкатулку. Она была раскрыта, и из неё лилась тихая мелодия — та самая, которую мама пела ей в детстве. Лера опустилась на колени. На крышке было выгравировано: «Добро пожаловать домой». Шкатулка закрылась, свет погас, но ключ всё ещё светился в её ладони — как обещание, что чудеса всё ещё рядом. Как"
	newChunk := "ты думать о рассказ?"

	fmt.Printf("current context: %q\n", current)
	fmt.Printf("new literal chunk: %q\n", newChunk)

	result, err := utils.UpdateTranscript(current, newChunk)
	if err != nil {
		t.Fatalf("OpenRouter request failed: %v", err)
	}

	if result == "" {
		t.Fatalf("expected non-empty transcript result")
	}

	fmt.Printf("updated transcript: %q\n", result)
}
