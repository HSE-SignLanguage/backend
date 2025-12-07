package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"streaming/config"
)

var openRouterURL = "https://openrouter.ai/api/v1/chat/completions"

const (
	openRouterAPIKeyEnvVar = "OPENROUTER_API_KEY"
	openRouterModelEnvVar  = "OPENROUTER_MODEL"
)

func SetOpenRouterURLForTest(url string) func() {
	prev := openRouterURL
	openRouterURL = url

	return func() {
		openRouterURL = prev
	}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

func UpdateTranscript(currentContext, newLiteral string) (string, error) {
	current := strings.TrimSpace(currentContext)
	newLiteral = strings.TrimSpace(newLiteral)
	if newLiteral == "" {
		log.Println("OpenRouter: literal chunk empty, returning current context")
		return current, nil
	}

	apiKey, err := config.GetEnv(openRouterAPIKeyEnvVar)
	if err != nil {
		log.Printf("OpenRouter: missing API key (%s): %v", openRouterAPIKeyEnvVar, err)
		return "", err
	}

	model, err := config.GetEnv(openRouterModelEnvVar)
	if err != nil {
		log.Printf("OpenRouter: missing model (%s): %v", openRouterModelEnvVar, err)
		return "", err
	}

	log.Printf("OpenRouter: preparing request (model=%s, context_len=%d, literal_len=%d)", model, len(current), len(newLiteral))

	prompt := buildPrompt(currentContext, newLiteral)

	reqBody := chatRequest{
		Model: model,
		Messages: []chatMessage{
			// {
			// 	Role:    "system",
			// 	Content: "Вы преобразуете буквальную расшифровку текста на языке жестов в естественный текст. Расширьте текущую расшифровку новым фрагментом, сохраняйте естественность грамматики и языка и отвечайте полностью обновленной расшифровкой. Если текстовая часть является бессмысленной, оставьте предыдущую расшифровку без изменений. ВАЖНО: всегда пишите на русском языке.",
			// },
			{
				Role:    "user",
				Content: "Вы преобразуете буквальную расшифровку текста на языке жестов в естественный текст. Расширьте текущую расшифровку новым фрагментом, сохраняйте естественность грамматики и языка и отвечайте полностью обновленной расшифровкой. Если текстовая часть является бессмысленной, оставьте предыдущую расшифровку без изменений. ВАЖНО: всегда пишите на русском языке и никогда не используй форматирование." + prompt,
			},
		},
	}

	log.Printf("OpenRouter: sending request payload: %v", reqBody)

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("encode request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, openRouterURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		log.Printf("OpenRouter: request failed: %v", err)
		return "", fmt.Errorf("call openrouter: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("OpenRouter: non-200 status %d", resp.StatusCode)
		return "", fmt.Errorf("openrouter returned status %d", resp.StatusCode)
	}

	var parsed chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		log.Printf("OpenRouter: decode error: %v", err)
		return "", fmt.Errorf("decode response: %w", err)
	}

	if len(parsed.Choices) == 0 {
		log.Println("OpenRouter: response missing choices")
		return "", errors.New("openrouter response missing choices")
	}

	updated := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if updated == "" {
		log.Println("OpenRouter: response contained empty transcript")
		return "", errors.New("empty transcript received from openrouter")
	}

	log.Printf("OpenRouter: success (updated_len=%d)", len(updated))

	log.Printf("OpenRouter: response preview: %.80s", updated)

	return updated, nil
}

func buildPrompt(currentContext, newLiteral string) string {
	var sb strings.Builder
	sb.WriteString("Current transcript (may be empty):\n")
	sb.WriteString(strings.TrimSpace(currentContext))
	sb.WriteString("\n\nNew literal chunk:\n")
	sb.WriteString(newLiteral)
	sb.WriteString("\n\nReturn the full updated transcript, sounding natural and coherent.")
	return sb.String()
}
