package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	newLiteral = strings.TrimSpace(newLiteral)
	if newLiteral == "" {
		return strings.TrimSpace(currentContext), nil
	}

	apiKey, err := config.GetEnv(openRouterAPIKeyEnvVar)
	if err != nil {
		return "", err
	}

	model, err := config.GetEnv(openRouterModelEnvVar)
	if err != nil {
		return "", err
	}

	prompt := buildPrompt(currentContext, newLiteral)

	reqBody := chatRequest{
		Model: model,
		Messages: []chatMessage{
			{
				Role:    "system",
				Content: "You turn literal sign-language transcripts into natural text. Extend the running transcript with the new literal chunk, fix grammar and punctuation, respond only with the full updated transcript, and always reply in the same language as the input.",
			},
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

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
		return "", fmt.Errorf("call openrouter: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("openrouter returned status %d", resp.StatusCode)
	}

	var parsed chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	if len(parsed.Choices) == 0 {
		return "", errors.New("openrouter response missing choices")
	}

	updated := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if updated == "" {
		return "", errors.New("empty transcript received from openrouter")
	}

	return updated, nil
}

func buildPrompt(currentContext, newLiteral string) string {
	var sb strings.Builder
	sb.WriteString("Current transcript (may be empty):\n")
	sb.WriteString(strings.TrimSpace(currentContext))
	sb.WriteString("\n\nNew literal chunk:\n")
	sb.WriteString(newLiteral)
	sb.WriteString("\n\nReturn the updated, natural-sounding transcript only.")
	return sb.String()
}
