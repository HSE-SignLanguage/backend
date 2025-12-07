package mlclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"streaming/utils"
)

const requestTimeout = 30 * time.Second

type processRequest struct {
	Frames []string `json:"frames"`
	Count  int      `json:"count"`
}

type processResponse struct {
	Text string `json:"text"`
}

type Client struct {
	endpoint   string
	httpClient *http.Client
}

func NewClient(endpoint string) (*Client, error) {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return nil, errors.New("ml api url is empty")
	}

	return &Client{
		endpoint:   trimmed,
		httpClient: &http.Client{},
	}, nil
}

func (c *Client) Endpoint() string {
	return c.endpoint
}

func (c *Client) ProcessFrames(ctx context.Context, frames [][]byte) (string, error) {
	if len(frames) == 0 {
		return "", errors.New("no frames to process")
	}

	reqBody := processRequest{
		Frames: utils.FramesToBase64(frames),
		Count:  len(frames),
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("encode frames: %w", err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call ml api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("ml api returned status %d", resp.StatusCode)
	}

	var parsed processResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode ml response: %w", err)
	}

	return strings.TrimSpace(parsed.Text), nil
}
