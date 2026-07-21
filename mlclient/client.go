package mlclient

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"streaming/utils"
)

const (
	requestTimeout      = 30 * time.Second
	maxResponseBodyLen  = 64 << 10
	maxTransientRetries = 2
	baseRetryDelay      = 100 * time.Millisecond
	maxRetryAfter       = 2 * time.Second
	maxRetryJitter      = 100 * time.Millisecond
)

type processRequest struct {
	Frames []string `json:"frames"`
	Count  int      `json:"count"`
}

type processResponse struct {
	Text       string  `json:"text"`
	Confidence float64 `json:"confidence"`
	Accepted   *bool   `json:"accepted"`
	ClassID    *int    `json:"class_id"`
}

// Prediction is the normalized ML response. Accepted defaults to true for
// legacy ML services that only return {"text":"..."}.
type Prediction struct {
	Text       string  `json:"text"`
	Confidence float64 `json:"confidence"`
	Accepted   bool    `json:"accepted"`
	ClassID    *int    `json:"class_id,omitempty"`
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
		httpClient: &http.Client{Timeout: requestTimeout},
	}, nil
}

func (c *Client) Endpoint() string {
	return c.endpoint
}

func (c *Client) ProcessFrames(ctx context.Context, frames [][]byte) (Prediction, error) {
	if len(frames) == 0 {
		return Prediction{}, errors.New("no frames to process")
	}

	reqBody := processRequest{
		Frames: utils.FramesToBase64(frames),
		Count:  len(frames),
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return Prediction{}, fmt.Errorf("encode frames: %w", err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	for attempt := 0; ; attempt++ {
		prediction, retryAfter, retryable, err := c.processFramesOnce(reqCtx, bodyBytes)
		if err == nil {
			return prediction, nil
		}
		if !retryable || attempt >= maxTransientRetries {
			return Prediction{}, err
		}

		delay := retryDelay(retryAfter, attempt)
		timer := time.NewTimer(delay)
		select {
		case <-reqCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Prediction{}, reqCtx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) processFramesOnce(ctx context.Context, bodyBytes []byte) (Prediction, time.Duration, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return Prediction{}, 0, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Prediction{}, 0, false, fmt.Errorf("call ml api: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBodyLen))
		resp.Body.Close()
		return Prediction{}, retryAfter, retryable, fmt.Errorf("ml api returned status %d", resp.StatusCode)
	}

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyLen+1))
	closeErr := resp.Body.Close()
	if readErr != nil {
		return Prediction{}, 0, false, fmt.Errorf("read ml response: %w", readErr)
	}
	if closeErr != nil {
		return Prediction{}, 0, false, fmt.Errorf("close ml response: %w", closeErr)
	}
	if len(responseBody) > maxResponseBodyLen {
		return Prediction{}, 0, false, errors.New("ml response is too large")
	}

	var parsed processResponse
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return Prediction{}, 0, false, fmt.Errorf("decode ml response: %w", err)
	}

	text := strings.TrimSpace(parsed.Text)
	accepted := text != ""
	if parsed.Accepted != nil {
		accepted = *parsed.Accepted
	}

	return Prediction{
		Text:       text,
		Confidence: parsed.Confidence,
		Accepted:   accepted,
		ClassID:    parsed.ClassID,
	}, 0, false, nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return minDuration(time.Duration(seconds)*time.Second, maxRetryAfter)
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		return minDuration(maxDuration(retryAt.Sub(now), 0), maxRetryAfter)
	}
	return 0
}

func retryDelay(retryAfter time.Duration, attempt int) time.Duration {
	delay := baseRetryDelay << attempt
	if retryAfter > delay {
		delay = retryAfter
	}
	return delay + retryJitter()
}

func retryJitter() time.Duration {
	var randomByte [1]byte
	if _, err := cryptorand.Read(randomByte[:]); err != nil {
		return 0
	}
	return time.Duration(int(randomByte[0]) * int(maxRetryJitter) / 255)
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
