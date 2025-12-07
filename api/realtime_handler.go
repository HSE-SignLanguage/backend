package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"streaming/config"
	"streaming/logger"
	"streaming/utils"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	maxContextChars = 1000
)

type HandlersConfig struct {
	log        *logger.MultiLogger
	demoAPIURL string
	jobManager *JobManager
	useMock    bool
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	demoAPIURL, err := config.GetEnv("DEMO_API_URL")
	if err != nil {
		log.Warn("DEMO_API_URL not set, using default", "error", err)
		demoAPIURL = "http://localhost:8080/process"
	}

	useMock := false
	if mockEnv, err := config.GetEnv("USE_MOCK"); err == nil {
		useMock = mockEnv == "true" || mockEnv == "1"
	}

	if useMock {
		log.Info("Mock mode enabled - will return test data")
	}

	return &HandlersConfig{
		log:        log,
		demoAPIURL: demoAPIURL,
		jobManager: NewJobManager(),
		useMock:    useMock,
	}
}

type WebSocketMessage struct {
	Text string `json:"text"`
}

// HealthCheck godoc
// @Summary Health check endpoint
// @Description Check if the API is running and healthy
// @Tags health
// @Produce plain
// @Success 200 {string} string "OK"
// @Router /health [get]
func (hc *HandlersConfig) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// VideoSocketHandler godoc
// @Summary WebSocket endpoint for video frame streaming
// @Description Establishes a WebSocket connection for receiving video frames. Send binary frames to the server, and receive text responses back.
// @Description
// @Description **Client Flow:**
// @Description 1. Connect to the WebSocket endpoint (ws://localhost:8080/socket)
// @Description 2. Send video frames as binary messages (MessageBinary)
// @Description 3. Server buffers frames and sends batches of 32 to processing API
// @Description 4. Receive text responses as JSON messages (MessageText)
// @Description
// @Description **Response Format:**
// @Description The server sends back JSON text messages with the structure:
// @Description ```json
// @Description {
// @Description   "text": "extracted or processed text from the frames"
// @Description }
// @Description ```
// @Description
// @Description **Frontend Example:**
// @Description ```javascript
// @Description const ws = new WebSocket('ws://localhost:8080/socket');
// @Description
// @Description // Send binary frame data
// @Description ws.send(frameDataBlob);
// @Description
// @Description // Receive text messages
// @Description ws.onmessage = (event) => {
// @Description   const data = JSON.parse(event.data);
// @Description   console.log('Received text:', data.text);
// @Description };
// @Description ```
// @Tags websocket
// @Accept octet-stream
// @Produce json
// @Success 101 {object} api.WebSocketMessage "WebSocket response with extracted text"
// @Failure 400 {string} string "Bad Request - Failed to accept websocket"
// @Router /socket [get]
func (hc *HandlersConfig) VideoSocketHandler(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		hc.log.Error("failed to accept websocket", "error", err)
		http.Error(w, "failed to accept websocket", http.StatusBadRequest)
		return
	}

	c.SetReadLimit(10 * 1024 * 1024)

	hc.log.Info("Client connected to video socket")
	defer func() {
		c.Close(websocket.StatusNormalClosure, "closing connection")
		hc.log.Info("Client disconnected from video socket")
	}()

	hc.handleFrameStream(r.Context(), c)
}

func (hc *HandlersConfig) handleFrameStream(ctx context.Context, c *websocket.Conn) {
	framesBuffer := make([][]byte, 0, 32)
	var writeMu sync.Mutex
	transcriptContext := ""

	for {
		typ, data, err := c.Read(ctx)

		if err != nil {
			hc.log.Error("error reading from websocket", "error", err)
			break
		}

		if typ != websocket.MessageBinary {
			hc.log.Warn("received non-binary message, skipping")
			continue
		}

		if len(data) == 0 {
			continue
		}

		hc.log.Info("received frame", "size", len(data))

		framesBuffer = append(framesBuffer, data)
		if len(framesBuffer) >= 32 {
			framesToSend := make([][]byte, 32)
			copy(framesToSend, framesBuffer[:32])
			framesBuffer = framesBuffer[32:]

			go hc.sendFramesToAPI(ctx, framesToSend, c, &writeMu, &transcriptContext)
		}
	}
}

func (hc *HandlersConfig) sendFramesToAPI(ctx context.Context, frames [][]byte, c *websocket.Conn, writeMu *sync.Mutex, transcriptContext *string) {
	hc.log.Info("sending batch of frames to API", "count", len(frames), "url", hc.demoAPIURL)

	var literalText string
	if hc.useMock {
		literalText = fmt.Sprintf("new literal text chunk %d", time.Now().Unix())
		hc.log.Debug("using mock literal text")
	} else {
		var err error
		literalText, err = hc.getRawLiteralFromAPI(frames)
		if err != nil {
			hc.log.Error("failed to get literal from demo API", "error", err)
			return
		}
	}

	if literalText == "" {
		hc.log.Warn("API returned empty text")
		return
	}

	trimmedContext := hc.trimContext(*transcriptContext, maxContextChars)

	updatedTranscript, err := hc.updateTranscriptWithContext(trimmedContext, literalText)
	if err != nil {
		hc.log.Error("failed to update transcript", "error", err)
		updatedTranscript = strings.TrimSpace(trimmedContext + " " + literalText)
	}

	*transcriptContext = updatedTranscript

	response := WebSocketMessage{Text: updatedTranscript}
	if err := hc.sendTextToClient(ctx, c, writeMu, response); err != nil {
		hc.log.Error("failed to send text to websocket client", "error", err)
	}

	hc.log.Info("successfully processed and sent transcript", "context_length", len(updatedTranscript))
}

func (hc *HandlersConfig) getRawLiteralFromAPI(frames [][]byte) (string, error) {
	payload := map[string]interface{}{
		"frames": frames,
		"count":  len(frames),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal frames: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var apiResp WebSocketMessage
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	return apiResp.Text, nil
}

func (hc *HandlersConfig) trimContext(context string, maxChars int) string {
	if len(context) <= maxChars {
		return context
	}
	return context[len(context)-maxChars:]
}

func (hc *HandlersConfig) updateTranscriptWithContext(context, newLiteral string) (string, error) {
	if hc.useMock {
		if context == "" {
			return "Mock transcript: " + newLiteral, nil
		}
		return context + " " + newLiteral, nil
	}

	return utils.UpdateTranscript(context, newLiteral)
}

func (hc *HandlersConfig) sendTextToClient(ctx context.Context, c *websocket.Conn, writeMu *sync.Mutex, message WebSocketMessage) error {
	writeMu.Lock()
	defer writeMu.Unlock()

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return c.Write(ctx, websocket.MessageText, data)
}
