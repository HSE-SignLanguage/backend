package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"streaming/config"
	"streaming/logger"
	"streaming/mlclient"
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
	log           *logger.MultiLogger
	demoAPIURL    string
	jobManager    *JobManager
	useMock       bool
	mlClient      *mlclient.Client
	useOpenRouter bool
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	mlAPIURL, err := config.GetEnv("ML_API_URL")
	if err != nil {
		log.Warn("ML_API_URL not set, using default", "error", err)
		mlAPIURL = "http://localhost:8080/process"
	}

	useMock := false
	if mockEnv, err := config.GetEnv("USE_MOCK"); err == nil {
		useMock = mockEnv == "true" || mockEnv == "1"
	}

	useOpenRouter := true
	if orEnv, err := config.GetEnv("USE_OPENROUTER"); err == nil {
		envVal := strings.ToLower(orEnv)
		useOpenRouter = envVal == "true" || envVal == "1" || envVal == "yes"
	}

	if useMock {
		log.Info("Mock mode enabled - will return test data")
	}

	if !useOpenRouter {
		log.Info("OpenRouter transcript improvement disabled via USE_OPENROUTER")
	}

	client, err := mlclient.NewClient(mlAPIURL)
	if err != nil {
		log.Fatal("failed to initialise ML API client", "error", err, "url", mlAPIURL)
	}

	return &HandlersConfig{
		log:           log,
		demoAPIURL:    mlAPIURL,
		jobManager:    NewJobManager(),
		useMock:       useMock,
		mlClient:      client,
		useOpenRouter: useOpenRouter,
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

	mockText := fmt.Sprintf("new literal text chunk %d", time.Now().Unix())
	literalText, err := hc.requestLiteralText(ctx, frames, mockText)
	if err != nil {
		hc.log.Error("failed to get literal from demo API", "error", err)
		return
	}

	literalText = strings.TrimSpace(literalText)
	if literalText == "" {
		hc.log.Warn("API returned empty text")
		return
	}

	if shouldSkipLiteral(literalText) {
		hc.log.Info("literal text indicates no update, skipping send")
		return
	}

	previousTranscript := *transcriptContext
	trimmedContext := hc.trimContext(previousTranscript, maxContextChars)

	updatedTranscript, newSegment, err := hc.updateTranscriptWithContext(trimmedContext, literalText)
	if err != nil {
		hc.log.Error("failed to update transcript", "error", err)
		newSegment = strings.TrimSpace(literalText)
		updatedTranscript = combineTranscript(trimmedContext, newSegment)
	}

	if strings.TrimSpace(newSegment) == "" {
		hc.log.Debug("no new transcript segment to send")
		return
	}

	*transcriptContext = updatedTranscript

	response := WebSocketMessage{Text: newSegment}
	if err := hc.sendTextToClient(ctx, c, writeMu, response); err != nil {
		hc.log.Error("failed to send text to websocket client", "error", err)
	}

	hc.log.Info("successfully processed and sent transcript segment", "segment_length", len(newSegment))
}

func (hc *HandlersConfig) trimContext(context string, maxChars int) string {
	if len(context) <= maxChars {
		return context
	}
	return context[len(context)-maxChars:]
}

func (hc *HandlersConfig) updateTranscriptWithContext(context, newLiteral string) (string, string, error) {
	chunk := strings.TrimSpace(newLiteral)

	if chunk == "" {
		return strings.TrimSpace(context), "", nil
	}

	if hc.useMock {
		if context == "" {
			chunk = "Mock transcript: " + chunk
		}
		return combineTranscript(context, chunk), chunk, nil
	}

	if !hc.useOpenRouter {
		return combineTranscript(context, chunk), chunk, nil
	}

	improvedChunk, err := utils.UpdateTranscript(context, newLiteral)
	if err != nil {
		return "", "", err
	}

	improvedChunk = strings.TrimSpace(improvedChunk)
	if improvedChunk == "" {
		return strings.TrimSpace(context), "", nil
	}

	return combineTranscript(context, improvedChunk), improvedChunk, nil
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

func (hc *HandlersConfig) requestLiteralText(ctx context.Context, frames [][]byte, mockText string) (string, error) {
	if len(frames) == 0 {
		return "", fmt.Errorf("no frames to send to ML API")
	}

	if hc.useMock {
		hc.log.Debug("using mock literal text")
		return mockText, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if hc.mlClient == nil {
		return "", fmt.Errorf("ml client is not configured")
	}

	text, err := hc.mlClient.ProcessFrames(ctx, frames)
	if err != nil {
		return "", fmt.Errorf("call ml api: %w", err)
	}

	hc.log.Info("received literal text from ML API", "text_length", len(text))
	return text, nil
}
