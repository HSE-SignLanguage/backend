package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"streaming/config"
	"streaming/logger"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type HandlersConfig struct {
	log        *logger.MultiLogger
	demoAPIURL string
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	demoAPIURL, err := config.GetEnv("DEMO_API_URL")
	if err != nil {
		log.Warn("DEMO_API_URL not set, using default", "error", err)
		demoAPIURL = "http://localhost:8080/process"
	}

	return &HandlersConfig{
		log:        log,
		demoAPIURL: demoAPIURL,
	}
}

// WebSocketMessage represents the payload sent back to clients over the socket.
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
// @Description Establishes a WebSocket connection for receiving video frames. Frames are buffered and sent in batches of 32 to the processing API and the resulting text is streamed back to the client as JSON messages
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

			go hc.sendFramesToAPI(ctx, framesToSend, c, &writeMu)
		}
	}
}

func (hc *HandlersConfig) sendFramesToAPI(ctx context.Context, frames [][]byte, c *websocket.Conn, writeMu *sync.Mutex) {
	hc.log.Info("sending batch of frames to demo API", "count", len(frames), "url", hc.demoAPIURL)

	payload := map[string]interface{}{
		"frames": frames,
		"count":  len(frames),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		hc.log.Error("failed to marshal frames", "error", err)
		return
	}

	requestCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		hc.log.Error("failed to create request", "error", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		hc.log.Error("failed to send frames to demo API", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		hc.log.Error("demo API returned error status", "status", resp.StatusCode)
		return
	}

	var apiResp WebSocketMessage
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		hc.log.Error("failed to decode demo API response", "error", err)
		return
	}

	if apiResp.Text == "" {
		hc.log.Warn("demo API returned empty text field")
		return
	}

	if err := hc.sendTextToClient(ctx, c, writeMu, apiResp); err != nil {
		hc.log.Error("failed to send text to websocket client", "error", err)
		return
	}

	hc.log.Info("successfully sent frames to demo API and forwarded response", "status", resp.StatusCode)
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
