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
	log          *logger.MultiLogger
	mu           sync.Mutex
	framesBuffer [][]byte
	demoAPIURL   string
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	demoAPIURL, err := config.GetEnv("DEMO_API_URL")
	if err != nil {
		log.Warn("DEMO_API_URL not set, using default", "error", err)
		demoAPIURL = "http://localhost:8080/process"
	}

	return &HandlersConfig{
		log:          log,
		framesBuffer: make([][]byte, 0, 32),
		demoAPIURL:   demoAPIURL,
	}
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
// @Description Establishes a WebSocket connection for receiving video frames. Frames are buffered and sent in batches of 32 to the processing API
// @Tags websocket
// @Accept octet-stream
// @Produce json
// @Success 101 {string} string "Switching Protocols"
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

		hc.mu.Lock()
		hc.framesBuffer = append(hc.framesBuffer, data)
		bufferLen := len(hc.framesBuffer)
		hc.mu.Unlock()

		if bufferLen >= 32 {
			hc.mu.Lock()
			framesToSend := make([][]byte, 32)
			copy(framesToSend, hc.framesBuffer[:32])
			hc.framesBuffer = hc.framesBuffer[32:]
			hc.mu.Unlock()

			go hc.sendFramesToAPI(framesToSend)
		}
	}
}

func (hc *HandlersConfig) sendFramesToAPI(frames [][]byte) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
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

	hc.log.Info("successfully sent frames to demo API", "status", resp.StatusCode)
}
