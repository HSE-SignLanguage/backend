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

		// Add frame to buffer
		hc.mu.Lock()
		hc.framesBuffer = append(hc.framesBuffer, data)
		bufferLen := len(hc.framesBuffer)
		hc.mu.Unlock()

		// When we have 32 frames, send them to the demo API
		if bufferLen >= 32 {
			hc.mu.Lock()
			framesToSend := make([][]byte, 32)
			copy(framesToSend, hc.framesBuffer[:32])
			hc.framesBuffer = hc.framesBuffer[32:]
			hc.mu.Unlock()

			// Send frames to demo API in a goroutine to not block receiving
			go hc.sendFramesToDemoAPI(framesToSend)
		}
	}
}

func (hc *HandlersConfig) sendFramesToDemoAPI(frames [][]byte) {
	hc.log.Info("sending batch of frames to demo API", "count", len(frames), "url", hc.demoAPIURL)

	// Prepare the payload
	payload := map[string]interface{}{
		"frames": frames,
		"count":  len(frames),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		hc.log.Error("failed to marshal frames", "error", err)
		return
	}

	// Create HTTP request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		hc.log.Error("failed to create request", "error", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the request
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
