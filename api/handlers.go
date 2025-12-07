package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"streaming/config"
	"streaming/logger"
	"streaming/utils"
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

// VideoUploadHandler godoc
// @Summary Upload video for frame-by-frame processing
// @Description Upload a video file, extract frames, and process them in batches of 32 frames. Unlike the WebSocket endpoint, this processes the entire video at once and returns a summary.
// @Description
// @Description **Process Flow:**
// @Description 1. Upload video file via multipart form data
// @Description 2. Server extracts frames from the video
// @Description 3. Frames are split into batches of 32
// @Description 4. Each batch is sent sequentially to the processing API
// @Description 5. Returns processing summary with statistics
// @Description
// @Description **Response Format:**
// @Description ```json
// @Description {
// @Description   "status": "completed",
// @Description   "total_frames": 250,
// @Description   "total_batches": 8,
// @Description   "successful_batches": 8,
// @Description   "video_info": {
// @Description     "fps": 25.0,
// @Description     "duration": 10.0,
// @Description     "frame_width": 1920,
// @Description     "frame_height": 1080,
// @Description     "estimated_frames": 250
// @Description   }
// @Description }
// @Description ```
// @Description
// @Description **Frontend Example:**
// @Description ```javascript
// @Description const formData = new FormData();
// @Description formData.append('video', videoFile);
// @Description formData.append('interval', '1'); // Optional: extract every Nth frame
// @Description
// @Description const response = await fetch('http://localhost:8080/upload', {
// @Description   method: 'POST',
// @Description   body: formData
// @Description });
// @Description
// @Description const result = await response.json();
// @Description console.log('Processing completed:', result);
// @Description ```
// @Tags video
// @Accept multipart/form-data
// @Produce json
// @Param video formData file true "Video file to process"
// @Param interval formData int false "Frame extraction interval (default: 1, extract every frame)"
// @Success 200 {object} map[string]interface{} "Processing result with status and metadata"
// @Failure 400 {object} map[string]string "Bad Request - Invalid file or format"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /upload [post]
func (hc *HandlersConfig) VideoUploadHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(100 << 20); err != nil {
		hc.log.Error("failed to parse multipart form", "error", err)
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("video")
	if err != nil {
		hc.log.Error("failed to get video file", "error", err)
		http.Error(w, "video file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	hc.log.Info("received video upload", "filename", header.Filename, "size", header.Size)

	tempDir := filepath.Join("tmp", "uploads")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		hc.log.Error("failed to create temp directory", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	tempFilePath := filepath.Join(tempDir, fmt.Sprintf("%d_%s", time.Now().Unix(), header.Filename))
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		hc.log.Error("failed to create temp file", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFilePath)
	}()

	if _, err := io.Copy(tempFile, file); err != nil {
		hc.log.Error("failed to save temp file", "error", err)
		http.Error(w, "failed to save video", http.StatusInternalServerError)
		return
	}
	tempFile.Close()

	hc.log.Info("extracting frames from video", "path", tempFilePath)
	extractor, err := utils.NewVideoFrameExtractor(tempFilePath)
	if err != nil {
		hc.log.Error("failed to create video extractor", "error", err)
		http.Error(w, "failed to process video", http.StatusInternalServerError)
		return
	}
	defer extractor.Close()

	// Get video info
	videoInfo := extractor.GetVideoInfo()
	hc.log.Info("video info", "info", videoInfo)

	// Extract frames (optionally with interval)
	var frames [][]byte
	interval := 1 // Default: extract every frame
	if intervalStr := r.FormValue("interval"); intervalStr != "" {
		fmt.Sscanf(intervalStr, "%d", &interval)
	}

	if interval > 1 {
		frames, err = extractor.ExtractFramesWithInterval(interval)
	} else {
		frames, err = extractor.ExtractAllFrames()
	}

	if err != nil {
		hc.log.Error("failed to extract frames", "error", err)
		http.Error(w, "failed to extract frames", http.StatusInternalServerError)
		return
	}

	hc.log.Info("extracted frames", "count", len(frames))

	// Split frames into batches of 32
	batches := utils.BatchFrames(frames, 32)
	hc.log.Info("created batches", "batch_count", len(batches))

	// Process batches sequentially
	successCount := 0
	for i, batch := range batches {
		hc.log.Info("processing batch", "batch_num", i+1, "batch_size", len(batch))

		if err := hc.sendFrameBatchToDemoAPI(batch); err != nil {
			hc.log.Error("failed to send batch to demo API", "batch_num", i+1, "error", err)
			// Continue processing remaining batches
		} else {
			successCount++
		}

		// Small delay between batches to avoid overwhelming the API
		time.Sleep(100 * time.Millisecond)
	}

	// Send response
	response := map[string]interface{}{
		"status":             "completed",
		"total_frames":       len(frames),
		"total_batches":      len(batches),
		"successful_batches": successCount,
		"video_info":         videoInfo,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	hc.log.Info("video processing completed", "total_frames", len(frames), "successful_batches", successCount)
}

// sendFrameBatchToDemoAPI sends a batch of frames to the demo API
func (hc *HandlersConfig) sendFrameBatchToDemoAPI(frames [][]byte) error {
	payload := map[string]interface{}{
		"frames": frames,
		"count":  len(frames),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal frames: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("demo API returned error status: %d", resp.StatusCode)
	}

	hc.log.Info("successfully sent batch to demo API", "status", resp.StatusCode)
	return nil
}
