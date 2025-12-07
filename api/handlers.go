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
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
)

type HandlersConfig struct {
	log        *logger.MultiLogger
	demoAPIURL string
	jobManager *JobManager
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
		jobManager: NewJobManager(),
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
// @Summary Upload video for async frame-by-frame processing
// @Description Upload a video file and receive a job ID immediately. The video will be processed asynchronously in batches of 32 frames.
// @Description
// @Description **Process Flow:**
// @Description 1. Upload video file via multipart form data
// @Description 2. Receive job ID immediately (status: queued)
// @Description 3. Server processes video in background
// @Description 4. Poll /job/{id} endpoint to check status and get results
// @Description 5. When complete, full transcription is available
// @Description
// @Description **Immediate Response Format:**
// @Description ```json
// @Description {
// @Description   "job_id": "550e8400-e29b-41d4-a716-446655440000",
// @Description   "status": "queued",
// @Description   "message": "Video upload accepted, processing started"
// @Description }
// @Description ```
// @Description
// @Description **Frontend Example:**
// @Description ```javascript
// @Description // 1. Upload video
// @Description const formData = new FormData();
// @Description formData.append('video', videoFile);
// @Description const uploadResp = await fetch('http://localhost:8080/upload', {
// @Description   method: 'POST',
// @Description   body: formData
// @Description });
// @Description const { job_id } = await uploadResp.json();
// @Description
// @Description // 2. Poll for results
// @Description const pollInterval = setInterval(async () => {
// @Description   const statusResp = await fetch(`http://localhost:8080/job/${job_id}`);
// @Description   const job = await statusResp.json();
// @Description
// @Description   if (job.status === 'completed') {
// @Description     console.log('Transcription:', job.full_text);
// @Description     clearInterval(pollInterval);
// @Description   } else if (job.status === 'failed') {
// @Description     console.error('Processing failed:', job.error);
// @Description     clearInterval(pollInterval);
// @Description   }
// @Description }, 2000);
// @Description ```
// @Tags video
// @Accept multipart/form-data
// @Produce json
// @Param video formData file true "Video file to process"
// @Param interval formData int false "Frame extraction interval (default: 1, extract every frame)"
// @Success 202 {object} map[string]interface{} "Job created and processing started"
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

	// Generate job ID
	jobID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), header.Filename)

	// Save file with job ID
	tempFilePath := filepath.Join(tempDir, fmt.Sprintf("%s_%s", jobID, header.Filename))
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		hc.log.Error("failed to create temp file", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if _, err := io.Copy(tempFile, file); err != nil {
		tempFile.Close()
		os.Remove(tempFilePath)
		hc.log.Error("failed to save temp file", "error", err)
		http.Error(w, "failed to save video", http.StatusInternalServerError)
		return
	}
	tempFile.Close()

	// Create job
	_ = hc.jobManager.CreateJob(jobID, header.Filename)

	// Get interval parameter
	interval := 1
	if intervalStr := r.FormValue("interval"); intervalStr != "" {
		fmt.Sscanf(intervalStr, "%d", &interval)
	}

	// Start async processing
	go hc.processVideoAsync(jobID, tempFilePath, interval)

	// Return immediately with job ID
	response := map[string]interface{}{
		"job_id":  jobID,
		"status":  "queued",
		"message": "Video upload accepted, processing started",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)

	hc.log.Info("video upload accepted", "job_id", jobID)
}

// processVideoAsync processes video in the background
func (hc *HandlersConfig) processVideoAsync(jobID, tempFilePath string, interval int) {
	defer os.Remove(tempFilePath) // Clean up temp file when done

	hc.log.Info("starting video processing", "job_id", jobID)

	// Update job status to processing
	hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
		job.Status = JobStatusProcessing
	})

	// Extract frames
	extractor, err := utils.NewVideoFrameExtractor(tempFilePath)
	if err != nil {
		hc.log.Error("failed to create video extractor", "job_id", jobID, "error", err)
		hc.jobManager.FailJob(jobID, fmt.Sprintf("failed to process video: %v", err))
		return
	}
	defer extractor.Close()

	// Get video info
	videoInfo := extractor.GetVideoInfo()
	hc.log.Info("video info", "job_id", jobID, "info", videoInfo)

	// Extract frames with interval
	var frames [][]byte
	if interval > 1 {
		frames, err = extractor.ExtractFramesWithInterval(interval)
	} else {
		frames, err = extractor.ExtractAllFrames()
	}

	if err != nil {
		hc.log.Error("failed to extract frames", "job_id", jobID, "error", err)
		hc.jobManager.FailJob(jobID, fmt.Sprintf("failed to extract frames: %v", err))
		return
	}

	hc.log.Info("extracted frames", "job_id", jobID, "count", len(frames))

	// Split frames into batches
	batches := utils.BatchFrames(frames, 32)

	// Update job with batch info
	hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
		job.TotalFrames = len(frames)
		job.TotalBatches = len(batches)
		job.VideoInfo = videoInfo
	})

	hc.log.Info("created batches", "job_id", jobID, "batch_count", len(batches))

	// Process batches sequentially
	for i, batch := range batches {
		hc.log.Info("processing batch", "job_id", jobID, "batch_num", i+1, "batch_size", len(batch))

		text, err := hc.sendFrameBatchToDemoAPIWithResponse(batch)
		if err != nil {
			hc.log.Error("failed to send batch to demo API", "job_id", jobID, "batch_num", i+1, "error", err)
			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.FailedBatches++
				job.ProcessedBatches++
			})
		} else {
			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.SuccessfulBatches++
				job.ProcessedBatches++
			})
			// Add transcription result if we got text back
			if text != "" {
				hc.jobManager.AddTranscriptionResult(jobID, text)
			}
		}

		// Small delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	// Complete the job with full transcription
	job, _ := hc.jobManager.GetJob(jobID)
	fullText := strings.Join(job.Transcription, " ")
	hc.jobManager.CompleteJob(jobID, fullText)

	hc.log.Info("video processing completed", "job_id", jobID, "total_frames", len(frames), "successful_batches", job.SuccessfulBatches)
}

// GetJobStatus godoc
// @Summary Get video processing job status
// @Description Poll this endpoint to check the status of a video processing job and retrieve results when complete
// @Description
// @Description **Job Status Values:**
// @Description - `queued`: Job created, waiting to start
// @Description - `processing`: Currently extracting and processing frames
// @Description - `completed`: All processing done, transcription available
// @Description - `failed`: Processing encountered an error
// @Description
// @Description **Response includes:**
// @Description - Current status and progress (processed_batches / total_batches)
// @Description - Accumulated transcription results
// @Description - Full combined text when completed
// @Description - Video metadata and statistics
// @Tags video
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} api.VideoJob "Job status and results"
// @Failure 404 {object} map[string]string "Job not found"
// @Router /job/{id} [get]
func (hc *HandlersConfig) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	job, exists := hc.jobManager.GetJob(jobID)
	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// sendFrameBatchToDemoAPIWithResponse sends frames and returns the text response
func (hc *HandlersConfig) sendFrameBatchToDemoAPIWithResponse(frames [][]byte) (string, error) {
	payload := map[string]interface{}{
		"frames": frames,
		"count":  len(frames),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal frames: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", hc.demoAPIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("demo API returned error status: %d", resp.StatusCode)
	}

	// Try to parse response for text
	var apiResp WebSocketMessage
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		hc.log.Warn("failed to decode demo API response", "error", err)
		return "", nil // Not a fatal error
	}

	hc.log.Info("successfully sent batch to demo API", "status", resp.StatusCode, "text_length", len(apiResp.Text))
	return apiResp.Text, nil
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
