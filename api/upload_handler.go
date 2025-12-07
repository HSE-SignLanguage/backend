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
	"streaming/utils"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

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
		hc.log.Error("failed to parse multipart form", "error", err, "content_type", r.Header.Get("Content-Type"))
		http.Error(w, fmt.Sprintf("failed to parse multipart form: %v. Ensure Content-Type is multipart/form-data with boundary", err), http.StatusBadRequest)
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
	jobID := uuid.New().String()

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
	job := hc.jobManager.CreateJob(jobID, header.Filename)
	hc.log.Info("created job", "job_id", jobID, "filename", header.Filename, "status", job.Status)

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

	// Process batches with context tracking
	transcriptContext := ""
	for i, batch := range batches {
		hc.log.Info("processing batch", "job_id", jobID, "batch_num", i+1, "batch_size", len(batch))

		literalText, err := hc.sendFrameBatchToDemoAPIWithResponse(batch)
		if err != nil {
			hc.log.Error("failed to send batch to demo API", "job_id", jobID, "batch_num", i+1, "error", err)
			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.FailedBatches++
				job.ProcessedBatches++
			})
		} else {
			// Trim context and update with OpenRouter
			trimmedContext := hc.trimContext(transcriptContext, 1000)
			updatedTranscript, err := hc.updateTranscriptWithContext(trimmedContext, literalText)
			if err != nil {
				hc.log.Warn("failed to update transcript, using literal", "job_id", jobID, "error", err)
				// Fallback to literal text
				updatedTranscript = strings.TrimSpace(trimmedContext + " " + literalText)
			}

			// Update running context
			transcriptContext = updatedTranscript

			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.SuccessfulBatches++
				job.ProcessedBatches++
			})
			if literalText != "" {
				hc.jobManager.AddTranscriptionResult(jobID, literalText)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Use the final improved context as full text
	hc.jobManager.CompleteJob(jobID, transcriptContext)

	job, _ := hc.jobManager.GetJob(jobID)
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
	
	// Debug logging
	urlParams := chi.RouteContext(r.Context()).URLParams
	hc.log.Info("getting job status", 
		"job_id", jobID, 
		"request_path", r.URL.Path,
		"request_url", r.URL.String(),
		"full_url", r.Host + r.RequestURI,
		"url_params_keys", urlParams.Keys,
		"url_params_values", urlParams.Values)

	job, exists := hc.jobManager.GetJob(jobID)
	if !exists {
		hc.log.Error("job not found", "job_id", jobID, "all_jobs_count", len(hc.jobManager.GetAllJobs()))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}

	hc.log.Info("job found", "job_id", jobID, "status", job.Status)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// ListJobs returns all jobs (for debugging)
func (hc *HandlersConfig) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := hc.jobManager.GetAllJobs()
	hc.log.Info("listing all jobs", "count", len(jobs))
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count": len(jobs),
		"jobs":  jobs,
	})
}

func (hc *HandlersConfig) sendFrameBatchToDemoAPIWithResponse(frames [][]byte) (string, error) {
	if hc.useMock {
		mockText := fmt.Sprintf("Mock transcription for batch of %d frames. This is test data.", len(frames))
		hc.log.Info("returning mock data", "text_length", len(mockText))
		return mockText, nil
	}

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

	var apiResp WebSocketMessage
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		hc.log.Warn("failed to decode demo API response", "error", err)
		return "", nil
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
