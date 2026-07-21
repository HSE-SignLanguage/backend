package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"streaming/utils"
	"strings"
	"time"
	"unicode"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	maxUploadFileBytes     int64 = 100 << 20
	maxUploadBodyBytes           = maxUploadFileBytes + (1 << 20)
	multipartMemoryBytes         = 8 << 20
	maxConcurrentUploads         = 2
	maxConcurrentVideoJobs       = 1
	maxFrameInterval             = 120
	maxUploadReadTime            = 3 * time.Minute
	maxJobProcessingTime         = 15 * time.Minute
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
// @Param interval formData int false "Minimum frame extraction interval (default: 1; automatically increased to keep extraction bounded)"
// @Success 202 {object} map[string]interface{} "Job created and processing started"
// @Failure 400 {object} map[string]string "Bad Request - Invalid file or format"
// @Failure 413 {object} map[string]string "Video exceeds 100 MiB"
// @Failure 415 {object} map[string]string "Unsupported or invalid video"
// @Failure 429 {object} map[string]string "Video processor is busy"
// @Failure 503 {object} map[string]string "Upload capacity exhausted"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /upload [post]
func (hc *HandlersConfig) VideoUploadHandler(w http.ResponseWriter, r *http.Request) {
	if !hc.tryAcquireUploadSlot() {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "upload capacity exhausted, retry later", http.StatusServiceUnavailable)
		return
	}
	defer hc.releaseUploadSlot()

	responseController := http.NewResponseController(w)
	if err := responseController.SetReadDeadline(time.Now().Add(maxUploadReadTime)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		hc.log.Warn("failed to set upload read deadline", "error", err)
	}
	defer responseController.SetReadDeadline(time.Time{})

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBodyBytes)
	if err := r.ParseMultipartForm(multipartMemoryBytes); err != nil {
		hc.log.Error("failed to parse multipart form", "error", err, "content_type", r.Header.Get("Content-Type"))
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "video upload exceeds 100 MiB", http.StatusRequestEntityTooLarge)
			return
		}
		if os.IsTimeout(err) {
			http.Error(w, "video upload timed out", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "invalid multipart video upload", http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, header, err := r.FormFile("video")
	if err != nil {
		hc.log.Error("failed to get video file", "error", err)
		http.Error(w, "video file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()
	if header.Size > maxUploadFileBytes {
		http.Error(w, "video upload exceeds 100 MiB", http.StatusRequestEntityTooLarge)
		return
	}

	filename := sanitizeFilename(header.Filename)

	hc.log.Info("received video upload", "filename", filename, "size", header.Size)

	interval := 1
	if intervalStr := r.FormValue("interval"); intervalStr != "" {
		parsedInterval, err := strconv.Atoi(intervalStr)
		if err != nil || parsedInterval < 1 || parsedInterval > maxFrameInterval {
			http.Error(w, "interval must be an integer between 1 and 120", http.StatusBadRequest)
			return
		}
		interval = parsedInterval
	}

	tempDir := filepath.Join("tmp", "uploads")
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		hc.log.Error("failed to create temp directory", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Generate job ID
	jobID := uuid.New().String()

	// Save file with job ID
	tempFilePath := filepath.Join(tempDir, jobID+".upload")
	tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		hc.log.Error("failed to create temp file", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	written, copyErr := io.Copy(tempFile, io.LimitReader(file, maxUploadFileBytes+1))
	closeErr := tempFile.Close()
	if copyErr != nil || closeErr != nil {
		os.Remove(tempFilePath)
		hc.log.Error("failed to save temp file", "copy_error", copyErr, "close_error", closeErr)
		http.Error(w, "failed to save video", http.StatusInternalServerError)
		return
	}
	if written > maxUploadFileBytes {
		os.Remove(tempFilePath)
		http.Error(w, "video upload exceeds 100 MiB", http.StatusRequestEntityTooLarge)
		return
	}

	// ffprobe is the source of truth for supported containers and video
	// streams. MIME sniffing rejects valid MOV files and can accept a forged
	// signature without proving the payload is actually decodable.
	extractor, err := utils.NewVideoFrameExtractor(tempFilePath)
	if err != nil {
		os.Remove(tempFilePath)
		hc.log.Warn("rejected uploaded video", "filename", filename, "error", err)
		http.Error(w, "unsupported or invalid video", http.StatusUnsupportedMediaType)
		return
	}

	if !hc.tryAcquireJobSlot() {
		extractor.Close()
		os.Remove(tempFilePath)
		w.Header().Set("Retry-After", "5")
		http.Error(w, "video processor is busy, retry later", http.StatusTooManyRequests)
		return
	}
	slotTransferred := false
	defer func() {
		if !slotTransferred {
			hc.releaseJobSlot()
		}
	}()

	// Create job
	job := hc.jobManager.CreateJob(jobID, filename)
	hc.log.Info("created job", "job_id", jobID, "filename", filename, "status", job.Status)

	// Start async processing
	slotTransferred = true
	go hc.processVideoAsync(jobID, tempFilePath, interval, extractor)

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

func (hc *HandlersConfig) tryAcquireJobSlot() bool {
	select {
	case hc.jobSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (hc *HandlersConfig) releaseJobSlot() {
	<-hc.jobSlots
}

func (hc *HandlersConfig) tryAcquireUploadSlot() bool {
	select {
	case hc.uploadSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (hc *HandlersConfig) releaseUploadSlot() {
	<-hc.uploadSlots
}

func sanitizeFilename(filename string) string {
	filename = filepath.Base(strings.ReplaceAll(filename, "\\", "/"))
	var sanitized strings.Builder
	for _, r := range filename {
		if sanitized.Len() >= 128 {
			break
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '_' {
			sanitized.WriteRune(r)
		} else {
			sanitized.WriteByte('_')
		}
	}

	result := strings.Trim(sanitized.String(), ".")
	if result == "" {
		return "video"
	}
	return result
}

// processVideoAsync processes video in the background
func (hc *HandlersConfig) processVideoAsync(jobID, tempFilePath string, requestedInterval int, extractor *utils.VideoFrameExtractor) {
	defer os.Remove(tempFilePath) // Clean up temp file when done
	defer hc.releaseJobSlot()
	defer extractor.Close()

	ctx, cancel := context.WithTimeout(context.Background(), maxJobProcessingTime)
	defer cancel()

	hc.log.Info("starting video processing", "job_id", jobID)

	// Update job status to processing
	hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
		job.Status = JobStatusProcessing
	})

	// Get video info
	videoInfo := extractor.GetVideoInfo()
	interval := extractor.EffectiveFrameInterval(requestedInterval)
	videoInfo["extraction_interval"] = interval
	hc.log.Info("video info", "job_id", jobID, "info", videoInfo)

	// Automatically increase the sampling interval for longer/high-FPS videos
	// so the documented two-minute input limit does not exceed the bounded
	// extraction budget.
	frames, err := extractor.ExtractFramesWithInterval(interval)

	if err != nil {
		hc.log.Error("failed to extract frames", "job_id", jobID, "error", err)
		hc.jobManager.FailJob(jobID, fmt.Sprintf("failed to extract frames: %v", err))
		return
	}

	hc.log.Info("extracted frames", "job_id", jobID, "count", len(frames))
	if len(frames) == 0 {
		hc.jobManager.FailJob(jobID, "video contains no decodable frames")
		return
	}

	// Use the same overlapping 32/16 windows as realtime recognition. The
	// final partial window is padded with its last frame.
	batches := utils.WindowFrames(frames, frameWindowSize, frameWindowStride)

	// Update job with batch info
	hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
		job.TotalFrames = len(frames)
		job.TotalBatches = len(batches)
		job.VideoInfo = videoInfo
	})

	hc.log.Info("created batches", "job_id", jobID, "batch_count", len(batches))

	// Process batches with context tracking
	transcriptContext := ""
	successfulInferences := 0
	var stabilizer predictionStabilizer
	for i, batch := range batches {
		if err := ctx.Err(); err != nil {
			hc.jobManager.FailJob(jobID, "video processing timed out")
			return
		}

		hc.log.Info("processing batch", "job_id", jobID, "batch_num", i+1, "batch_size", len(batch))

		mockText := fmt.Sprintf("Mock transcription for batch of %d frames. This is test data.", len(batch))
		prediction, err := hc.requestPrediction(ctx, batch, mockText)
		if err != nil {
			stabilizer.OnError()
			hc.log.Error("failed to send batch to demo API", "job_id", jobID, "batch_num", i+1, "error", err)
			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.FailedBatches++
				job.ProcessedBatches++
			})
		} else {
			successfulInferences++
			stablePrediction := prediction
			stable := prediction.Accepted && strings.TrimSpace(prediction.Text) != "" && !shouldSkipLiteral(prediction.Text)
			if len(batches) > 1 {
				stablePrediction, stable = stabilizer.Observe(prediction)
			}
			if !stable {
				hc.log.Debug("ML prediction not stable for batch", "job_id", jobID, "batch_num", i+1)
				hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
					job.SuccessfulBatches++
					job.ProcessedBatches++
				})
				continue
			}
			literalText, err := utils.NormalizeTranscriptTokenText(stablePrediction.Text)
			if err != nil {
				hc.log.Warn("discarded invalid ML transcript token", "job_id", jobID, "batch_num", i+1, "error", err)
				hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
					job.FailedBatches++
					job.ProcessedBatches++
				})
				continue
			}

			updatedTranscript, newSegment, err := hc.updateTranscriptWithContext(ctx, transcriptContext, literalText)
			if err != nil {
				hc.log.Warn("failed to update transcript, using literal", "job_id", jobID, "error", err)
				newSegment = strings.TrimSpace(literalText)
				updatedTranscript = combineTranscript(transcriptContext, newSegment)
			}

			if strings.TrimSpace(newSegment) == "" {
				hc.log.Info("no new transcript segment generated", "job_id", jobID, "batch_num", i+1)
				hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
					job.SuccessfulBatches++
					job.ProcessedBatches++
				})
				continue
			}

			// Update running context
			transcriptContext = updatedTranscript

			hc.jobManager.UpdateJob(jobID, func(job *VideoJob) {
				job.SuccessfulBatches++
				job.ProcessedBatches++
			})
			hc.jobManager.AddTranscriptionResult(jobID, newSegment)
		}

	}
	if err := ctx.Err(); err != nil {
		hc.jobManager.FailJob(jobID, "video processing timed out")
		return
	}

	if successfulInferences == 0 && len(batches) > 0 {
		hc.jobManager.FailJob(jobID, "recognition service was unavailable")
		return
	}

	// Use the final improved context as full text. Empty text is valid when the
	// ML service ran successfully but found no confident signs.
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

	job, exists := hc.jobManager.GetJob(jobID)
	if !exists {
		hc.log.Debug("job not found", "job_id", jobID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}

	hc.log.Debug("job found", "job_id", jobID, "status", job.Status)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}
