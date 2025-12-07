package api

import (
	"sync"
	"time"
)

// JobStatus represents the status of a video processing job
type JobStatus string

const (
	JobStatusQueued     JobStatus = "queued"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// VideoJob represents a video processing job
type VideoJob struct {
	ID                string                 `json:"id"`
	Status            JobStatus              `json:"status"`
	Filename          string                 `json:"filename"`
	TotalFrames       int                    `json:"total_frames"`
	TotalBatches      int                    `json:"total_batches"`
	ProcessedBatches  int                    `json:"processed_batches"`
	SuccessfulBatches int                    `json:"successful_batches"`
	FailedBatches     int                    `json:"failed_batches"`
	Transcription     []string               `json:"transcription"` // Accumulated text results
	FullText          string                 `json:"full_text"`     // Combined transcription
	VideoInfo         map[string]interface{} `json:"video_info"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
	CompletedAt       *time.Time             `json:"completed_at,omitempty"`
	Error             string                 `json:"error,omitempty"`
}

// JobManager manages video processing jobs
type JobManager struct {
	jobs map[string]*VideoJob
	mu   sync.RWMutex
}

// NewJobManager creates a new job manager
func NewJobManager() *JobManager {
	return &JobManager{
		jobs: make(map[string]*VideoJob),
	}
}

// CreateJob creates a new video processing job
func (jm *JobManager) CreateJob(id, filename string) *VideoJob {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job := &VideoJob{
		ID:            id,
		Status:        JobStatusQueued,
		Filename:      filename,
		Transcription: make([]string, 0),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	jm.jobs[id] = job
	return job
}

// GetJob retrieves a job by ID
func (jm *JobManager) GetJob(id string) (*VideoJob, bool) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	job, exists := jm.jobs[id]
	return job, exists
}

// UpdateJob updates a job's status and information
func (jm *JobManager) UpdateJob(id string, updateFn func(*VideoJob)) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, exists := jm.jobs[id]
	if !exists {
		return nil
	}

	updateFn(job)
	job.UpdatedAt = time.Now()
	return nil
}

// AddTranscriptionResult adds a transcription result to the job
func (jm *JobManager) AddTranscriptionResult(id string, text string) {
	jm.UpdateJob(id, func(job *VideoJob) {
		if text != "" {
			job.Transcription = append(job.Transcription, text)
		}
	})
}

// CompleteJob marks a job as completed
func (jm *JobManager) CompleteJob(id string, fullText string) {
	jm.UpdateJob(id, func(job *VideoJob) {
		job.Status = JobStatusCompleted
		job.FullText = fullText
		now := time.Now()
		job.CompletedAt = &now
	})
}

// FailJob marks a job as failed
func (jm *JobManager) FailJob(id string, errorMsg string) {
	jm.UpdateJob(id, func(job *VideoJob) {
		job.Status = JobStatusFailed
		job.Error = errorMsg
		now := time.Now()
		job.CompletedAt = &now
	})
}

// CleanupOldJobs removes jobs older than the specified duration
func (jm *JobManager) CleanupOldJobs(maxAge time.Duration) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for id, job := range jm.jobs {
		if job.CompletedAt != nil && job.CompletedAt.Before(cutoff) {
			delete(jm.jobs, id)
		}
	}
}

// GetAllJobs returns all jobs (for debugging)
func (jm *JobManager) GetAllJobs() map[string]*VideoJob {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	jobs := make(map[string]*VideoJob, len(jm.jobs))
	for id, job := range jm.jobs {
		jobs[id] = job
	}
	return jobs
}
