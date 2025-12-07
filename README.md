# backend

A Go backend service for real-time video frame streaming and processing.

## Features

- **WebSocket Frame Handler**: Accepts video frames from frontend clients via WebSocket
- **Video Upload & Processing**: Upload video files for frame extraction and batch processing
- **Frame Batching**: Groups incoming frames into batches of 32 frames
- **Demo API Integration**: Automatically sends batched frames to a configurable demo API endpoint

## API Endpoints

### `/health` - Health Check

Simple health check endpoint to verify the service is running.

**Method:** GET  
**Response:** `200 OK` with body `"OK"`

### `/socket` - Video Frame WebSocket

Connect to this endpoint to stream video frames in real-time. The server expects binary messages containing individual frames.

**Method:** WebSocket (GET upgrade)

**Behavior:**
- Accepts WebSocket connections with binary frame data
- Buffers frames until 32 frames are collected
- Sends batches of 32 frames to the demo API endpoint (configured via `ML_API_URL`)
- Continues to accept and batch frames concurrently

**Frame Format:**
- Message Type: Binary (`websocket.MessageBinary`)
- Content: Raw frame data (bytes)

### `/upload` - Video Upload & Async Processing

Upload a video file for asynchronous frame-by-frame extraction and batch processing.

**Method:** POST  
**Content-Type:** `multipart/form-data`

**Parameters:**
- `video` (required): Video file to process
- `interval` (optional): Frame extraction interval (default: 1). Set to 2 to extract every 2nd frame, etc.

**Process:**
1. Upload video file (returns immediately with job ID)
2. Video is processed in the background:
   - Frames extracted using FFmpeg
   - Split into batches of 32
   - Each batch sent to demo API
   - Transcription results accumulated
3. Poll `/job/{id}` to check status and get results

**Immediate Response (202 Accepted):**
```json
{
  "job_id": "1733587200000000000-video.mp4",
  "status": "queued",
  "message": "Video upload accepted, processing started"
}
```

**Example Usage:**
```bash
# 1. Upload video
RESPONSE=$(curl -s -X POST http://localhost:8080/upload \
  -F "video=@/path/to/video.mp4" \
  -F "interval=1")

JOB_ID=$(echo $RESPONSE | jq -r '.job_id')

# 2. Poll for status
curl http://localhost:8080/job/$JOB_ID
```

### `/job/{id}` - Get Job Status

Poll this endpoint to check processing status and retrieve results.

**Method:** GET

**Response:**
```json
{
  "id": "1733587200000000000-video.mp4",
  "status": "completed",
  "filename": "video.mp4",
  "total_frames": 250,
  "total_batches": 8,
  "processed_batches": 8,
  "successful_batches": 8,
  "failed_batches": 0,
  "transcription": ["text from batch 1", "text from batch 2", ...],
  "full_text": "complete transcription of entire video",
  "video_info": {
    "fps": 25.0,
    "duration": 10.0,
    "frame_width": 1920,
    "frame_height": 1080,
    "estimated_frames": 250
  },
  "created_at": "2025-12-07T12:00:00Z",
  "updated_at": "2025-12-07T12:01:30Z",
  "completed_at": "2025-12-07T12:01:30Z"
}
```

**Status Values:**
- `queued`: Job created, waiting to start
- `processing`: Currently extracting and processing frames
- `completed`: All processing done, transcription available in `full_text`
- `failed`: Processing error (see `error` field)

## Demo API Request Format

Both endpoints send frames to the demo API in the following format:

```json
{
  "frames": ["<base64_frame_1>", "<base64_frame_2>", ..., "<base64_frame_32>"],
  "count": 32
}
```

**Expected Response:**
```json
{
  "text": "extracted or transcribed text from frames"
}
```

## Configuration

Set the following environment variables in `.env`:

```env
BACKEND_PORT=8080              # Port for the backend server
ML_API_URL=http://localhost:9000/api/process  # Demo API endpoint for frame processing
USE_MOCK=false                 # Enable mock mode (returns test data without calling demo API)
OPENROUTER_API_KEY=your_key    # OpenRouter API key for transcript improvement
OPENROUTER_MODEL=anthropic/claude-3.5-sonnet  # Model to use for transcript processing
OPENROUTER_SYSTEM_PROMPT="You turn literal sign-language transcripts into natural text..."  # System prompt sent to OpenRouter
USE_OPENROUTER=true            # Set to false to skip OpenRouter transcript polishing
```

### Transcript Processing

The system uses a two-stage approach for transcription:

1. **Raw Literal Text**: Demo API returns raw literal transcription from sign language frames
2. **Improved Transcript**: OpenRouter AI improves the literal text by:
   - Fixing grammar and punctuation
   - Making text more natural and readable
   - Maintaining context across batches (up to 1000 chars)
   - Preserving the original language

Set `USE_OPENROUTER=false` if you only need the literal output (the backend will concatenate batches without calling OpenRouter).

Customize `OPENROUTER_SYSTEM_PROMPT` to control how the polishing model behaves (language, tone, formatting rules, etc.).

**Context Management:**
- Keeps running context of up to 1000 characters
- Automatically trims older context when limit is reached
- Each new batch is processed with recent context for better coherence

### Mock Mode

Set `USE_MOCK=true` to enable mock/test mode. When enabled:
- WebSocket endpoint returns mock transcription data without calling the demo API
- Video upload endpoint returns test data for each batch
- Useful for frontend development and testing without a real processing backend
- Can be enabled by setting env var to `true` or `1`

**Example with mock mode:**
```bash
# In .env
USE_MOCK=true

# Or run with environment variable
USE_MOCK=true go run main.go
```

## Prerequisites

The video upload endpoint requires FFmpeg to be installed on your system for frame extraction:

**macOS:**
```bash
brew install ffmpeg
```

**Ubuntu/Debian:**
```bash
sudo apt-get install ffmpeg
```

**Windows:**
Download from [ffmpeg.org](https://ffmpeg.org/download.html) or use `choco install ffmpeg`

## Running

```bash
go run main.go
```

## API Documentation

Swagger documentation is available at:

**Swagger UI:** `http://localhost:8080/swagger/index.html`

The API documentation includes:
- Health check endpoint
- WebSocket connection endpoint
- Request/response schemas
- Interactive API testing

To regenerate Swagger docs after making changes to annotations:

```bash
swag init -g main.go --output ./docs
```
