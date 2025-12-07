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
- Sends batches of 32 frames to the demo API endpoint (configured via `DEMO_API_URL`)
- Continues to accept and batch frames concurrently

**Frame Format:**
- Message Type: Binary (`websocket.MessageBinary`)
- Content: Raw frame data (bytes)

### `/upload` - Video Upload & Processing

Upload a video file for frame-by-frame extraction and batch processing.

**Method:** POST  
**Content-Type:** `multipart/form-data`

**Parameters:**
- `video` (required): Video file to process
- `interval` (optional): Frame extraction interval (default: 1). Set to 2 to extract every 2nd frame, etc.

**Process:**
1. Accepts video file upload (supports common formats: MP4, AVI, MOV, etc.)
2. Extracts frames from the video using FFmpeg
3. Splits frames into batches of 32
4. Sends each batch sequentially to the demo API endpoint
5. Returns processing summary

**Response:**
```json
{
  "status": "completed",
  "total_frames": 250,
  "total_batches": 8,
  "successful_batches": 8,
  "video_info": {
    "fps": 25.0,
    "duration": 10.0,
    "frame_width": 1920,
    "frame_height": 1080,
    "estimated_frames": 250
  }
}
```

**Example Usage:**
```bash
curl -X POST http://localhost:8080/upload \
  -F "video=@/path/to/video.mp4" \
  -F "interval=1"
```

## Demo API Request Format

Both endpoints send frames to the demo API in the following format:

```json
{
  "frames": ["<base64_frame_1>", "<base64_frame_2>", ..., "<base64_frame_32>"],
  "count": 32
}
```

## Configuration

Set the following environment variables in `.env`:

```env
BACKEND_PORT=8080              # Port for the backend server
DEMO_API_URL=http://localhost:9000/api/process  # Demo API endpoint for frame processing
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