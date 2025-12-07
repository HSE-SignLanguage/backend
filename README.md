# backend

A Go backend service for real-time video frame streaming and processing.

## Features

- **WebSocket Frame Handler**: Accepts video frames from frontend clients via WebSocket
- **Frame Batching**: Groups incoming frames into batches of 32 frames
- **Demo API Integration**: Automatically sends batched frames to a configurable demo API endpoint

## WebSocket API

### `/api/socket` - Video Frame WebSocket

Connect to this endpoint to stream video frames. The server expects binary messages containing individual frames.

**Behavior:**
- Accepts WebSocket connections with binary frame data
- Buffers frames until 32 frames are collected
- Sends batches of 32 frames to the demo API endpoint (configured via `DEMO_API_URL`)
- Continues to accept and batch frames concurrently

**Frame Format:**
- Message Type: Binary (`websocket.MessageBinary`)
- Content: Raw frame data (bytes)

**Demo API Request Format:**
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

## Running

```bash
go run main.go
```