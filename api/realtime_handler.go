package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"streaming/config"
	"streaming/logger"
	"streaming/mlclient"
	"streaming/utils"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	frameWindowSize        = 32
	frameWindowStride      = 16
	frameBatchQueueSize    = 1
	stableLiteralQueueSize = 8
	maxContextRunes        = 1000
	maxWebSocketFrameSize  = 512 << 10
	webSocketWriteTimeout  = 5 * time.Second
	webSocketIdleTimeout   = 45 * time.Second
	maxConcurrentSockets   = 8
	maxSocketsPerIP        = 2
	maxFramesPerSecond     = 45
	maxBytesPerSecond      = 4 << 20
	maxProtocolViolations  = 3
	maxConcurrentMLCalls   = 1
)

type HandlersConfig struct {
	log            *logger.MultiLogger
	jobManager     *JobManager
	useMock        bool
	mlClient       *mlclient.Client
	useOpenRouter  bool
	jobSlots       chan struct{}
	uploadSlots    chan struct{}
	mlSlots        chan struct{}
	webSocketSlots chan struct{}
	webSocketMu    sync.Mutex
	webSocketsByIP map[string]int
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	mlAPIURL, err := config.GetEnv("ML_API_URL")
	if err != nil {
		log.Warn("ML_API_URL not set, using default", "error", err)
		mlAPIURL = "http://localhost:8085/process"
	}

	useMock := false
	if mockEnv, err := config.GetEnv("USE_MOCK"); err == nil {
		useMock = mockEnv == "true" || mockEnv == "1"
	}

	useOpenRouter := true
	if orEnv, err := config.GetEnv("USE_OPENROUTER"); err == nil {
		envVal := strings.ToLower(orEnv)
		useOpenRouter = envVal == "true" || envVal == "1" || envVal == "yes"
	}

	if useMock {
		log.Info("Mock mode enabled - will return test data")
	}

	if !useOpenRouter {
		log.Info("OpenRouter transcript improvement disabled via USE_OPENROUTER")
	}

	client, err := mlclient.NewClient(mlAPIURL)
	if err != nil {
		log.Fatal("failed to initialise ML API client", "error", err, "url", mlAPIURL)
	}

	jobManager := NewJobManager()
	go jobManager.RunCleanup(context.Background(), time.Hour, 24*time.Hour)

	return &HandlersConfig{
		log:            log,
		jobManager:     jobManager,
		useMock:        useMock,
		mlClient:       client,
		useOpenRouter:  useOpenRouter,
		jobSlots:       make(chan struct{}, maxConcurrentVideoJobs),
		uploadSlots:    make(chan struct{}, maxConcurrentUploads),
		mlSlots:        make(chan struct{}, maxConcurrentMLCalls),
		webSocketSlots: make(chan struct{}, maxConcurrentSockets),
		webSocketsByIP: make(map[string]int),
	}
}

type WebSocketMessage struct {
	Type       string  `json:"type"`
	Text       string  `json:"text"`
	FullText   string  `json:"full_text"`
	Confidence float64 `json:"confidence"`
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
// @Description   "type": "transcript",
// @Description   "text": "append-only delta",
// @Description   "full_text": "authoritative transcript snapshot",
// @Description   "confidence": 0.91
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
	clientIP := requestClientIP(r)
	if !hc.tryAcquireWebSocketSlot(clientIP) {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "websocket capacity exhausted", http.StatusServiceUnavailable)
		return
	}
	defer hc.releaseWebSocketSlot(clientIP)

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		hc.log.Error("failed to accept websocket", "error", err)
		return
	}

	c.SetReadLimit(maxWebSocketFrameSize)

	hc.log.Info("Client connected to video socket")
	defer func() {
		c.Close(websocket.StatusNormalClosure, "closing connection")
		hc.log.Info("Client disconnected from video socket")
	}()

	// The request context is not reliable after an HTTP connection is hijacked.
	// The session is instead cancelled when the WebSocket read loop exits.
	ctx, cancel := context.WithCancel(context.Background())
	batchQueue := make(chan [][]byte, frameBatchQueueSize)
	workerDone := make(chan struct{})

	go func() {
		defer close(workerDone)
		hc.processFrameBatches(ctx, c, batchQueue)
	}()

	hc.handleFrameStream(ctx, c, batchQueue)
	cancel()
	close(batchQueue)
	<-workerDone
}

func (hc *HandlersConfig) tryAcquireWebSocketSlot(clientIP string) bool {
	select {
	case hc.webSocketSlots <- struct{}{}:
	default:
		return false
	}

	hc.webSocketMu.Lock()
	defer hc.webSocketMu.Unlock()
	if hc.webSocketsByIP == nil {
		hc.webSocketsByIP = make(map[string]int)
	}
	if hc.webSocketsByIP[clientIP] >= maxSocketsPerIP {
		<-hc.webSocketSlots
		return false
	}
	hc.webSocketsByIP[clientIP]++
	return true
}

func (hc *HandlersConfig) releaseWebSocketSlot(clientIP string) {
	hc.webSocketMu.Lock()
	if hc.webSocketsByIP[clientIP] <= 1 {
		delete(hc.webSocketsByIP, clientIP)
	} else {
		hc.webSocketsByIP[clientIP]--
	}
	hc.webSocketMu.Unlock()
	<-hc.webSocketSlots
}

func requestClientIP(r *http.Request) string {
	if address, ok := parseRemoteIP(r.RemoteAddr); ok {
		return address.String()
	}
	return "unknown"
}

func (hc *HandlersConfig) handleFrameStream(ctx context.Context, c *websocket.Conn, batchQueue chan [][]byte) {
	framesBuffer := make([][]byte, 0, frameWindowSize)
	rateWindowStarted := time.Now()
	framesInWindow := 0
	bytesInWindow := 0
	protocolViolations := 0

	for {
		readCtx, cancelRead := context.WithTimeout(ctx, webSocketIdleTimeout)
		typ, data, err := c.Read(readCtx)
		cancelRead()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				hc.log.Info("closing idle websocket")
				_ = c.Close(websocket.StatusPolicyViolation, "idle timeout")
				return
			}
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
				hc.log.Debug("websocket closed by client", "status", status)
			} else {
				hc.log.Warn("error reading from websocket", "error", err)
			}
			return
		}

		if typ != websocket.MessageBinary {
			protocolViolations++
			if protocolViolations >= maxProtocolViolations {
				hc.log.Warn("closing websocket after repeated non-binary messages")
				_ = c.Close(websocket.StatusUnsupportedData, "binary video frames required")
				return
			}
			continue
		}

		if len(data) == 0 {
			continue
		}

		now := time.Now()
		if now.Sub(rateWindowStarted) >= time.Second {
			rateWindowStarted = now
			framesInWindow = 0
			bytesInWindow = 0
		}
		framesInWindow++
		bytesInWindow += len(data)
		if framesInWindow > maxFramesPerSecond || bytesInWindow > maxBytesPerSecond {
			hc.log.Warn("closing websocket that exceeded frame rate", "frames", framesInWindow, "bytes", bytesInWindow)
			_ = c.Close(websocket.StatusPolicyViolation, "video frame rate exceeded")
			return
		}

		framesBuffer = append(framesBuffer, data)
		if len(framesBuffer) >= frameWindowSize {
			framesToSend := make([][]byte, frameWindowSize)
			copy(framesToSend, framesBuffer[:frameWindowSize])
			framesBuffer = framesBuffer[frameWindowStride:]

			if offerLatestBatch(batchQueue, framesToSend) {
				hc.log.Debug("dropped stale frame batch")
			}
		}
	}
}

func offerLatestBatch(queue chan [][]byte, batch [][]byte) bool {
	select {
	case queue <- batch:
		return false
	default:
	}

	dropped := false
	select {
	case <-queue:
		dropped = true
	default:
	}

	select {
	case queue <- batch:
	default:
	}
	return dropped
}

func (hc *HandlersConfig) processFrameBatches(ctx context.Context, c *websocket.Conn, batchQueue <-chan [][]byte) {
	var stabilizer predictionStabilizer
	workerCtx, cancelWorker := context.WithCancel(ctx)
	literalQueue := make(chan mlclient.Prediction, stableLiteralQueueSize)
	transcriptDone := make(chan struct{})

	go func() {
		defer close(transcriptDone)
		hc.processStablePredictions(workerCtx, c, literalQueue)
		cancelWorker()
	}()

	defer func() {
		cancelWorker()
		close(literalQueue)
		<-transcriptDone
	}()

	for {
		select {
		case <-workerCtx.Done():
			return
		case frames, ok := <-batchQueue:
			if !ok {
				return
			}

			prediction, err := hc.requestPrediction(workerCtx, frames, "mock sign")
			if err != nil {
				stabilizer.OnError()
				hc.log.Error("failed to get prediction from ML API", "error", err)
				continue
			}

			stablePrediction, stable := stabilizer.Observe(prediction)
			if !stable {
				continue
			}

			select {
			case literalQueue <- stablePrediction:
			case <-workerCtx.Done():
				return
			}
		}
	}
}

// processStablePredictions keeps slow OpenRouter requests and WebSocket writes
// out of the ML inference loop. Stable literals are processed in order through
// a small bounded queue while the frame queue continues to discard stale video.
func (hc *HandlersConfig) processStablePredictions(ctx context.Context, c *websocket.Conn, literalQueue <-chan mlclient.Prediction) {
	fullTranscript := ""

	for {
		select {
		case <-ctx.Done():
			return
		case stablePrediction, ok := <-literalQueue:
			if !ok {
				return
			}

			updatedTranscript, delta, err := hc.updateTranscriptWithContext(ctx, fullTranscript, stablePrediction.Text)
			if err != nil {
				hc.log.Warn("failed to improve transcript, using literal text", "error", err)
				delta = strings.TrimSpace(stablePrediction.Text)
				updatedTranscript = combineTranscript(fullTranscript, delta)
			}
			if delta == "" {
				continue
			}

			fullTranscript = updatedTranscript
			response := WebSocketMessage{
				Type:       "transcript",
				Text:       delta,
				FullText:   fullTranscript,
				Confidence: stablePrediction.Confidence,
			}
			if err := hc.sendTextToClient(ctx, c, response); err != nil {
				hc.log.Warn("failed to send text to websocket client", "error", err)
				_ = c.Close(websocket.StatusInternalError, "failed to send transcript")
				return
			}

			hc.log.Info("sent stable transcript segment", "segment_length", len(delta), "confidence", stablePrediction.Confidence)
		}
	}
}

func (hc *HandlersConfig) updateTranscriptWithContext(ctx context.Context, fullTranscript, newLiteral string) (string, string, error) {
	fullTranscript = strings.TrimSpace(fullTranscript)
	chunk := strings.TrimSpace(newLiteral)

	if chunk == "" {
		return fullTranscript, "", nil
	}

	if hc.useMock {
		return combineTranscript(fullTranscript, chunk), chunk, nil
	}

	if !hc.useOpenRouter {
		return combineTranscript(fullTranscript, chunk), chunk, nil
	}

	contextWindow := trimContext(fullTranscript, maxContextRunes)
	update, err := utils.UpdateTranscript(ctx, contextWindow, chunk)
	if err != nil {
		return "", "", err
	}
	if update.Delta == "" {
		return fullTranscript, "", nil
	}

	return combineTranscript(fullTranscript, update.Delta), update.Delta, nil
}

func (hc *HandlersConfig) sendTextToClient(ctx context.Context, c *websocket.Conn, message WebSocketMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, webSocketWriteTimeout)
	defer cancel()
	return c.Write(writeCtx, websocket.MessageText, data)
}

func (hc *HandlersConfig) requestPrediction(ctx context.Context, frames [][]byte, mockText string) (mlclient.Prediction, error) {
	if len(frames) == 0 {
		return mlclient.Prediction{}, fmt.Errorf("no frames to send to ML API")
	}

	if hc.useMock {
		hc.log.Debug("using mock literal text")
		return mlclient.Prediction{Text: mockText, Confidence: 1, Accepted: true}, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if hc.mlClient == nil {
		return mlclient.Prediction{}, fmt.Errorf("ml client is not configured")
	}

	if !hc.acquireMLSlot(ctx) {
		return mlclient.Prediction{}, ctx.Err()
	}
	defer hc.releaseMLSlot()

	prediction, err := hc.mlClient.ProcessFrames(ctx, frames)
	if err != nil {
		return mlclient.Prediction{}, fmt.Errorf("call ml api: %w", err)
	}

	hc.log.Debug("received prediction from ML API", "accepted", prediction.Accepted, "confidence", prediction.Confidence)
	return prediction, nil
}

func (hc *HandlersConfig) acquireMLSlot(ctx context.Context) bool {
	if hc.mlSlots == nil {
		return true
	}
	select {
	case hc.mlSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (hc *HandlersConfig) releaseMLSlot() {
	if hc.mlSlots != nil {
		<-hc.mlSlots
	}
}
