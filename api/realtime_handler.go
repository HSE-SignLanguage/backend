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
	lifecycle      handlerLifecycle
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
	handlers := &HandlersConfig{
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
	go jobManager.RunCleanup(handlers.lifecycle.context(), time.Hour, 24*time.Hour)
	return handlers
}

type WebSocketMessage struct {
	Type          string  `json:"type"`
	Text          string  `json:"text"`
	FullText      string  `json:"full_text"`
	LiteralText   string  `json:"literal_text"`
	FinalText     string  `json:"final_text"`
	DraftText     string  `json:"draft_text"`
	Confidence    float64 `json:"confidence"`
	Status        string  `json:"status,omitempty"`
	Enhanced      *bool   `json:"enhanced,omitempty"`
	Sequence      uint64  `json:"sequence,omitempty"`
	SegmentID     uint64  `json:"segment_id,omitempty"`
	FirstSequence uint64  `json:"first_sequence,omitempty"`
	LastSequence  uint64  `json:"last_sequence,omitempty"`
	TokenCount    int     `json:"token_count,omitempty"`
	Truncated     bool    `json:"truncated,omitempty"`
}

// HealthCheck godoc
// @Summary Health check endpoint
// @Description Check if the API is running and healthy
// @Tags health
// @Produce plain
// @Success 200 {string} string "OK"
// @Failure 503 {string} string "Server is draining"
// @Router /health [get]
func (hc *HandlersConfig) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if hc.lifecycle.isDraining() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "SHUTTING_DOWN", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// VideoSocketHandler godoc
// @Summary WebSocket endpoint for video frame streaming
// @Description Establishes a WebSocket connection for receiving video frames. Send binary frames to the server, and receive text responses back.
// @Description
// @Description **Client Flow:**
// @Description 1. Connect to same-origin `/api/socket` using `wss:` on HTTPS pages and `ws:` otherwise
// @Description 2. Send video frames as binary messages (MessageBinary)
// @Description 3. Server buffers frames and sends batches of 32 to processing API
// @Description 4. Receive ordered gesture, formatting, and transcript events as JSON text messages
// @Description
// @Description **Two-layer transcript:**
// @Description Every stable gesture is emitted immediately as type=gesture with status=draft. Stable gestures are grouped after 3 seconds of idle time or 6 tokens. A type=formatting event marks a segment being processed. The final type=transcript event replaces that segment's raw draft with enhanced or literal text. OpenRouter never blocks newer gesture events.
// @Description
// @Description **Gesture event:**
// @Description ```json
// @Description {
// @Description   "type": "gesture", "status": "draft", "text": "работать",
// @Description   "final_text": "", "draft_text": "я работать", "full_text": "я работать", "literal_text": "я работать",
// @Description   "confidence": 0.91, "sequence": 2, "segment_id": 1
// @Description }
// @Description ```
// @Description
// @Description **Final segment event:**
// @Description ```json
// @Description {
// @Description   "type": "transcript", "status": "enhanced", "enhanced": true,
// @Description   "text": "Я работаю.", "final_text": "Я работаю.", "draft_text": "",
// @Description   "full_text": "Я работаю.", "literal_text": "я работать", "sequence": 2, "segment_id": 1,
// @Description   "first_sequence": 1, "last_sequence": 2, "token_count": 2,
// @Description   "confidence": 0.91
// @Description }
// @Description ```
// @Description full_text is the authoritative rendered snapshot and equals finalized presentation text followed by all raw draft tokens. literal_text independently preserves recognizer-only output. text and confidence remain for compatibility. Sequences and segment IDs are monotonic per connection. truncated=true means the bounded session discarded its oldest finalized prefix.
// @Description
// @Description **Frontend Example:**
// @Description ```javascript
// @Description const wsURL = new URL('/api/socket', window.location.origin);
// @Description wsURL.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
// @Description const ws = new WebSocket(wsURL);
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
// @Success 101 {object} api.WebSocketMessage "Ordered two-layer transcript event"
// @Failure 400 {string} string "Bad Request - Failed to accept websocket"
// @Failure 503 {string} string "WebSocket capacity exhausted or server draining"
// @Router /socket [get]
func (hc *HandlersConfig) VideoSocketHandler(w http.ResponseWriter, r *http.Request) {
	if !hc.lifecycle.tryBeginWork() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer hc.lifecycle.endWork()

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

	// The request context is not reliable after an HTTP connection is hijacked.
	// A dedicated lifecycle context lets server shutdown cancel upgraded
	// connections, which net/http cannot drain on its own.
	ctx, cancel := context.WithCancel(hc.lifecycle.context())
	stopSession := func() {
		_ = c.Close(websocket.StatusGoingAway, "server shutting down")
		cancel()
	}
	sessionID := hc.lifecycle.registerWebSocket(stopSession)
	hc.log.Info("Client connected to video socket")
	defer func() {
		hc.lifecycle.unregisterWebSocket(sessionID)
		cancel()
		status := websocket.StatusNormalClosure
		reason := "closing connection"
		if hc.lifecycle.isDraining() {
			status = websocket.StatusGoingAway
			reason = "server shutting down"
		}
		_ = c.Close(status, reason)
		hc.log.Info("Client disconnected from video socket")
	}()

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
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				hc.log.Debug("websocket session cancelled")
				return
			}
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

// processStablePredictions owns the sole WebSocket writer for transcript
// events. OpenRouter runs asynchronously and never blocks raw gesture updates.
func (hc *HandlersConfig) processStablePredictions(ctx context.Context, c *websocket.Conn, literalQueue <-chan mlclient.Prediction) {
	formatter := func(formatCtx context.Context, priorContext string, tokens []utils.TranscriptSegmentToken) (string, bool, error) {
		if hc.useMock || !hc.useOpenRouter {
			return utils.LiteralSegmentText(tokens), false, nil
		}
		started := time.Now()
		segment, err := utils.FormatTranscriptSegment(formatCtx, priorContext, tokens)
		firstSequence := tokens[0].Sequence
		lastSequence := tokens[len(tokens)-1].Sequence
		hc.log.Debug(
			"OpenRouter segment formatting completed",
			"duration_ms", time.Since(started).Milliseconds(),
			"token_count", len(tokens),
			"first_sequence", firstSequence,
			"last_sequence", lastSequence,
			"success", err == nil,
		)
		if err != nil {
			return "", false, err
		}
		return segment.SegmentText, true, nil
	}

	sender := func(sendCtx context.Context, message WebSocketMessage) error {
		return hc.sendTextToClient(sendCtx, c, message)
	}
	assembler := newLiveTranscriptAssembler(
		liveSegmentIdleTimeout,
		utils.MaxTranscriptSegmentTokens,
		formatter,
		sender,
		hc.log,
	)
	if err := assembler.Run(ctx, literalQueue); err != nil && !errors.Is(err, context.Canceled) {
		hc.log.Warn("live transcript session stopped", "error", err)
		_ = c.Close(websocket.StatusInternalError, "failed to send transcript")
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
	updatedTranscript, delta := appendTranscriptDelta(fullTranscript, update.Delta)
	return updatedTranscript, delta, nil
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

	started := time.Now()
	prediction, err := hc.mlClient.ProcessFrames(ctx, frames)
	hc.log.Debug(
		"ML frame processing completed",
		"duration_ms", time.Since(started).Milliseconds(),
		"frame_count", len(frames),
		"success", err == nil,
	)
	if err != nil {
		return mlclient.Prediction{}, fmt.Errorf("call ml api: %w", err)
	}

	hc.log.Debug(
		"received prediction from ML API",
		"accepted", prediction.Accepted,
		"confidence", prediction.Confidence,
		"reason", prediction.Reason,
	)
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
