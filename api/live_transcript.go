package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"streaming/logger"
	"streaming/mlclient"
	"streaming/utils"
)

const (
	liveSegmentIdleTimeout    = 3 * time.Second
	maxAssemblerPendingTokens = 24
	maxLiveFinalRunes         = 16_000
)

type transcriptMessageSender func(context.Context, WebSocketMessage) error

type transcriptSegmentFormatter func(context.Context, string, []utils.TranscriptSegmentToken) (string, bool, error)

type liveTranscriptSegment struct {
	ID     uint64
	Tokens []utils.TranscriptSegmentToken
}

type liveFormatResult struct {
	Segment  liveTranscriptSegment
	Text     string
	Enhanced bool
	Err      error
}

// liveTranscriptAssembler owns all mutable transcript state and is the only
// component allowed to invoke send. The formatter runs in at most one worker;
// its result is applied by this event loop in segment order.
type liveTranscriptAssembler struct {
	idleTimeout time.Duration
	maxTokens   int
	formatter   transcriptSegmentFormatter
	send        transcriptMessageSender
	log         *logger.MultiLogger

	finalText        string
	literalFinalText string
	truncated        bool
	inFlight         *liveTranscriptSegment
	ready            []liveTranscriptSegment
	current          *liveTranscriptSegment
	nextSequence     uint64
	nextSegmentID    uint64
}

func newLiveTranscriptAssembler(
	idleTimeout time.Duration,
	maxTokens int,
	formatter transcriptSegmentFormatter,
	send transcriptMessageSender,
	log *logger.MultiLogger,
) *liveTranscriptAssembler {
	if idleTimeout <= 0 {
		idleTimeout = liveSegmentIdleTimeout
	}
	if maxTokens <= 0 || maxTokens > utils.MaxTranscriptSegmentTokens {
		maxTokens = utils.MaxTranscriptSegmentTokens
	}
	return &liveTranscriptAssembler{
		idleTimeout:   idleTimeout,
		maxTokens:     maxTokens,
		formatter:     formatter,
		send:          send,
		log:           log,
		nextSequence:  1,
		nextSegmentID: 1,
	}
}

func (a *liveTranscriptAssembler) Run(ctx context.Context, predictions <-chan mlclient.Prediction) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if a.formatter == nil {
		return errors.New("live transcript formatter is not configured")
	}
	if a.send == nil {
		return errors.New("live transcript sender is not configured")
	}

	results := make(chan liveFormatResult, 1)
	var idleTimer *time.Timer
	var idle <-chan time.Time

	stopIdle := func() {
		idle = nil
		if idleTimer == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
	}
	defer stopIdle()

	resetIdle := func() {
		stopIdle()
		if idleTimer == nil {
			idleTimer = time.NewTimer(a.idleTimeout)
		} else {
			idleTimer.Reset(a.idleTimeout)
		}
		idle = idleTimer.C
	}

	moveCurrentToReady := func() {
		if a.current == nil || len(a.current.Tokens) == 0 {
			return
		}
		a.ready = append(a.ready, cloneLiveSegment(*a.current))
		a.current = nil
		stopIdle()
	}

	launchNext := func() error {
		if a.inFlight != nil || len(a.ready) == 0 {
			return nil
		}

		segment := cloneLiveSegment(a.ready[0])
		a.ready = a.ready[1:]
		a.inFlight = &segment

		formatting := a.messageForSegment("formatting", "formatting", segment, "", false)
		if err := a.send(ctx, formatting); err != nil {
			return err
		}

		priorContext := trimContext(a.finalText, maxContextRunes)
		go func() {
			text, enhanced, err := a.formatter(ctx, priorContext, cloneTokens(segment.Tokens))
			results <- liveFormatResult{Segment: segment, Text: text, Enhanced: enhanced, Err: err}
		}()
		return nil
	}

	for {
		if predictions == nil && a.inFlight == nil && len(a.ready) == 0 && a.current == nil {
			return nil
		}
		activePredictions := predictions
		if a.pendingTokenCount() >= maxAssemblerPendingTokens {
			// Backpressure the stabilizer while formatting is slower than ML.
			// Its separate 8-token bounded ingress queue allows a total of at
			// most 32 stable pending tokens before inference stops and stale
			// camera batches start replacing queued work.
			activePredictions = nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case prediction, ok := <-activePredictions:
			if !ok {
				predictions = nil
				moveCurrentToReady()
				if err := launchNext(); err != nil {
					return err
				}
				continue
			}

			literal, err := utils.NormalizeTranscriptTokenText(prediction.Text)
			if err != nil {
				if a.log != nil {
					a.log.Debug("discarded invalid ML transcript token", "error", err)
				}
				continue
			}
			if a.current == nil {
				a.current = &liveTranscriptSegment{ID: a.nextSegmentID}
				a.nextSegmentID++
			}

			token := utils.TranscriptSegmentToken{
				Text:       literal,
				Confidence: prediction.Confidence,
				Sequence:   a.nextSequence,
			}
			a.nextSequence++
			a.current.Tokens = append(a.current.Tokens, token)

			gesture := a.messageForToken("gesture", "draft", a.current.ID, token)
			if err := a.send(ctx, gesture); err != nil {
				return err
			}

			if len(a.current.Tokens) >= a.maxTokens {
				moveCurrentToReady()
				if err := launchNext(); err != nil {
					return err
				}
			} else {
				resetIdle()
			}

		case <-idle:
			moveCurrentToReady()
			if err := launchNext(); err != nil {
				return err
			}

		case result := <-results:
			if a.inFlight == nil || a.inFlight.ID != result.Segment.ID {
				return errors.New("received out-of-order live transcript formatter result")
			}

			segmentText := strings.TrimSpace(result.Text)
			enhanced := result.Enhanced && result.Err == nil && segmentText != ""
			if !enhanced {
				segmentText = utils.LiteralSegmentText(result.Segment.Tokens)
				if a.log != nil && result.Err != nil {
					a.log.Warn(
						"failed to format live transcript segment, using literal text",
						"error", result.Err,
						"segment_id", result.Segment.ID,
					)
				}
			}

			literalSegment := utils.LiteralSegmentText(result.Segment.Tokens)
			var formattedTruncated, literalTruncated bool
			a.finalText, formattedTruncated = appendBoundedTranscript(a.finalText, segmentText, maxLiveFinalRunes)
			a.literalFinalText, literalTruncated = appendBoundedTranscript(a.literalFinalText, literalSegment, maxLiveFinalRunes)
			a.truncated = a.truncated || formattedTruncated || literalTruncated
			a.inFlight = nil
			transcript := a.messageForSegment("transcript", statusForEnhancement(enhanced), result.Segment, segmentText, enhanced)
			if err := a.send(ctx, transcript); err != nil {
				return err
			}
			if err := launchNext(); err != nil {
				return err
			}
		}
	}
}

func (a *liveTranscriptAssembler) messageForToken(messageType, status string, segmentID uint64, token utils.TranscriptSegmentToken) WebSocketMessage {
	finalText, draftText, fullText, literalText := a.snapshots()
	return WebSocketMessage{
		Type:          messageType,
		Text:          token.Text,
		FullText:      fullText,
		LiteralText:   literalText,
		FinalText:     finalText,
		DraftText:     draftText,
		Confidence:    token.Confidence,
		Status:        status,
		Sequence:      token.Sequence,
		SegmentID:     segmentID,
		FirstSequence: token.Sequence,
		LastSequence:  token.Sequence,
		TokenCount:    1,
		Truncated:     a.truncated,
	}
}

func (a *liveTranscriptAssembler) messageForSegment(messageType, status string, segment liveTranscriptSegment, text string, enhanced bool) WebSocketMessage {
	finalText, draftText, fullText, literalText := a.snapshots()
	first, last := segmentSequences(segment)
	var enhancedValue *bool
	if messageType == "transcript" {
		enhancedValue = &enhanced
	}
	return WebSocketMessage{
		Type:          messageType,
		Text:          text,
		FullText:      fullText,
		LiteralText:   literalText,
		FinalText:     finalText,
		DraftText:     draftText,
		Confidence:    segmentConfidence(segment),
		Status:        status,
		Enhanced:      enhancedValue,
		Sequence:      last,
		SegmentID:     segment.ID,
		FirstSequence: first,
		LastSequence:  last,
		TokenCount:    len(segment.Tokens),
		Truncated:     a.truncated,
	}
}

func (a *liveTranscriptAssembler) snapshots() (string, string, string, string) {
	draftParts := make([]string, 0, 2+len(a.ready))
	if a.inFlight != nil {
		draftParts = append(draftParts, utils.LiteralSegmentText(a.inFlight.Tokens))
	}
	for _, segment := range a.ready {
		draftParts = append(draftParts, utils.LiteralSegmentText(segment.Tokens))
	}
	if a.current != nil {
		draftParts = append(draftParts, utils.LiteralSegmentText(a.current.Tokens))
	}
	draftText := strings.TrimSpace(strings.Join(draftParts, " "))
	return a.finalText, draftText, combineTranscript(a.finalText, draftText), combineTranscript(a.literalFinalText, draftText)
}

func (a *liveTranscriptAssembler) pendingTokenCount() int {
	count := 0
	if a.inFlight != nil {
		count += len(a.inFlight.Tokens)
	}
	for _, segment := range a.ready {
		count += len(segment.Tokens)
	}
	if a.current != nil {
		count += len(a.current.Tokens)
	}
	return count
}

func appendBoundedTranscript(current, segment string, maxRunes int) (string, bool) {
	combined := combineTranscript(current, segment)
	runes := []rune(combined)
	if maxRunes <= 0 {
		return "", combined != ""
	}
	if len(runes) <= maxRunes {
		return combined, false
	}
	return strings.TrimSpace(string(runes[len(runes)-maxRunes:])), true
}

func statusForEnhancement(enhanced bool) string {
	if enhanced {
		return "enhanced"
	}
	return "literal"
}

func segmentSequences(segment liveTranscriptSegment) (uint64, uint64) {
	if len(segment.Tokens) == 0 {
		return 0, 0
	}
	return segment.Tokens[0].Sequence, segment.Tokens[len(segment.Tokens)-1].Sequence
}

func segmentConfidence(segment liveTranscriptSegment) float64 {
	if len(segment.Tokens) == 0 {
		return 0
	}
	var total float64
	for _, token := range segment.Tokens {
		total += token.Confidence
	}
	return total / float64(len(segment.Tokens))
}

func cloneLiveSegment(segment liveTranscriptSegment) liveTranscriptSegment {
	segment.Tokens = cloneTokens(segment.Tokens)
	return segment
}

func cloneTokens(tokens []utils.TranscriptSegmentToken) []utils.TranscriptSegmentToken {
	cloned := make([]utils.TranscriptSegmentToken, len(tokens))
	copy(cloned, tokens)
	return cloned
}
