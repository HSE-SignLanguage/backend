package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"streaming/mlclient"
	"streaming/utils"
)

func TestLiveTranscriptAssemblerStreamsDraftWhileFormatting(t *testing.T) {
	predictions := make(chan mlclient.Prediction)
	messages := make(chan WebSocketMessage, 16)
	firstFormatStarted := make(chan []utils.TranscriptSegmentToken, 1)
	releaseFirstFormat := make(chan struct{})
	formatCall := 0

	formatter := func(ctx context.Context, _ string, tokens []utils.TranscriptSegmentToken) (string, bool, error) {
		formatCall++
		if formatCall == 1 {
			firstFormatStarted <- tokens
			select {
			case <-releaseFirstFormat:
				return "Привет, мир.", true, nil
			case <-ctx.Done():
				return "", false, ctx.Err()
			}
		}
		return "", false, errors.New("formatter unavailable")
	}
	sender := func(_ context.Context, message WebSocketMessage) error {
		messages <- message
		return nil
	}

	assembler := newLiveTranscriptAssembler(100*time.Millisecond, utils.MaxTranscriptSegmentTokens, formatter, sender, nil)
	runDone := make(chan error, 1)
	go func() {
		runDone <- assembler.Run(context.Background(), predictions)
	}()

	predictions <- mlclient.Prediction{Text: "привет", Confidence: 0.91, Accepted: true}
	assertLiveMessage(t, nextLiveMessage(t, messages), WebSocketMessage{
		Type: "gesture", Text: "привет", FullText: "привет", LiteralText: "привет", FinalText: "", DraftText: "привет",
		Confidence: 0.91, Status: "draft", Sequence: 1, SegmentID: 1, FirstSequence: 1, LastSequence: 1, TokenCount: 1,
	})

	predictions <- mlclient.Prediction{Text: "мир", Confidence: 0.83, Accepted: true}
	secondGesture := nextLiveMessage(t, messages)
	assertLiveMessage(t, secondGesture, WebSocketMessage{
		Type: "gesture", Text: "мир", FullText: "привет мир", LiteralText: "привет мир", FinalText: "", DraftText: "привет мир",
		Confidence: 0.83, Status: "draft", Sequence: 2, SegmentID: 1, FirstSequence: 2, LastSequence: 2, TokenCount: 1,
	})

	formatting := nextLiveMessage(t, messages)
	if formatting.Type != "formatting" || formatting.Status != "formatting" || formatting.SegmentID != 1 || formatting.FirstSequence != 1 || formatting.LastSequence != 2 || formatting.TokenCount != 2 {
		t.Fatalf("unexpected formatting event: %#v", formatting)
	}
	if formatting.FullText != "привет мир" || formatting.FinalText != "" || formatting.DraftText != "привет мир" {
		t.Fatalf("unexpected formatting snapshots: %#v", formatting)
	}
	if formatting.LiteralText != "привет мир" {
		t.Fatalf("unexpected literal formatting snapshot: %#v", formatting)
	}

	startedTokens := nextFormatTokens(t, firstFormatStarted)
	if len(startedTokens) != 2 || startedTokens[0].Text != "привет" || startedTokens[1].Text != "мир" {
		t.Fatalf("expected two-token idle batch, got %#v", startedTokens)
	}

	// A new gesture is published immediately while segment 1 is still blocked
	// in the external formatter. It belongs to the next ordered segment and is
	// included in the authoritative draft/full snapshots.
	predictions <- mlclient.Prediction{Text: "дом", Confidence: 0.79, Accepted: true}
	thirdGesture := nextLiveMessage(t, messages)
	assertLiveMessage(t, thirdGesture, WebSocketMessage{
		Type: "gesture", Text: "дом", FullText: "привет мир дом", LiteralText: "привет мир дом", FinalText: "", DraftText: "привет мир дом",
		Confidence: 0.79, Status: "draft", Sequence: 3, SegmentID: 2, FirstSequence: 3, LastSequence: 3, TokenCount: 1,
	})

	close(releaseFirstFormat)
	transcript := nextLiveMessage(t, messages)
	if transcript.Type != "transcript" || transcript.Status != "enhanced" || transcript.Enhanced == nil || !*transcript.Enhanced {
		t.Fatalf("unexpected enhanced transcript event: %#v", transcript)
	}
	if transcript.Text != "Привет, мир." || transcript.FinalText != "Привет, мир." || transcript.DraftText != "дом" || transcript.FullText != "Привет, мир. дом" {
		t.Fatalf("new pending token was lost or snapshots are wrong: %#v", transcript)
	}
	if transcript.LiteralText != "привет мир дом" {
		t.Fatalf("literal recognition was replaced by formatter output: %#v", transcript)
	}
	if transcript.Sequence != 2 || transcript.SegmentID != 1 || transcript.FirstSequence != 1 || transcript.LastSequence != 2 {
		t.Fatalf("unexpected enhanced transcript ordering metadata: %#v", transcript)
	}

	close(predictions)
	secondFormatting := nextLiveMessage(t, messages)
	if secondFormatting.Type != "formatting" || secondFormatting.SegmentID != 2 || secondFormatting.FullText != "Привет, мир. дом" {
		t.Fatalf("unexpected second formatting event: %#v", secondFormatting)
	}
	literalTranscript := nextLiveMessage(t, messages)
	if literalTranscript.Type != "transcript" || literalTranscript.Status != "literal" || literalTranscript.Enhanced == nil || *literalTranscript.Enhanced {
		t.Fatalf("expected literal fallback event, got %#v", literalTranscript)
	}
	if literalTranscript.Text != "дом" || literalTranscript.FinalText != "Привет, мир. дом" || literalTranscript.DraftText != "" || literalTranscript.FullText != "Привет, мир. дом" || literalTranscript.LiteralText != "привет мир дом" {
		t.Fatalf("unexpected literal fallback snapshots: %#v", literalTranscript)
	}
	if literalTranscript.Sequence != 3 || literalTranscript.SegmentID != 2 {
		t.Fatalf("unexpected literal fallback ordering: %#v", literalTranscript)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("assembler returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("assembler did not stop after draining closed input")
	}
}

func assertLiveMessage(t *testing.T, got, want WebSocketMessage) {
	t.Helper()
	if got.Type != want.Type || got.Text != want.Text || got.FullText != want.FullText || got.LiteralText != want.LiteralText || got.FinalText != want.FinalText || got.DraftText != want.DraftText ||
		got.Confidence != want.Confidence || got.Status != want.Status || got.Sequence != want.Sequence || got.SegmentID != want.SegmentID ||
		got.FirstSequence != want.FirstSequence || got.LastSequence != want.LastSequence || got.TokenCount != want.TokenCount {
		t.Fatalf("unexpected live message:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestAppendBoundedTranscriptKeepsLatestRunes(t *testing.T) {
	got, truncated := appendBoundedTranscript("один два", "три", 8)
	if !truncated || got != "два три" {
		t.Fatalf("unexpected bounded transcript: %q, truncated=%v", got, truncated)
	}

	got, truncated = appendBoundedTranscript("один", "два", 20)
	if truncated || got != "один два" {
		t.Fatalf("unexpected unbounded transcript: %q, truncated=%v", got, truncated)
	}
}

func TestLiveTranscriptAssemblerDiscardsOversizedMLToken(t *testing.T) {
	predictions := make(chan mlclient.Prediction, 2)
	predictions <- mlclient.Prediction{Text: strings.Repeat("я", utils.MaxTranscriptTokenRunes+1), Confidence: 1, Accepted: true}
	predictions <- mlclient.Prediction{Text: "дом", Confidence: 0.9, Accepted: true}
	close(predictions)

	messages := make([]WebSocketMessage, 0, 3)
	assembler := newLiveTranscriptAssembler(
		time.Millisecond,
		utils.MaxTranscriptSegmentTokens,
		func(_ context.Context, _ string, tokens []utils.TranscriptSegmentToken) (string, bool, error) {
			return utils.LiteralSegmentText(tokens), false, nil
		},
		func(_ context.Context, message WebSocketMessage) error {
			messages = append(messages, message)
			return nil
		},
		nil,
	)
	if err := assembler.Run(context.Background(), predictions); err != nil {
		t.Fatalf("run assembler: %v", err)
	}
	if len(messages) != 3 || messages[0].Type != "gesture" || messages[0].Text != "дом" || messages[0].Sequence != 1 {
		t.Fatalf("unexpected messages after invalid token: %#v", messages)
	}
}

func nextLiveMessage(t *testing.T, messages <-chan WebSocketMessage) WebSocketMessage {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live transcript message")
		return WebSocketMessage{}
	}
}

func nextFormatTokens(t *testing.T, started <-chan []utils.TranscriptSegmentToken) []utils.TranscriptSegmentToken {
	t.Helper()
	select {
	case tokens := <-started:
		return tokens
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for formatter")
		return nil
	}
}
