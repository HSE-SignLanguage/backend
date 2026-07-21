package api

import (
	"testing"

	"streaming/mlclient"
)

func TestPredictionStabilizerEmitsFirstAcceptedPrediction(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "привет", Confidence: 0.91, Accepted: true}

	got, stable := stabilizer.Observe(prediction)
	if !stable {
		t.Fatal("an ML-accepted prediction must be emitted immediately")
	}
	if got.Text != prediction.Text || got.Confidence != prediction.Confidence {
		t.Fatalf("unexpected emitted prediction: %#v", got)
	}
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("an already emitted sign must not repeat")
	}
}

func TestPredictionStabilizerRequiresTwoRejectedPredictionsToRelease(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "дом", Accepted: true}

	if _, emitted := stabilizer.Observe(prediction); !emitted {
		t.Fatal("expected initial accepted sign to be emitted")
	}
	if _, stable := stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false}); stable {
		t.Fatal("rejected prediction must not be emitted")
	}
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("a single rejected window must not release the emitted sign")
	}

	stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false})
	stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false})
	if _, stable := stabilizer.Observe(prediction); !stable {
		t.Fatal("same sign must be allowed after two rejected separators")
	}
}

func TestPredictionStabilizerConfirmsDifferentAcceptedSign(t *testing.T) {
	var stabilizer predictionStabilizer
	if _, emitted := stabilizer.Observe(mlclient.Prediction{Text: "один", Accepted: true}); !emitted {
		t.Fatal("expected the first sign to be emitted")
	}
	if _, emitted := stabilizer.Observe(mlclient.Prediction{Text: "два", Accepted: true}); emitted {
		t.Fatal("a different sign must remain provisional for one window")
	}
	if got, emitted := stabilizer.Observe(mlclient.Prediction{Text: "два", Accepted: true}); !emitted || got.Text != "два" {
		t.Fatalf("expected a confirmed replacement sign, got %#v emitted=%v", got, emitted)
	}
}

func TestPredictionStabilizerSuppressesAlternatingAcceptedJitter(t *testing.T) {
	var stabilizer predictionStabilizer
	if _, emitted := stabilizer.Observe(mlclient.Prediction{Text: "один", Accepted: true}); !emitted {
		t.Fatal("expected the first sign to be emitted")
	}

	for _, text := range []string{"два", "один", "два", "один"} {
		if _, emitted := stabilizer.Observe(mlclient.Prediction{Text: text, Accepted: true}); emitted {
			t.Fatalf("alternating jitter %q must not be emitted", text)
		}
	}

	stabilizer.Observe(mlclient.Prediction{Accepted: false, Reason: "low_confidence"})
	stabilizer.Observe(mlclient.Prediction{Accepted: false, Reason: "low_confidence"})
	if got, emitted := stabilizer.Observe(mlclient.Prediction{Text: "два", Accepted: true}); !emitted || got.Text != "два" {
		t.Fatalf("a sign after a real pause must emit immediately, got %#v emitted=%v", got, emitted)
	}
}

func TestPredictionStabilizerTransportErrorDoesNotReleaseEmittedSign(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "дом", Accepted: true}
	stabilizer.Observe(prediction)

	stabilizer.OnError()
	stabilizer.OnError()
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("transport errors must not allow a held sign to be emitted twice")
	}
}

func TestTrimContextIsRuneSafe(t *testing.T) {
	if got := trimContext("абвгд", 3); got != "вгд" {
		t.Fatalf("expected rune-safe tail, got %q", got)
	}
}

func TestWebSocketSlotsEnforceGlobalCapacity(t *testing.T) {
	handlers := &HandlersConfig{
		webSocketSlots: make(chan struct{}, maxConcurrentSockets),
		webSocketsByIP: make(map[string]int),
	}
	for i := 0; i < maxConcurrentSockets; i++ {
		if !handlers.tryAcquireWebSocketSlot(string(rune('a' + i))) {
			t.Fatalf("slot %d should be available", i)
		}
	}
	if handlers.tryAcquireWebSocketSlot("overflow") {
		t.Fatal("slot above global capacity must be rejected")
	}

	handlers.releaseWebSocketSlot("a")
	if !handlers.tryAcquireWebSocketSlot("replacement") {
		t.Fatal("released slot must become available")
	}
}

func TestWebSocketSlotsEnforcePerIPCapacity(t *testing.T) {
	handlers := &HandlersConfig{
		webSocketSlots: make(chan struct{}, maxConcurrentSockets),
		webSocketsByIP: make(map[string]int),
	}

	for i := 0; i < maxSocketsPerIP; i++ {
		if !handlers.tryAcquireWebSocketSlot("192.0.2.1") {
			t.Fatalf("per-IP slot %d should be available", i)
		}
	}
	if handlers.tryAcquireWebSocketSlot("192.0.2.1") {
		t.Fatal("slot above per-IP capacity must be rejected")
	}
	if !handlers.tryAcquireWebSocketSlot("192.0.2.2") {
		t.Fatal("another IP must have its own capacity")
	}

	handlers.releaseWebSocketSlot("192.0.2.1")
	if !handlers.tryAcquireWebSocketSlot("192.0.2.1") {
		t.Fatal("released per-IP slot must become available")
	}
}

func TestOfferLatestBatchDropsQueuedStaleBatch(t *testing.T) {
	queue := make(chan [][]byte, 1)
	queue <- [][]byte{[]byte("stale")}

	latest := [][]byte{[]byte("latest")}
	if !offerLatestBatch(queue, latest) {
		t.Fatal("expected the queued stale batch to be dropped")
	}
	got := <-queue
	if string(got[0]) != "latest" {
		t.Fatalf("expected latest batch, got %q", got[0])
	}
}
