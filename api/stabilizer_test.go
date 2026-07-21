package api

import (
	"testing"

	"streaming/mlclient"
)

func TestPredictionStabilizerRequiresTwoMatchingPredictions(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "привет", Confidence: 0.91, Accepted: true}

	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("first prediction must remain provisional")
	}
	got, stable := stabilizer.Observe(prediction)
	if !stable {
		t.Fatal("second matching prediction must be emitted")
	}
	if got.Text != prediction.Text || got.Confidence != prediction.Confidence {
		t.Fatalf("unexpected stable prediction: %#v", got)
	}
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("an already emitted sign must not repeat")
	}
}

func TestPredictionStabilizerRequiresTwoRejectedPredictionsToRelease(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "дом", Accepted: true}

	stabilizer.Observe(prediction)
	if _, stable := stabilizer.Observe(prediction); !stable {
		t.Fatal("expected initial sign to stabilize")
	}
	if _, stable := stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false}); stable {
		t.Fatal("rejected prediction must not be emitted")
	}
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("first prediction after a single rejection must remain provisional")
	}
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("a single rejected window must not release the emitted sign")
	}

	stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false})
	stabilizer.Observe(mlclient.Prediction{Text: "no", Accepted: false})
	if _, stable := stabilizer.Observe(prediction); stable {
		t.Fatal("first prediction after release must remain provisional")
	}
	if _, stable := stabilizer.Observe(prediction); !stable {
		t.Fatal("same sign must be allowed after two rejected separators")
	}
}

func TestPredictionStabilizerResetsCandidateOnDifferentSign(t *testing.T) {
	var stabilizer predictionStabilizer
	stabilizer.Observe(mlclient.Prediction{Text: "один", Accepted: true})
	if _, stable := stabilizer.Observe(mlclient.Prediction{Text: "два", Accepted: true}); stable {
		t.Fatal("a different sign must restart stabilization")
	}
	if got, stable := stabilizer.Observe(mlclient.Prediction{Text: "два", Accepted: true}); !stable || got.Text != "два" {
		t.Fatalf("expected the repeated replacement sign, got %#v stable=%v", got, stable)
	}
}

func TestPredictionStabilizerTransportErrorDoesNotReleaseEmittedSign(t *testing.T) {
	var stabilizer predictionStabilizer
	prediction := mlclient.Prediction{Text: "дом", Accepted: true}
	stabilizer.Observe(prediction)
	stabilizer.Observe(prediction)

	stabilizer.OnError()
	stabilizer.OnError()
	stabilizer.Observe(prediction)
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
