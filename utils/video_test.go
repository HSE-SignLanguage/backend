package utils

import "testing"

func TestParseFrameRateFraction(t *testing.T) {
	got, err := parseFrameRate("30000/1001")
	if err != nil {
		t.Fatal(err)
	}
	if got < 29.96 || got > 29.98 {
		t.Fatalf("unexpected frame rate: %f", got)
	}
}

func TestParseFrameRateRejectsZero(t *testing.T) {
	if _, err := parseFrameRate("0/0"); err == nil {
		t.Fatal("expected invalid zero frame rate")
	}
}

func TestBatchFramesPadsFinalBatch(t *testing.T) {
	frames := make([][]byte, 33)
	for index := range frames {
		frames[index] = []byte{byte(index)}
	}

	batches := BatchFrames(frames, 32)
	if len(batches) != 2 || len(batches[0]) != 32 || len(batches[1]) != 32 {
		t.Fatalf("unexpected batch sizes: %#v", batches)
	}
	if batches[1][0][0] != 32 || batches[1][31][0] != 32 {
		t.Fatal("final frame must be repeated to pad the last ML window")
	}
}

func TestWindowFramesUsesOverlapAndPadsTail(t *testing.T) {
	frames := make([][]byte, 40)
	for index := range frames {
		frames[index] = []byte{byte(index)}
	}

	windows := WindowFrames(frames, 32, 16)
	if len(windows) != 2 || len(windows[0]) != 32 || len(windows[1]) != 32 {
		t.Fatalf("unexpected windows: %#v", windows)
	}
	if windows[1][0][0] != 16 || windows[1][23][0] != 39 || windows[1][31][0] != 39 {
		t.Fatal("second window must start at stride 16 and pad the final frame")
	}
}

func TestWindowFramesDoesNotAddTailAfterExactFinalWindow(t *testing.T) {
	tests := []struct {
		name        string
		frameCount  int
		windowCount int
	}{
		{name: "one exact window", frameCount: 32, windowCount: 1},
		{name: "two exact overlapping windows", frameCount: 48, windowCount: 2},
		{name: "one exact and one padded window", frameCount: 40, windowCount: 2},
		{name: "maximum exact upload boundary", frameCount: 960, windowCount: 59},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frames := make([][]byte, tt.frameCount)
			for index := range frames {
				frames[index] = []byte{byte(index)}
			}

			windows := WindowFrames(frames, 32, 16)
			if len(windows) != tt.windowCount {
				t.Fatalf("expected %d windows for %d frames, got %d", tt.windowCount, tt.frameCount, len(windows))
			}
			for index, window := range windows {
				if len(window) != 32 {
					t.Fatalf("window %d has %d frames", index, len(window))
				}
			}
		})
	}
}

func TestExtractionRejectsUnboundedFrameCountBeforeFfmpeg(t *testing.T) {
	extractor := &VideoFrameExtractor{duration: 60, frameRate: 30}
	if _, err := extractor.ExtractFramesWithInterval(1); err == nil {
		t.Fatal("expected an excessive extracted frame count to be rejected")
	}
}

func TestEffectiveFrameIntervalFitsTwoMinuteVideoIntoBudget(t *testing.T) {
	extractor := &VideoFrameExtractor{duration: 120, frameRate: 30}
	if got := extractor.EffectiveFrameInterval(1); got != 4 {
		t.Fatalf("expected interval 4, got %d", got)
	}
}

func TestEffectiveFrameIntervalKeepsLargerRequestedValue(t *testing.T) {
	extractor := &VideoFrameExtractor{duration: 120, frameRate: 30}
	if got := extractor.EffectiveFrameInterval(10); got != 10 {
		t.Fatalf("expected requested interval 10, got %d", got)
	}
}
