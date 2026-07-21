package api

import (
	"strings"

	"streaming/mlclient"
)

const (
	replacementPredictionCount = 2
	releasePredictionCount     = 2
)

type predictionStabilizer struct {
	candidate      string
	candidateCount int
	emitted        string
	rejectedCount  int
}

func (s *predictionStabilizer) Observe(prediction mlclient.Prediction) (mlclient.Prediction, bool) {
	text := strings.TrimSpace(prediction.Text)
	if !prediction.Accepted || text == "" || shouldSkipLiteral(text) {
		s.clearCandidate()
		s.rejectedCount++
		if s.rejectedCount >= releasePredictionCount {
			s.Reset()
		}
		return mlclient.Prediction{}, false
	}
	s.rejectedCount = 0

	prediction.Text = text
	if s.emitted == "" {
		s.emitted = text
		s.clearCandidate()
		return prediction, true
	}
	if strings.EqualFold(s.emitted, text) {
		s.clearCandidate()
		return mlclient.Prediction{}, false
	}

	if !strings.EqualFold(s.candidate, text) {
		s.candidate = text
		s.candidateCount = 1
		return mlclient.Prediction{}, false
	}
	s.candidateCount++
	if s.candidateCount < replacementPredictionCount {
		return mlclient.Prediction{}, false
	}

	s.emitted = text
	s.clearCandidate()
	return prediction, true
}

func (s *predictionStabilizer) Reset() {
	s.clearCandidate()
	s.emitted = ""
	s.rejectedCount = 0
}

func (s *predictionStabilizer) clearCandidate() {
	s.candidate = ""
	s.candidateCount = 0
}

// OnError interrupts a rejection streak. A transport failure is not a semantic
// "no gesture" signal and must not release an already emitted sign.
func (s *predictionStabilizer) OnError() {
	s.clearCandidate()
	s.rejectedCount = 0
}
