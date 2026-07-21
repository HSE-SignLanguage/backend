package api

import (
	"strings"

	"streaming/mlclient"
)

const (
	stablePredictionCount  = 2
	releasePredictionCount = 2
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
		s.candidate = ""
		s.candidateCount = 0
		s.rejectedCount++
		if s.rejectedCount >= releasePredictionCount {
			s.Reset()
		}
		return mlclient.Prediction{}, false
	}
	s.rejectedCount = 0

	prediction.Text = text
	if !strings.EqualFold(s.candidate, text) {
		s.candidate = text
		s.candidateCount = 1
		return mlclient.Prediction{}, false
	}

	s.candidateCount++
	if s.candidateCount < stablePredictionCount || strings.EqualFold(s.emitted, text) {
		return mlclient.Prediction{}, false
	}

	s.emitted = text
	return prediction, true
}

func (s *predictionStabilizer) Reset() {
	s.candidate = ""
	s.candidateCount = 0
	s.emitted = ""
	s.rejectedCount = 0
}

// OnError discards only provisional evidence. A transport failure is not a
// semantic "no gesture" signal and must not release an already emitted sign.
func (s *predictionStabilizer) OnError() {
	s.candidate = ""
	s.candidateCount = 0
	s.rejectedCount = 0
}
