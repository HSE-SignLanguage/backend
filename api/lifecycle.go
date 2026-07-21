package api

import (
	"context"
	"sync"
	"time"
)

const shutdownCancellationReserve = 3 * time.Second

// handlerLifecycle coordinates work that outlives a regular HTTP request,
// notably upgraded WebSocket sessions and detached video jobs. Admission and
// WaitGroup registration share one lock so no work can be added after draining
// starts while Shutdown is waiting.
type handlerLifecycle struct {
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	draining      bool
	active        sync.WaitGroup
	nextSessionID uint64
	webSocketStop map[uint64]func()
}

// BeginShutdown closes admission for new long-running work and asks active
// WebSocket sessions to leave. It is idempotent.
func (hc *HandlersConfig) BeginShutdown() {
	hc.lifecycle.beginShutdown()
}

// Shutdown drains active WebSocket handlers and detached video jobs within
// ctx. Near the deadline it cancels the shared work context so external calls
// and FFmpeg can terminate and clean up.
func (hc *HandlersConfig) Shutdown(ctx context.Context) error {
	return hc.lifecycle.shutdown(ctx)
}

func (l *handlerLifecycle) initialize() {
	l.once.Do(func() {
		l.ctx, l.cancel = context.WithCancel(context.Background())
		l.webSocketStop = make(map[uint64]func())
	})
}

func (l *handlerLifecycle) context() context.Context {
	l.initialize()
	return l.ctx
}

func (l *handlerLifecycle) tryBeginWork() bool {
	l.initialize()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return false
	}
	l.active.Add(1)
	return true
}

func (l *handlerLifecycle) endWork() {
	l.active.Done()
}

func (l *handlerLifecycle) isDraining() bool {
	l.initialize()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func (l *handlerLifecycle) registerWebSocket(stop func()) uint64 {
	l.initialize()
	l.mu.Lock()
	l.nextSessionID++
	sessionID := l.nextSessionID
	l.webSocketStop[sessionID] = stop
	draining := l.draining
	l.mu.Unlock()

	// A session may have passed admission immediately before shutdown started.
	// Stop it here if it was registered after BeginShutdown took its snapshot.
	if draining {
		go stop()
	}
	return sessionID
}

func (l *handlerLifecycle) unregisterWebSocket(sessionID uint64) {
	l.initialize()
	l.mu.Lock()
	delete(l.webSocketStop, sessionID)
	l.mu.Unlock()
}

func (l *handlerLifecycle) beginShutdown() {
	l.initialize()
	l.mu.Lock()
	if l.draining {
		l.mu.Unlock()
		return
	}
	l.draining = true
	stops := make([]func(), 0, len(l.webSocketStop))
	for _, stop := range l.webSocketStop {
		stops = append(stops, stop)
	}
	l.mu.Unlock()

	// WebSocket sessions are unbounded by nature, so ask them to close as soon
	// as draining begins. Video jobs keep their grace period and are cancelled
	// only when the bounded drain window expires.
	for _, stop := range stops {
		go stop()
	}
}

func (l *handlerLifecycle) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	l.beginShutdown()

	done := make(chan struct{})
	go func() {
		l.active.Wait()
		close(done)
	}()

	drainCtx := ctx
	stopDrain := func() {}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			reserve := min(shutdownCancellationReserve, remaining/2)
			drainDeadline := deadline.Add(-reserve)
			var cancel context.CancelFunc
			drainCtx, cancel = context.WithDeadline(ctx, drainDeadline)
			stopDrain = cancel
		}
	}

	select {
	case <-done:
		stopDrain()
		l.cancel()
		return nil
	case <-drainCtx.Done():
		stopDrain()
	}

	// The graceful window is over. Cancellation reaches ML/OpenRouter and the
	// context-aware FFmpeg process, leaving the remaining caller deadline for
	// goroutines to run their cleanup defers.
	l.cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
