package api

import (
	"context"
	"log"
	"net/http"
	"streaming/logger"
	"streaming/utils"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type HandlersConfig struct {
	log      *logger.MultiLogger
	receiver *websocket.Conn
	sender   *websocket.Conn
	mu       sync.Mutex
}

func NewHandlersConfig(log *logger.MultiLogger) *HandlersConfig {
	return &HandlersConfig{
		log: log,
	}
}

func (hc *HandlersConfig) VideoSocketHandler(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		hc.log.Error("failed to accept websocket", "error", err)
		utils.WriteError(w, http.StatusBadRequest, "failed to accept websocket")
		return
	}

	c.SetReadLimit(10 * 1024 * 1024)

	hc.mu.Lock()

	var role string
	switch {
	case hc.receiver == nil:
		hc.receiver = c
		role = "receiver"
		hc.log.Info("Client connected as receiver")
	case hc.sender == nil:
		hc.sender = c
		role = "sender"
		hc.log.Info("Client connected as sender")
	default:
		hc.mu.Unlock()
		c.Close(websocket.StatusPolicyViolation, "max clients reached")
		hc.log.Warn("Connection rejected: max clients reached")
		return
	}
	hc.mu.Unlock()

	if role == "receiver" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"config","role":"receiver"}`))
		cancel()
		if err != nil {
			hc.log.Error("failed to send role assignment", "error", err)
			hc.mu.Lock()
			hc.receiver = nil
			hc.mu.Unlock()
			c.Close(websocket.StatusInternalError, "failed to send config")
			return
		}
		hc.handleReceiver(c)
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"config","role":"sender"}`))
		cancel()
		if err != nil {
			hc.log.Error("failed to send role assignment", "error", err)
			hc.mu.Lock()
			hc.sender = nil
			hc.mu.Unlock()
			c.Close(websocket.StatusInternalError, "failed to send config")
			return
		}
		hc.handleSender(c)
	}
}

func (hc *HandlersConfig) handleReceiver(c *websocket.Conn) {
	defer func() {
		hc.mu.Lock()
		hc.receiver = nil
		hc.mu.Unlock()
		c.Close(websocket.StatusNormalClosure, "bye")
		hc.log.Info("Receiver disconnected")
	}()
	for {
		_, _, err := c.Read(context.Background())

		if err != nil {
			log.Println("receiver disconnected:", err)
			break
		}
	}
}

func (hc *HandlersConfig) handleSender(c *websocket.Conn) {
	defer func() {
		hc.mu.Lock()
		hc.sender = nil
		hc.mu.Unlock()
		c.Close(websocket.StatusNormalClosure, "bye")
		hc.log.Info("Sender disconnected")
	}()

	for {
		typ, data, err := c.Read(context.Background())

		if err != nil {
			log.Println("sender read error:", err)
			break
		}

		if typ != websocket.MessageBinary {
			continue
		}

		if len(data) == 0 {
			continue
		}

		log.Printf("received video chunk from sender: %d bytes\n", len(data))

		hc.mu.Lock()
		receiver := hc.receiver
		hc.mu.Unlock()

		if receiver != nil {
			writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := receiver.Write(writeCtx, websocket.MessageBinary, data)
			writeCancel()

			if err != nil {
				log.Println("failed to forward to receiver:", err)
				hc.mu.Lock()
				hc.receiver = nil
				hc.mu.Unlock()
			} else {
				log.Printf("forwarded %d bytes to receiver\n", len(data))
			}
		}
	}
}
