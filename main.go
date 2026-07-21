package main

import (
	"context"
	"fmt"
	"os/signal"
	"streaming/api"
	"streaming/config"
	"streaming/docs"
	"streaming/logger"
	"syscall"
	"time"
)

// @title Video Streaming API
// @version 1.0
// @description API for video frame streaming and processing via WebSocket and video upload
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
// @schemes http ws

func main() {
	logger, err := logger.New("")
	if err != nil {
		fmt.Printf("failed to create logger: %s", err)
	}

	config := config.GetConfig()

	docs.SwaggerInfo.Host = config.SwaggerHost
	docs.SwaggerInfo.BasePath = config.SwaggerBasePath
	docs.SwaggerInfo.Schemes = config.SwaggerSchemes

	router := api.NewRouter(logger)
	server := api.NewServer(config.Port, logger, router)

	go server.Start()

	shutdownSignal, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-shutdownSignal.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Stop(shutdownCtx); err != nil {
		logger.Error("failed to stop server gracefully", "error", err)
	}
}
