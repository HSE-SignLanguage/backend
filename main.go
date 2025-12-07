package main

import (
	"fmt"
	"streaming/api"
	"streaming/config"
	"streaming/logger"

	_ "streaming/docs"
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
	logger, err := logger.New("main.log")
	if err != nil {
		fmt.Printf("failed to create logger: %s", err)
	}

	config := config.GetConfig()

	router := api.NewRouter(logger)
	server := api.NewServer(config.Port, logger, router)

	server.Start()
}
