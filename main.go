package main

import (
	"fmt"
	"streaming/api"
	"streaming/config"
	"streaming/logger"
)

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
