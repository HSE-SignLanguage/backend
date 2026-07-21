package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port        int
	SwaggerHost string
}

func GetEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("environment variable %s not set", key)
	}
	return v, nil
}

func GetEnvInt(key string) int {
	vStr, err := GetEnv(key)
	if err != nil {
		return 0
	}
	var v int
	_, err = fmt.Sscanf(vStr, "%d", &v)
	if err != nil {
		return 0
	}
	return v
}

func GetConfig() *Config {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("could not load config: %s", err)
	}

	port := GetEnvInt("BACKEND_PORT")
	if port < 1 || port > 65535 {
		log.Fatalf("BACKEND_PORT not set or invalid")
	}

	swaggerHost, err := GetEnv("SWAGGER_BASE_URL")
	if err != nil {
		swaggerHost = fmt.Sprintf("localhost:%d", port)
		log.Printf("SWAGGER_BASE_URL not set, using %s", swaggerHost)
	}

	return &Config{
		Port:        port,
		SwaggerHost: swaggerHost,
	}
}
