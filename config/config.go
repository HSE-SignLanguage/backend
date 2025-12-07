package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port int
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
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("could not load config: %s", err)
	}

	return &Config{
		Port: GetEnvInt("BACKEND_PORT"),
	}
}
