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
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("could not load config: %s", err)
	}

	port := GetEnvInt("BACKEND_PORT")
	if port == 0 {
		log.Fatalf("BACKEND_PORT not set or invalid")
	}

	return &Config{
		Port: port,
	}
}
