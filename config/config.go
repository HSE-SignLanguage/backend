package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port            int
	SwaggerHost     string
	SwaggerBasePath string
	SwaggerSchemes  []string
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

	swaggerBaseURL, err := GetEnv("SWAGGER_BASE_URL")
	if err != nil {
		swaggerBaseURL = fmt.Sprintf("http://localhost:%d", port)
		log.Printf("SWAGGER_BASE_URL not set, using %s", swaggerBaseURL)
	}

	swaggerHost, swaggerBasePath, swaggerSchemes, err := parseSwaggerBaseURL(swaggerBaseURL)
	if err != nil {
		log.Fatalf("SWAGGER_BASE_URL is invalid: %s", err)
	}

	return &Config{
		Port:            port,
		SwaggerHost:     swaggerHost,
		SwaggerBasePath: swaggerBasePath,
		SwaggerSchemes:  swaggerSchemes,
	}
}

func parseSwaggerBaseURL(raw string) (string, string, []string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", nil, fmt.Errorf("value is empty")
	}

	// Keep legacy host-only values working while preferring an explicit public URL.
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", nil, fmt.Errorf("parse URL: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", nil, fmt.Errorf("scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", "", nil, fmt.Errorf("host is required")
	}
	if parsed.User != nil {
		return "", "", nil, fmt.Errorf("credentials are not allowed")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", nil, fmt.Errorf("query parameters and fragments are not allowed")
	}

	basePath := path.Clean(parsed.EscapedPath())
	if basePath == "." {
		basePath = "/"
	}
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}

	return parsed.Host, basePath, []string{scheme}, nil
}
