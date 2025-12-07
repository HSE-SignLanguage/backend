# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.24.4

FROM golang:${GO_VERSION}-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /bin/backend .

FROM alpine:3.20 AS runtime

RUN apk add --no-cache ffmpeg ca-certificates tzdata

ARG BACKEND_PORT=8080
ENV BACKEND_PORT=${BACKEND_PORT}

WORKDIR /app

COPY --from=builder /bin/backend /usr/local/bin/backend

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/backend"]