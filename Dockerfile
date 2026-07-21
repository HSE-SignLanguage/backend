# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5

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

FROM alpine:3.23 AS runtime

RUN apk add --no-cache ffmpeg ca-certificates tzdata \
    && addgroup -S -g 10001 backend \
    && adduser -S -D -H -u 10001 -G backend backend

ARG BACKEND_PORT=8080
ENV BACKEND_PORT=${BACKEND_PORT}
ENV TMPDIR=/app/tmp

WORKDIR /app

RUN mkdir -p /app/tmp/uploads \
    && chown -R backend:backend /app

COPY --from=builder /bin/backend /usr/local/bin/backend

EXPOSE 8080

USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --quiet --tries=1 --spider "http://127.0.0.1:${BACKEND_PORT}/health" || exit 1

ENTRYPOINT ["/usr/local/bin/backend"]
