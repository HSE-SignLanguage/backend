# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
	go build -trimpath -ldflags="-s -w" -o /bin/backend .

FROM gcr.io/distroless/static-debian12:nonroot

ARG BACKEND_PORT=8080
ENV BACKEND_PORT=${BACKEND_PORT}
WORKDIR /app

COPY --from=builder /bin/backend /usr/local/bin/backend

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/backend"]
