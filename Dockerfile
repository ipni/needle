# syntax=docker/dockerfile:1
# Builder
FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.26-bookworm AS builder

LABEL org.opencontainers.image.source=https://github.com/ipni/needle
LABEL org.opencontainers.image.documentation=https://github.com/ipni/needle#docker
LABEL org.opencontainers.image.description="Needle: IPNI's delegated /routing/v1 HTTP server for IPFS systems"
LABEL org.opencontainers.image.licenses=MIT+APACHE_2.0

ARG TARGETPLATFORM TARGETOS TARGETARCH

ENV GOPATH=/go
ENV SRC_PATH=$GOPATH/src/github.com/ipni/needle
ENV GO111MODULE=on
ENV GOPROXY=https://proxy.golang.org

COPY go.mod go.sum $SRC_PATH/
WORKDIR $SRC_PATH
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . $SRC_PATH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o $GOPATH/bin/needle

# Runner
FROM debian:bookworm-slim

RUN apt-get update && \
  apt-get install --no-install-recommends -y tini ca-certificates curl && \
  rm -rf /var/lib/apt/lists/*

ENV GOPATH=/go
ENV SRC_PATH=$GOPATH/src/github.com/ipni/needle
ENV DATA_PATH=/data/needle

COPY --from=builder $GOPATH/bin/needle /usr/local/bin/needle

RUN mkdir -p $DATA_PATH && \
    useradd -d $DATA_PATH -u 1000 -G users ipfs && \
    chown ipfs:users $DATA_PATH
VOLUME $DATA_PATH
WORKDIR $DATA_PATH

USER ipfs
ENTRYPOINT ["tini", "--", "/usr/local/bin/needle", "start"]
