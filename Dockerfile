# Build the server statically so the runtime stage needs no libc.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Copy the module files first so dependency resolution is cached independently of
# the sources. shardkv has no third-party dependencies today; keeping the split
# means adding one later does not invalidate the source layer.
COPY go.mod ./
RUN go mod download

COPY . .

# The version reported by INFO is the constant in internal/server, so it needs no
# link-time stamping here -- the image reports whatever the source says.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shardkv ./cmd/shardkv

FROM alpine:3.21

# A non-root user owns the data directory: the AOF is the only thing the server
# writes, so it needs write access to nothing else.
RUN adduser -D -u 10001 -h /data shardkv \
    && mkdir -p /data \
    && chown shardkv:shardkv /data

COPY --from=build /out/shardkv /usr/local/bin/shardkv

USER shardkv
WORKDIR /data
VOLUME /data
EXPOSE 6380

# An inline PING proves the server is answering commands, not merely holding the
# port open. busybox nc is already in the base image, so this needs no client
# library and no extra layer.
HEALTHCHECK --interval=10s --timeout=3s --start-period=2s --retries=3 \
    CMD printf 'PING\r\n' | nc -w 2 127.0.0.1 6380 | grep -q PONG

ENTRYPOINT ["shardkv"]
CMD ["-addr", ":6380", "-aof", "/data/shardkv.aof"]
