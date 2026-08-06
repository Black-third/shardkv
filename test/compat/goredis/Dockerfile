FROM golang:1.26-alpine
WORKDIR /suite
COPY go.mod /suite/go.mod
COPY main.go /suite/main.go
# go get at build time rather than a checked-in go.sum: the dependency belongs to
# this harness, not to the server, so its lock file has no business in the repo.
RUN go get github.com/redis/go-redis/v9@latest \
    && go mod tidy \
    && go build -o /suite/compat .
CMD ["/suite/compat"]
