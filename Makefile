build:
	go build ./cmd/...
.PHONY: build

test:
	go test ./...
.PHONY: test