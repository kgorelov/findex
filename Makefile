BINARY_NAME := findex
BIN_DIR := bin
BINARY := $(BIN_DIR)/$(BINARY_NAME)
SRC := $(shell find . -name "*.go" -not -path "./bin/*")

.PHONY: all build test clean

all: build

build: $(BINARY)

$(BINARY): $(SRC) go.mod go.sum
	@mkdir -p $(BIN_DIR)
	go build -trimpath -o $(BINARY) ./cmd/findex

test:
	go test -v ./...

clean:
	rm -rf $(BIN_DIR)
