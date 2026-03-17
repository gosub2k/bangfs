.PHONY: clean install-tools all test build unit-test dummy-test integration-test

# Default target: build all binaries
.DEFAULT_GOAL := build

# Quick build (no proto regeneration)
mount-fuse-bangfs: ./cmd/mount-fuse-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o mount-fuse-bangfs ./cmd/mount-fuse-bangfs

mkfs-bangfs: ./cmd/mkfs-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o mkfs-bangfs ./cmd/mkfs-bangfs

reformat-bangfs: ./cmd/reformat-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o reformat-bangfs ./cmd/reformat-bangfs

build: mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Full build with proto regeneration
all: build

# Generate protobuf code only when .proto changes
proto/metadata.pb.go: proto/metadata.proto
	protoc --go_out=. --go_opt=paths=source_relative proto/metadata.proto

# Clean generated files and binaries
clean:
	rm -f proto/*.pb.go
	rm -f mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Install required tools (protoc-gen-go) - run manually once
install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# All tests in dummy mode (no external deps)
test: unit-test dummy-test

# Go unit tests
unit-test: build
	go test -v ./bangfuse/
	go test -v ./bangutil/

# Full test suite against file-backed store (includes multi-client)
dummy-test: build
	cd test && python3 test_bangfs.py --dummy

# Full test suite against Riak (includes multi-client)
# Set RIAK_HOST, RIAK_PORT, BANGFS_NAMESPACE env vars or use defaults
integration-test: build
	cd test && python3 test_bangfs.py
