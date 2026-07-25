#!/usr/bin/env bash
# Generate Go stubs from the demo-local proto copy into ./gen.
# Requires protoc plus the two Go plugins:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail
cd "$(dirname "$0")"

export PATH="$PATH:$(go env GOPATH)/bin"

rm -rf gen
mkdir -p gen
protoc \
  --proto_path=proto \
  --go_out=gen --go_opt=paths=source_relative \
  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
  proto/turbovec/v1/turbovec.proto

echo "generated ./gen/turbovec/v1/{turbovec.pb.go,turbovec_grpc.pb.go}"
