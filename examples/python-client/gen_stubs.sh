#!/usr/bin/env bash
# Generate the Python gRPC stubs from the vendored proto into ./generated.
# Run with the venv python:  .venv/bin/python -m pip install -r requirements.txt
#                            ./gen_stubs.sh        (or: .venv/bin/python gen_stubs.sh)
set -euo pipefail
cd "$(dirname "$0")"
PY="${1:-.venv/bin/python}"
mkdir -p generated
"$PY" -m grpc_tools.protoc \
    -I proto \
    --python_out=generated \
    --grpc_python_out=generated \
    proto/turbovec/v1/turbovec.proto
# Package markers so `generated.turbovec.v1` imports cleanly.
touch generated/turbovec/__init__.py generated/turbovec/v1/__init__.py
echo "stubs written to ./generated"
