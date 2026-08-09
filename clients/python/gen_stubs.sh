#!/usr/bin/env bash
# Generate the Python gRPC stubs from the vendored proto into the package.
#
# Run once before importing turbovec_client, and again whenever the proto
# changes. Nothing generated is checked in.
#
#   python3 -m venv .venv
#   .venv/bin/pip install -r requirements-dev.txt
#   ./gen_stubs.sh                    # or: ./gen_stubs.sh /path/to/python
set -euo pipefail
cd "$(dirname "$0")"
PY="${1:-.venv/bin/python}"
OUT=turbovec_client/_generated
mkdir -p "$OUT"
"$PY" -m grpc_tools.protoc \
    -I proto \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    proto/turbovec/v1/turbovec.proto \
    proto/turbovec/v1/coordinator.proto
# Package markers, so `turbovec.v1` resolves inside the generated tree.
touch "$OUT/turbovec/__init__.py" "$OUT/turbovec/v1/__init__.py"
echo "stubs written to $OUT"
