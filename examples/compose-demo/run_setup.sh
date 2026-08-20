#!/bin/sh
# One-shot index setup entrypoint. The clients/python tree is mounted
# read-only and stub generation writes into the package, so work on a copy
# in /tmp.
set -eu
cp -r /client /tmp/client
cd /tmp/client
pip install -q --disable-pip-version-check -r requirements-dev.txt
./gen_stubs.sh "$(command -v python)"
exec env PYTHONPATH=/tmp/client python /demo/setup_index.py
