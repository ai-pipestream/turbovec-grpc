"""Import the generated protobuf stubs, or say how to generate them.

The stubs are build artifacts: they are produced from the vendored proto by
``gen_stubs.sh`` and are not checked in, the same way the crate's own generated
code is produced at build time rather than committed. Importing them through
this module means a missing generation step reports itself as one instruction
rather than as a bare ModuleNotFoundError from three frames down.
"""

import os
import sys

_GENERATED = os.path.join(os.path.dirname(__file__), "_generated")

if _GENERATED not in sys.path:
    sys.path.insert(0, _GENERATED)

try:
    from turbovec.v1 import (
        coordinator_pb2,
        coordinator_pb2_grpc,
        turbovec_pb2,
        turbovec_pb2_grpc,
    )
except ImportError as error:  # pragma: no cover - only hit before generation
    raise ImportError(
        "turbovec_client's protobuf stubs have not been generated. Run "
        "./gen_stubs.sh in clients/python (see its README), then import again."
    ) from error

__all__ = [
    "coordinator_pb2",
    "coordinator_pb2_grpc",
    "turbovec_pb2",
    "turbovec_pb2_grpc",
]
