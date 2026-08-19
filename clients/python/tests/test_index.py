"""Integration tests for the Index parity surface, against a real node.

Starts the release-built turbovec-grpc binary on a scratch port with a
temporary TURBOVEC_DATA_DIR, once per test, and exercises the client's
node-level surface end to end: create (positional and id-mapped), add,
add_with_ids, search ranking against a brute-force dot product, retry-safe
replay, remove, and flush + restart + restore.

The binary defaults to target/release/turbovec-grpc in this repository;
override with TURBOVEC_NODE_BIN. With no binary the module skips.
"""

import os
import random
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

CLIENT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = CLIENT_DIR.parents[1]
sys.path.insert(0, str(CLIENT_DIR))

try:
    import turbovec_client  # noqa: F401
except ImportError:
    # The stubs are a build artifact; generate them the documented way.
    subprocess.run(
        [str(CLIENT_DIR / "gen_stubs.sh"), sys.executable], check=True, cwd=CLIENT_DIR
    )

import grpc  # noqa: E402

from turbovec_client import CollectionError, create_index, open_index  # noqa: E402

NODE_BIN = Path(
    os.environ.get("TURBOVEC_NODE_BIN", REPO_ROOT / "target/release/turbovec-grpc")
)
if not NODE_BIN.is_file():
    pytest.skip(
        f"node binary not found at {NODE_BIN}; build it or set TURBOVEC_NODE_BIN",
        allow_module_level=True,
    )

DIM = 64
BIT_WIDTH = 4


def make_rows(seed, count, dim=DIM):
    rng = random.Random(seed)
    return [[rng.uniform(-1.0, 1.0) for _ in range(dim)] for _ in range(count)]


def dot(a, b):
    return sum(x * y for x, y in zip(a, b))


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Node:
    """One node process over one data dir; restartable on a fresh port."""

    def __init__(self, data_dir: Path, log_path: Path):
        self.data_dir = data_dir
        self.log_path = log_path
        self.process = None
        self.address = None
        self._log = None

    def start(self):
        port = free_port()
        self.address = f"127.0.0.1:{port}"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._log = open(self.log_path, "ab")
        env = dict(
            os.environ,
            TURBOVEC_DATA_DIR=str(self.data_dir),
            TURBOVEC_GRPC_ADDR=self.address,
        )
        self.process = subprocess.Popen(
            [str(NODE_BIN)], env=env, stdout=self._log, stderr=subprocess.STDOUT
        )
        deadline = time.monotonic() + 15
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"node exited with {self.process.returncode}; see {self.log_path}"
                )
            try:
                channel = grpc.insecure_channel(self.address)
                grpc.channel_ready_future(channel).result(timeout=0.25)
                channel.close()
                return
            except grpc.FutureTimeoutError:
                if time.monotonic() > deadline:
                    raise RuntimeError("node did not become ready in 15s")

    def stop(self):
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self._log.close()
        self.process = None


@pytest.fixture
def node(tmp_path):
    node = Node(tmp_path / "data", tmp_path / "node.log")
    node.start()
    yield node
    node.stop()


def test_create_positional_and_info(node):
    with create_index(node.address, DIM, BIT_WIDTH) as index:
        info = index.info()
        assert info.dim == DIM
        assert info.bit_width == BIT_WIDTH
        assert info.len == 0
        assert info.calibration_state == "uncalibrated"
        assert info.generation == 0
        assert len(index) == 0
        assert index.index_id


def test_lazy_index_binds_dim_on_first_add(node):
    with create_index(node.address, bit_width=BIT_WIDTH) as index:
        assert index.info().dim == 0
        index.add(make_rows(1, 8))
        assert index.info().dim == DIM


def test_add_and_search_ranking(node):
    rows = make_rows(2, 256)
    with create_index(node.address, DIM, BIT_WIDTH) as index:
        index.add(rows)
        assert len(index) == 256

        # A stored row is its own best neighbour, exactly: its self-dot
        # dwarfs any cross-dot of uniform random rows, so quantization
        # cannot cost it the top slot.
        found = index.search(rows[7], k=10)
        assert found[0].id == 7
        assert [n.score for n in found] == sorted(
            (n.score for n in found), reverse=True
        )

        # For a novel query the scores are approximations, so assert the
        # identity of the top row (its brute-force margin is wide) and that
        # the rest come from the brute-force top ranks — not float equality
        # or an exact ordering, which quantization does not promise.
        query = make_rows(99, 1)[0]
        expected = sorted(range(len(rows)), key=lambda i: dot(query, rows[i]), reverse=True)
        found = index.search(query, k=5)
        assert found[0].id == expected[0]
        assert {n.id for n in found} <= set(expected[:8])

        # A batch returns one list per query, in order.
        batch = index.search([rows[0], rows[1]], k=3)
        assert len(batch) == 2
        assert batch[0][0].id == 0
        assert batch[1][0].id == 1


def test_add_with_ids_search_and_remove(node):
    rows = make_rows(3, 128)
    ids = [5000 + i for i in range(len(rows))]
    with create_index(node.address, DIM, BIT_WIDTH, id_mapped=True) as index:
        index.add_with_ids(rows, ids)
        assert len(index) == 128

        # Search reports the caller's own ids, not slots.
        found = index.search(rows[3], k=10)
        assert found[0].id == 5003

        assert index.remove(5003) is True
        assert len(index) == 127
        found = index.search(rows[3], k=10)
        assert all(n.id != 5003 for n in found)

        # Removing what is not there is False, not an error.
        assert index.remove(5003) is False
        assert index.remove(999_999) is False


def test_retry_safe_replay(node):
    rows = make_rows(4, 64)
    with create_index(node.address, DIM, BIT_WIDTH) as index:
        op = index.add(rows)
        assert len(index) == 64

        # Repeating the operation with its own id answers the committed
        # result; the rows are not added twice.
        assert index.add(rows, operation_id=op) == op
        assert len(index) == 64
        assert index.search(rows[0], k=1)[0].id == 0

        # The same id with a different row count is a different operation
        # under a reused name, and is refused rather than applied.
        with pytest.raises(CollectionError):
            index.add(make_rows(5, 32), operation_id=op)
        assert len(index) == 64


def test_flush_restart_restore(node):
    rows = make_rows(6, 128)
    ids = [7000 + i for i in range(len(rows))]
    with create_index(node.address, DIM, BIT_WIDTH, id_mapped=True) as index:
        index.add_with_ids(rows, ids)
        generation = index.flush()
        assert generation >= 1
        assert index.info().generation == generation
    node.stop()

    # Startup restores the flushed generation; open_index reattaches to it
    # without being told the handle, because the node holds exactly one. A
    # graceful stop persists again, so the generation may have advanced;
    # the rows and their ids are what must survive.
    node.start()
    with open_index(node.address) as index:
        assert len(index) == 128
        assert index.info().generation >= generation
        found = index.search(rows[5], k=10)
        assert found[0].id == 7005
