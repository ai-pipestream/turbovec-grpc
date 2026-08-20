# Compose demo: one node to three, by autosplit

Three durable nodes and one coordinator in docker compose. The loader fills
a collection that starts as one shard on node1; the coordinator's grow-only
autoscaler then splits it onto node2 and node3 as the row ceiling is crossed.

## Prerequisites

docker with the compose plugin. Nothing else: the image builds both binaries
from the repo root, and the loader runs in a container.

## Run

```bash
docker compose build
docker compose up -d node1 coordinator
docker compose --profile demo run --rm loader
```

Bringing the coordinator up first runs a one-shot `setup` container: the
coordinator's topology is durable, which pins every startup shard to an
index id and a durable generation, so setup creates node1's index, flushes
it, and hands the node-table entry to the coordinator through a shared
volume. The loader then calibrates the collection (before any rows: a
calibration pair commits at construction), adds 1,000,000
clustered-gaussian vectors to node1 in chunks of 20,000, and waits. While
it waits, stock the spare pool:

```bash
docker compose up -d node2 node3
docker compose logs -f coordinator
```

The coordinator log shows the splits: every
`TURBOVEC_AUTOSCALE_INTERVAL_MS` it splits one over-ceiling shard onto its
own node plus one spare, moving half the rows as encoded codes under the
collection's one calibration pair, and flushing the targets before the new
topology publishes - which is also where the ingested rows become durable.
Two splits later the collection is three shards - 250k, 250k, 500k rows at
the default 1M - and the pool is empty. There it stops: a split always
keeps half its rows on the source node, so three nodes hold at most three
shards, and the 500k shard waits for a spare that never comes. The loader
sees the shard count settle and runs the query phase: p50/p95/p99 over 100
queries, a self-match check (the top neighbour of a stored row planted away
from the clusters is itself), and a final health dump whose per-shard rows
must sum to the ingest count. It exits non-zero if any of that fails.

## Why the phased startup

A collection takes rows only until its first split. Split moves encoded
rows with their labels, and a labelled index refuses further adds
(`labelled_index_immutable`); the source index is dropped once its split
publishes. So the demo fills node1 while the spare pool is empty - the
coordinator logs `shard over the row ceiling but the spare pool is empty`
until node2 and node3 register - and grows afterwards. The loader refuses
to ingest with a non-empty spare pool rather than lose rows to a mid-ingest
split.

For the same pinning reason the loader uses plain adds, not the client's
retry-safe envelope: a retry-safe add persists a new durable generation per
operation, and the topology pins the source shard at the generation the
calibration commit flushed, so the coordinator would read the first
persisted add as `shard_generation_mismatch`. Plain adds leave the pinned
generation alone; the autosplit's target flush is the durability boundary.

This is also why the index is positional: an id-mapped index does not
expose its encoded rows, so Split - and therefore the autoscaler - refuses
it by name (`positional_index_required`). Row identity still survives the
splits: a split shard labels every row with the slot it had in the index
the collection was built as, which is what the self-match check reads back.

## Smaller or larger runs

```bash
docker compose --profile demo run --rm loader 600000
```

The argument overrides the `DEMO_ROWS` default. The row ceiling and tick
interval are the `TURBOVEC_AUTOSCALE_*` variables on the coordinator
service in `docker-compose.yml`. A run smaller than the ceiling never
splits, and the loader's watch phase eventually times out - that is the
autoscaler working as configured, not a failure of the cluster.

## From the host

node1's 50051 and the coordinator's 50050 are published, so the same script
runs outside compose against the same cluster:

```bash
python3 -m venv .venv
.venv/bin/pip install -r ../../clients/python/requirements-dev.txt numpy
(cd ../../clients/python && ./gen_stubs.sh)
PYTHONPATH=../../clients/python .venv/bin/python load_demo.py 600000
```

The client calls used here - `create_index`, `connect`,
`Collection.calibrate`, `search`, `health` - are documented in
[../../clients/python/README.md](../../clients/python/README.md).

## Teardown

```bash
docker compose down -v
```

The volumes are named, so `down` without `-v` keeps the shards and the
topology. Note that a node persists every shard on shutdown, which moves it
off the generation the topology pinned, so a restarted cluster reports
`shard_generation_mismatch` until it is rebuilt; `-v` gives the next run a
clean cluster.
