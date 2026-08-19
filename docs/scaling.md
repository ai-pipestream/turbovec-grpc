# Scaling: the metadata protocol and the road to an autoscaled surface

This document is the normative description of how collection metadata moves
through `turbovec-grpc`, the wire-compatibility policy for its two protos, and
the staged plan for growing the coordinator from operator-driven to
spare-driven. It describes what the code does today first, because the
protocol that already exists is the one being formalized — not replaced.

## 1. Roles

- **Coordinator** — the single metadata authority. It owns the shard table,
  the topology generation, the spare pool, and every decision about which
  node serves which rows. One coordinator per collection.
- **Node** — dumb by design. A node serves handle-addressed indexes, answers
  probes, executes scans under floors, and moves encoded rows when told. A
  node never learns the topology and never needs to.
- **Client** — never names a shard. It searches the collection; the
  coordinator fans out and merges.

There is no gossip. Every metadata fact flows through the coordinator, which
serializes all reads and writes of it.

## 2. Topology generations are fencing tokens

The shard table is versioned by a monotonically increasing `u64` generation.
The generation and the table are persisted atomically to the coordinator
state file (`TURBOVEC_COORD_STATE`) before any actor may observe them.

Rules:

1. **Monotonic, never reused.** A new topology publishes at `generation + 1`.
   `persist_topology` refuses generation 0 and empty tables.
2. **Durable before visible.** On restart the coordinator loads the persisted
   state. It never silently falls back to the startup node table: the table
   initializes generation 1 only when no state file exists, and a state file
   whose contents disagree with the startup table is an operator-visible
   error, not a quiet override.
3. **Every collection response carries the generation it served**
   (`topology_generation` on `ListNodesResponse`, `CollectionSearchResponse`,
   `FitCalibrationResponse`, `SplitResponse`, `JoinResponse`,
   `RegisterNodeResponse`). In-flight searches may finish on the old
   generation while an administrative rebind activates the next one; the
   response says which one answered.
4. **Replicas fence themselves.** A shard may name a required durable
   generation; a replica is eligible only when it serves the same index id at
   exactly that generation. A stale replica is not a slow replica — it is not
   a replica.

Because all metadata writes serialize through one coordinator, the generation
number is also a total order over topology changes. It plays the role a Raft
term would: any future consensus-backed coordinator can adopt the same wire
fields unchanged, with the term simply becoming the generation.

## 3. Membership: announce, dial-back, spare pool

A node joins by announcing itself with `RegisterNode` (startup, then periodic
re-announce every `TURBOVEC_REGISTER_INTERVAL_MS`, default 30 s).

- The coordinator **dials the advertised address back before accepting it**.
  An unreachable address is refused at registration, while the operator can
  still read the node's logs — not later, mid-Split.
- Registration is idempotent: re-announcing a known address succeeds and
  changes nothing.
- **Registration never changes the serving topology.** A non-member joins the
  persisted spare pool. A spare serves rows only when an operator (today) or
  the placement policy (§6) names it as a `Split`/`Join` target.
- A node already serving a shard is reported as `member` and is not pooled.

Membership is soft state layered on durable state: the spare pool persists
with the topology, liveness is established by probing, and a dead spare is
listed with its probe error rather than silently dropped.

## 4. Visibility: probe-per-call

`ListNodes` probes every node on every call and reports per-shard errors
instead of failing the listing — the visibility RPC exists precisely for when
something is wrong. `servable=false` on the listing is the same refusal
`Search` would give, spelled as a named reason (`mixed_calibration`,
`dimension_mismatch`, `node_unreachable`, … — see §5).

Readiness health works the same way: a coordinator reports serving only when
its shard table agrees, and a node only when its durable state validates. An
open port is not readiness.

## 5. Named failures are the contract

Refusals carry stable machine-readable names (`src/errors.rs`):
`mixed_calibration`, `dimension_mismatch`, `bit_width_mismatch`,
`node_unreachable`, `empty_collection`, `ambiguous_index`,
`positional_index_required`, `index_not_empty`, `labelled_index_immutable`,
`row_count_mismatch`, `invalid_calibration`. Clients may match on these
names; they are part of the wire contract and change only under the same
compatibility discipline as the protos (§7). New refusal conditions get new
names rather than overloading existing ones.

## 6. Topology mutation: Split/Join today, placement policy next

The only topology mutations are `Split` and `Join`. Both move **encoded
rows** under the source's own calibration pair — never vectors — so a
mutation cannot drift scores. Both fully validate and flush their targets
before publishing the new generation; a failed mutation leaves the serving
topology untouched.

**The autoscaled surface is a policy layer over exactly these mechanics, in
two stages:**

1. **Grow-only placement (next).** When a shard crosses a row-count or
   latency threshold, the coordinator stages a `Split` into spare-pool nodes
   it selected itself, publishes the new generation, and drains the source
   when the new shards certify. When capacity falls, a `Join` onto a spare.
   The operator knob becomes "pool size", not "shard list".
2. **Never automatic for live-shard rebalancing.** Moving rows off a
   *serving* shard for balance reasons is the dangerous version: it
   multiplies the states a query can straddle. Grow/shrink via Split/Join of
   quiesced sources only.

No new wire surface is needed for stage 1 beyond what exists: the policy is
coordinator-internal, and its effects are visible through the same
generation-stamped responses.

## 7. Wire compatibility policy

The public surface is two files: `proto/turbovec/v1/turbovec.proto` and
`proto/turbovec/v1/coordinator.proto`. Discipline:

- **Additive only by default.** New fields get new tag numbers; removed
  fields and message members leave `reserved` tags and names behind (see
  `ShardStatus`, `CollectionSearchRequest`, `CollectionSearchResponse` for
  the established style).
- **CI enforces it.** `scripts/check-proto-compat.sh` compiles the protos at
  HEAD and at the merge base and runs `buf breaking` over the two descriptor
  sets. A wire-breaking change fails the build.
- **Deliberate breaks are loud, not silent.** Pre-1.0, an intentional break
  is made by putting `[proto-breaking]` in the commit message, which turns
  the gate's failure into a printed report of every violation. The removal
  of the transitional `Documents` service was such a break. After a 1.0
  declaration the escape hatch closes.

## 8. The parity-shaped client (planned)

The Python client under `clients/python` will grow a class whose methods
mirror the embedded turbovec API one-to-one in capability — `create` /
`add` / `search` / `remove` / `calibrate` / `flush` — so numpy code written
against local turbovec reads the same against a collection. The mapping is
semantic, not cosmetic: `flush` publishes a fenced durable generation
server-side, which is what `write(path, durable=True)` means here; there is
no `load(path)` because restore is the node's startup contract. One client
surface, one node or one hundred behind it.
