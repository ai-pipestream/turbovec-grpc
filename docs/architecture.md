# Thin Coordinator Architecture

## Purpose

`turbovec-grpc` is a minimal sharded vector-search engine built around
`turbovec`. It keeps the engine library small and owns all distribution,
networking, coordination, and failure behavior in this sister repository.

The current system provides exact vector top-k search. BM25, hybrid fusion,
and embedding generation are future layers, not part of this contract yet.

## Components

```text
                         Collection client
                                |
                                | Coordinator.Search
                                v
                      turbovec-coordinator
                    global top-k heap per query
                       /        |        \
          StreamSearch         |         StreamSearch
             /                  |                  \
            v                   v                   v
      turbovec-grpc       turbovec-grpc       turbovec-grpc
       shard node          shard node          shard node
            |                   |                   |
            v                   v                   v
       TurboQuantIndex     TurboQuantIndex     TurboQuantIndex
```

- A node process serves handle-addressed turbovec indexes.
- The coordinator holds a static table of `(node address, index handle)`
  shards. It stores no vectors.
- Every shard in one collection must have the same dimension, bit width, and
  coordinate-for-coordinate TQ+ calibration.
- `ai-pipestream/turbovec:turbovec-pipestream-s13` supplies only the seedable
  floor and streaming scan primitives. Distributed behavior stays here.

## Complete search

1. The coordinator validates and pins the collection shape and calibration.
2. For each query, it opens one bidirectional `StreamSearch` RPC per shard.
3. Nodes emit candidates admitted by the inclusive floor in effect for each
   scan chunk. Nodes do not own a top-k heap.
4. The coordinator maintains the only global top-k heap.
5. Once that heap contains `k` candidates, its k-th score is broadcast as a
   monotonically rising floor to every unfinished shard.
6. The coordinator answers only after every shard returns
   `completed = true`.

The floor is lossless. The k-th score of the candidates observed so far is a
lower bound on the final global k-th score, so discarding candidates below it
cannot remove a true hit. Candidates equal to the floor remain eligible.

## Identity and redistribution

Search results carry node address, index handle, and local slot. Indexes made
by `Split` or `Join` also carry stable row labels, because slots change when a
row moves.

Redistribution moves packed codes, scales, labels, and calibration without
decoding or re-encoding. This preserves scores bit for bit. The current
`ExportRows` and `ImportRows` transport is unary and bounded by the gRPC frame
limit; converting it to bounded streaming is required before treating
resharding as large-index safe.

## Failure behavior

- A missing shard, incomplete stream, shape mismatch, or calibration mismatch
  fails a complete search.
- A short result is never presented as complete.
- `allow_partial` is an explicit compatibility path and currently uses the
  older unary shard-local top-k fan-out.
- Lower or duplicate floor updates are ignored. NaN floors are rejected.

## Scope boundary

The coordinator does not own a document store, corpus pipeline, analyzer, or
embedding runtime. Those systems may call this service later, but their data
model does not belong in the vector distribution layer.

The Python wrapper is also downstream of this architecture. The Rust gRPC
engine is completed and validated first.
