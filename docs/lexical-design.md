# Lexical Retrieval Design Note

Design only. Nothing in this document is implemented in `turbovec-grpc`,
and none of it should be implemented until the vector product boundary is
stable and profiling motivates the work. Its purpose is to fix the contract
before the first line of code, distilled from what `turbovec-search` built,
measured, and in some cases measured *against*, so that the small product
does not rediscover the expensive lessons.

`turbovec-search` is the proving ground, not the template. It carries a
document platform (WAL, columns, facets, CEL filters, corpus pipelines)
that this repository must not inherit. What transfers is the scoring
contract and four mechanisms.

## The contract: exact, or refused

The rule this repository already enforces for vectors extends unchanged to
lexical scoring: a distributed result must equal the same monolithic
computation, score for score, and anything that cannot promise that fails
by name rather than degrading. Concretely:

- One BM25 score exists per (query, document) pair, computed from global
  statistics. Two shards must never disagree about what a term is worth.
- A partial result set is never returned as if it were complete. A shard
  that cannot answer fails the query.
- Analysis identity is part of the contract. A query analyzed differently
  from the corpus is an error, not a degraded search.

## Mechanism 1: global statistics with an epoch

The one unforgivable shortcut is scoring each shard with its own document
frequencies and average length and merging the results as if they were
comparable. They are not on one scale; the merge ranks nothing. This is a
measured negative in turbovec-search, recorded as such, and it is the
lexical twin of this repository's refusal to merge mixed calibrations.

The design that works:

- The coordinator owns the corpus-wide statistics: per-term document
  frequency, total document count, average field length.
- Statistics carry an **epoch**, bumped whenever ingest changes them. Every
  scoring request names the epoch it was planned under; a shard serving a
  different epoch refuses, the coordinator refreshes and retries. This is
  the same shape as `topology_generation` and the calibration agreement
  check: agreement is structural, not assumed.
- Two-phase query: phase one resolves query terms to global statistics
  (one round, cacheable, invalidated by epoch), phase two fans out scoring
  requests that carry the global values with them. Shards apply statistics;
  they never compute them.

The analogy to hold on to: **global lexical statistics are to BM25 what the
shared TQ+ calibration pair is to vector scores.** Same invariant, same
failure mode when violated, same remedy (read back and compare, refuse on
disagreement).

## Mechanism 2: block-max postings with the occurrence split

turbovec-search's postings layout landed in three measured stages, and the
ordering matters because each stage's win was attributed before the next
was built:

1. **Occurrence split** — postings (doc id, term frequency) separate from
   occurrence positions, so scoring never touches position data until a
   document survives the top-k. Measured: the dominant cost was per-posting
   allocation, 162 ms to 40 ms on the worst high-df query.
2. **Block-max impacts** — per-block score upper bounds, so a scan holding
   a floor skips whole blocks that cannot beat it. Measured: 184 ms to
   3.8 ms on a mixed-shape query. This is the mechanism that converts a
   floor into bytes never read; floors alone only skip heap insertions.
3. **MaxScore partition with level-1 skips** — essential/non-essential term
   partition inside competitive windows. Measured: full evaluations 644k to
   1.5k at k=10.

Exactness held at every stage against an exhaustive oracle, with one wire
rule worth stealing verbatim: the floor a shard emits is its k-th best
score **one f32 ULP down**, so boundary ties survive quantization and a
seeded run equals the unseeded run filtered to that floor, bit for bit.

For turbovec-grpc this means: if lexical lands, it lands with the split
and block-max from day one. The staged history proves the layout; it does
not need to be re-proven here in stages.

## Mechanism 3: seeded floors

`min_score` on the scoring request, `kth_best` on the response. A caller
holding a floor (a previous identical query, a coordinator merging as
results arrive) seeds every shard, and block-max turns that seed into
skipped blocks. Cheap, unary, exact by the lower-bound argument: a proven
global lower bound can only exclude candidates the merge would discard.

This is part of the v1 lexical contract if lexical lands at all: the
fields cost nothing, and retrofitting them changes the wire.

## Mechanism 4: the mid-query floor relay — measured, not assumed

turbovec-search now has both protocols side by side on the same fleet, so
this decision is informed by numbers rather than symmetry with the vector
path (measured 2026-08, v9 court fleet, 8 shards on one host, 36 queries):

- k=10: relay indistinguishable from unary (p50 61 vs 64 ms, tails within
  noise).
- k=100: p50 flat (71 ms both ways); tail improved, bm25 p90 262 → 231 ms,
  max 360 → 298 ms.

The relay is a tail mechanism: it converts sibling progress into skipped
blocks on whichever shard is slowest, so it pays in proportion to scan
length, shard skew, and real network fan-out, and it pays nothing at p50.

Decision for turbovec-grpc: **the v1 lexical contract carries seeded
floors only.** The bidi relay is not in v1 — it is more protocol surface
(a streaming RPC, conflation, completion certificates) than a single-digit
tail win justifies before there is a measured multi-host lexical
deployment at all. The contract must leave room for it: the scoring RPC's
request message reserves a floor-update payload shape (the vector side's
`StreamSearch` is the template), and nothing in v1 may assume a shard's
floor is fixed for the life of a scan.

## Concurrency discipline

A lesson that cost a measurement to find: turbovec-search ran lexical
scoring inline on runtime threads while every vector scan wrapped in
`spawn_blocking`, and the asymmetry showed up as throughput collapse under
concurrency (p90 136 → 92 ms from the fix alone), invisible to any
single-query benchmark. In this repository CPU-bound scoring goes through
`spawn_blocking` from the first commit, matching how `Search` already
offloads the engine scan.

## What is deliberately out

- Shard-local statistics in any form, including "temporarily". Measured
  negative, recorded in the workspace rules.
- Document storage, analysis pipelines, facets, filters, WAL. The sidecar
  (`grpc-opennlp-analysis`) owns term identity when analysis enters the
  picture; this repository would own only term-indexed scoring.
- Fusion of vector and lexical scores. That is a contract of its own
  (global rank fusion is the only turbovec-search mode that reproduces
  the monolithic result exactly) and belongs to a later design note once
  lexical retrieval exists to fuse.

## Order of work, when the time comes

1. Global-statistics service with epochs on the coordinator; refuse on
   epoch mismatch from the first commit.
2. Postings with the occurrence split and block-max, exhaustive oracle
   and property gate alongside.
3. Seeded floors on the wire (`min_score` / `kth_best`, ULP-down rule).
4. Only after multi-host profiles exist: revisit the floor relay with the
   turbovec-search measurement as the baseline to beat.
