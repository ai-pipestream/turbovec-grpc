# Deploying turbovec-grpc

What the server is: a single process holding quantized vector indexes in
memory, addressed by `index_id` handles. Handles are ephemeral — they do not
survive a restart, and there is no built-in replication or clustering. That
shape is deliberate; this document covers running it well within that shape.

## Listen address

`TURBOVEC_GRPC_ADDR` sets the bind address (default `0.0.0.0:50051`). The
default assumes a container or pod network. On a shared host, bind
`127.0.0.1:50051` and put a proxy in front, because of what comes next.

## TLS and authentication: none, by design

The server speaks plaintext HTTP/2 and authenticates nothing. Any client that
can reach the port can create indexes, read every handle, and call `Snapshot`
and `Load` with server-local paths. Treat the port as trusted-network-only and
terminate TLS/authz in front of it — envoy, nginx, traefik, or a service mesh
all speak gRPC and can add mTLS, JWT checks, and per-tenant routing without
the server knowing about it. This is a boundary, not an omission: embedding
auth here would just be a worse version of what those proxies already do.

## Health checking and reflection

The server registers the standard `grpc.health.v1.Health` service, reporting
`SERVING` for both the overall server and `turbovec.v1.TurboVec` specifically.
Kubernetes probes can use it directly:

```yaml
livenessProbe:
  grpc:
    port: 50051
readinessProbe:
  grpc:
    port: 50051
    service: turbovec.v1.TurboVec
```

(or `grpc_health_probe` / `grpcurl` in an exec probe on older clusters.)

gRPC server reflection is also on, so `grpcurl -plaintext host:50051 list`
and similar tooling work without a local copy of the proto. If you would
rather not expose the schema, that is a one-line removal in `src/main.rs`.

## State and persistence

Indexes live only in process memory. `Snapshot` writes an index to a
server-local path in turbovec's own format and `Load` reads it back as a new
handle — in a container those paths must be on a mounted volume to mean
anything across restarts. There is no automatic snapshotting; if durability
matters, drive `Snapshot` from your own scheduler and `Load` at startup.

Memory per index is roughly the quantized payload: `dim * bit_width / 8`
bytes per vector, plus scales and (for id-mapped indexes) the id tables. A
million 1536-dim vectors at 4-bit is on the order of 1 GB.

## Request sizing

Single messages are capped at 256 MB in both directions. Bulk ingest should
still go through the client-streaming `Add` in frames of a few MB — one
enormous frame works but defeats the point of streaming, and intermediaries
(envoy included) often default to a 4 MB message limit that the server and
clients must then be configured to match.

## CPU

Search and encode are CPU-bound SIMD work running on tokio's blocking pool.
The crate targets x86-64-v3 (AVX2, Haswell-or-newer) on x86_64 and dispatches
an AVX-512 kernel at runtime where available; aarch64 uses NEON. There is no
GPU path and no internal concurrency limit — concurrency is naturally bounded
by core count, so size CPU requests/limits accordingly.

## Docker

An [example Dockerfile](../Dockerfile) builds a minimal image (multi-stage,
Debian slim runtime). It is a starting point — it runs as root and sets no
limits or health checks of its own:

```bash
docker build -f turbovec-grpc/Dockerfile -t turbovec-grpc .
docker run --rm -p 50051:50051 turbovec-grpc
```
