//! Rust client example for the turbovec gRPC server.
//!
//! Shows the full client flow against a running server:
//!   1. Connect to the server (`TURBOVEC_GRPC_ADDR`, default 127.0.0.1:50051).
//!   2. Create an ID_MAP index (bit width 4).
//!   3. Ingest 20,000 vectors over the client-streaming `Add` RPC, chunked
//!      into frames well under the 4 MB gRPC message cap, and report the
//!      ingest wall time and vectors/sec.
//!   4. Run 500 unary top-10 searches and report QPS plus p50/p95/p99 latency.
//!   5. Run one `SearchStream` batch of 4 queries and print per-query results.
//!
//! All client types (`TurboVecClient` and the request/response messages) come
//! from the `turbovec-grpc` crate itself, generated from
//! `proto/turbovec/v1/turbovec.proto` at build time — a Rust consumer needs
//! no protoc or codegen of its own, just the crate as a dependency.
//!
//! Run it:
//!   # start the server first (plaintext, loopback):
//!   TURBOVEC_GRPC_ADDR=127.0.0.1:50051 ./target/debug/turbovec-grpc &
//!   # then, from the repo root:
//!   cargo run -p turbovec-grpc --example rust-client
//!
//! Optional arguments: `rust-client [vectors] [dim] [queries]` — e.g.
//! `cargo run -p turbovec-grpc --example rust-client -- 50000 256 1000`.
//! The defaults finish in well under a minute.

use std::time::{Duration, Instant};

use tokio_stream::StreamExt;
use tonic::transport::Endpoint;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{AddRequest, CreateIndexRequest, IndexKind, SearchRequest};

/// Deterministic pseudo-random-looking vector, so the example needs no RNG
/// dependency and results are reproducible run to run.
fn vector(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| ((i.wrapping_mul(31).wrapping_add(seed)) as f32 * 0.013) % 2.0 - 1.0)
        .collect()
}

fn percentile(sorted: &[Duration], p: usize) -> Duration {
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().as_deref().unwrap_or("20000").parse()?;
    let dim: usize = args.next().as_deref().unwrap_or("128").parse()?;
    let queries: usize = args.next().as_deref().unwrap_or("500").parse()?;

    // Connect: the address is a bare host:port, tonic wants a URI.
    let addr = std::env::var("TURBOVEC_GRPC_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let channel = Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let mut client = TurboVecClient::new(channel);
    println!("connected to {addr}");

    // 1. Create an ID_MAP index with 4-bit quantization.
    let created = client
        .create_index(CreateIndexRequest {
            dim: dim as u32,
            bit_width: 4,
            kind: IndexKind::IdMap as i32,
            lazy: false,
        })
        .await?
        .into_inner();
    let index_id = created.index_id;
    println!("created ID_MAP index {index_id} (dim {dim}, bit_width 4)");

    // 2. Client-streaming add: 1024 vectors per frame = 512 KB at dim 128,
    // well under tonic's default 4 MB message limit.
    let chunk = 1024usize;
    let start = Instant::now();
    // Collect the frames up front: tonic's streaming request must be 'static,
    // so a lazy iterator borrowing `index_id` will not do.
    let frames: Vec<AddRequest> = (0..n)
        .step_by(chunk)
        .map(|base| {
            let rows = chunk.min(n - base);
            let mut vectors = Vec::with_capacity(rows * dim);
            for s in base..base + rows {
                vectors.extend(vector(s, dim));
            }
            AddRequest {
                index_id: index_id.clone(),
                dim: dim as u32,
                vectors,
                ids: (base..base + rows).map(|i| i as u64).collect(),
            }
        })
        .collect();
    let added = client.add(tokio_stream::iter(frames)).await?.into_inner();
    let ingest = start.elapsed();
    println!(
        "ingested {} vectors in {:.2?} ({:.0} vectors/sec)",
        added.added,
        ingest,
        added.added as f64 / ingest.as_secs_f64()
    );

    // 3. Unary top-10 searches; collect per-call latency.
    let mut latencies = Vec::with_capacity(queries);
    let start = Instant::now();
    for q in 0..queries {
        let t = Instant::now();
        client
            .search(SearchRequest {
                index_id: index_id.clone(),
                queries: vector(q % n, dim),
                k: 10,
                allowlist: vec![],
            })
            .await?;
        latencies.push(t.elapsed());
    }
    let search_wall = start.elapsed();
    latencies.sort();
    println!(
        "search: {:.0} QPS over {} queries | p50 {:.2?} p95 {:.2?} p99 {:.2?}",
        queries as f64 / search_wall.as_secs_f64(),
        queries,
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
    );

    // 4. Server-streaming search: one batch of 4 queries, one QueryResult
    // streamed back per query, in order.
    let batch: Vec<f32> = (0..4).flat_map(|q| vector(q * 7 + 1, dim)).collect();
    let mut stream = client
        .search_stream(SearchRequest {
            index_id: index_id.clone(),
            queries: batch,
            k: 10,
            allowlist: vec![],
        })
        .await?
        .into_inner();
    let mut q = 0;
    while let Some(result) = stream.next().await {
        let result = result?;
        let best = result.scores.first().copied().unwrap_or(f32::NAN);
        println!("search_stream query {q}: {} neighbours, best score {best:.4}", result.ids.len());
        q += 1;
    }

    // 5. Release the handle.
    client
        .drop_index(turbovec_grpc::proto::DropIndexRequest { index_id })
        .await?;
    println!("done");
    Ok(())
}
