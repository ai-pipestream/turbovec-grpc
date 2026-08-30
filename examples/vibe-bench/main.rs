//! VIBE benchmark harness for turbovec, in-process and over gRPC.
//!
//! VIBE (<https://github.com/vector-index-bench/vibe>, arXiv 2505.17810) is
//! an ann-benchmarks-derived ANN benchmark with precomputed HDF5 datasets.
//! This harness downloads one, builds or fills a turbovec index with its
//! train rows, runs its test queries, and reports recall against the
//! published ground truth plus single-query latency and QPS.
//!
//! Modes:
//! - `local`: a `turbovec::TurboQuantIndex` built in this process.
//! - `node`: a positional index on one running `turbovec-grpc` node.
//! - `coordinator`: a one-shard collection served by a running
//!   `turbovec-coordinator`; rows are added to the shard's node directly
//!   (the coordinator has no add path) and searched through the
//!   coordinator's exact merge.
//!
//! Calibration always comes first, then every row is added, then search
//! starts: a row's codes are a function of the row and the calibration
//! pair, so this order keeps the benchmarked index identical to the
//! calibrate-then-fill contract the engine documents. Ingest uses plain
//! adds on purpose; the retry-safe envelope persists a durable generation
//! per operation and conflicts with pinned-generation topologies. The
//! autoscaler must stay off for the same reason: a split shard refuses
//! further adds.
//!
//! Run it:
//!   cargo run --release --example vibe-bench -- --dataset yi-128-ip
//!   cargo run --release --example vibe-bench -- --dataset yi-128-ip \
//!       --mode node --node-addr 127.0.0.1:50051
//!
//! See benchmarks/README.md for the full guide.

mod dataset;
mod report;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use tonic::transport::Endpoint;
use turbovec::TurboQuantIndex;
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::turbo_vec_admin_client::TurboVecAdminClient;
use turbovec_grpc::proto::turbo_vec_query_client::TurboVecQueryClient;
use turbovec_grpc::proto::{
    AddRequest, CollectionSearchRequest, CreateIndexRequest, DropIndexRequest,
    FitCalibrationRequest, IndexKind, ListIndexesRequest, ListNodesRequest, SearchRequest,
    SetCalibrationRequest,
};

/// Quantization bit width every run builds at.
const BIT_WIDTH: usize = 4;
/// Untimed warmup queries per run: the first search after ingest pays the
/// engine's one-time cache build, which belongs outside the timed window.
const WARMUP_QUERIES: usize = 20;
/// Floats per client-streaming Add frame: 4 MiB of vector data, well under
/// the server's default 16 MiB message cap and its per-operation
/// coordinate bound.
const ADD_FRAME_FLOATS: usize = 1 << 20;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Local,
    Node,
    Coordinator,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Local => "local",
            Mode::Node => "node",
            Mode::Coordinator => "coordinator",
        }
    }
}

struct Args {
    dataset: String,
    cache_dir: PathBuf,
    mode: Mode,
    node_addr: String,
    coordinator_addr: String,
    k: usize,
    max_train: Option<usize>,
    max_queries: Option<usize>,
    calibration_sample: usize,
    /// Coordinator mode only: create the empty shard index on `--node-addr`
    /// when the node holds none, so an ephemeral local fleet needs no
    /// separate setup step.
    provision: bool,
    out: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!(
        "usage: vibe-bench --dataset NAME [options]

  --dataset NAME           VIBE dataset name (required), e.g. yi-128-ip
  --cache-dir PATH         dataset cache (default ~/.cache/turbovec-vibe)
  --mode local|node|coordinator   benchmark mode (default local)
  --node-addr HOST:PORT    turbovec-grpc node (default 127.0.0.1:50051)
  --coordinator-addr HOST:PORT    coordinator (default 127.0.0.1:50050)
  --k N                    headline recall depth (default 10)
  --max-train N            index only the first N train rows; queries whose
                           full ground truth reaches past N are dropped
  --max-queries M          run at most M queries (after any drops)
  --calibration-sample N   rows in the calibration sample (default 25000)
  --provision              coordinator mode: create the empty shard index on
                           --node-addr when the node holds none
  --out FILE               write the JSON report to FILE instead of stdout"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
    let mut args = Args {
        dataset: String::new(),
        cache_dir: PathBuf::from(home).join(".cache/turbovec-vibe"),
        mode: Mode::Local,
        node_addr: "127.0.0.1:50051".into(),
        coordinator_addr: "127.0.0.1:50050".into(),
        k: 10,
        max_train: None,
        max_queries: None,
        calibration_sample: 25_000,
        provision: false,
        out: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = |flag: &str| -> Result<String, Box<dyn std::error::Error>> {
            it.next()
                .ok_or_else(|| format!("{flag} needs a value").into())
        };
        match arg.as_str() {
            "--dataset" => args.dataset = value("--dataset")?,
            "--cache-dir" => args.cache_dir = PathBuf::from(value("--cache-dir")?),
            "--mode" => {
                args.mode = match value("--mode")?.as_str() {
                    "local" => Mode::Local,
                    "node" => Mode::Node,
                    "coordinator" => Mode::Coordinator,
                    other => return Err(format!("unknown --mode {other:?}").into()),
                }
            }
            "--node-addr" => args.node_addr = value("--node-addr")?,
            "--coordinator-addr" => args.coordinator_addr = value("--coordinator-addr")?,
            "--k" => args.k = value("--k")?.parse()?,
            "--max-train" => args.max_train = Some(value("--max-train")?.parse()?),
            "--max-queries" => args.max_queries = Some(value("--max-queries")?.parse()?),
            "--calibration-sample" => {
                args.calibration_sample = value("--calibration-sample")?.parse()?
            }
            "--out" => args.out = Some(PathBuf::from(value("--out")?)),
            "--provision" => args.provision = true,
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown argument {other:?}");
                usage();
            }
        }
    }
    if args.dataset.is_empty() {
        usage();
    }
    if args.k == 0 {
        return Err("--k must be at least 1".into());
    }
    Ok(args)
}

/// Deterministic, evenly spaced calibration sample: `want` rows drawn at a
/// regular stride across the train set. A stride draw avoids clustering
/// bias without needing an RNG dependency.
fn calibration_sample(train: &[f32], n_train: usize, dim: usize, want: usize) -> Vec<f32> {
    let take = want.min(n_train);
    let mut sample = Vec::with_capacity(take * dim);
    for i in 0..take {
        let row = i * n_train / take;
        sample.extend_from_slice(&train[row * dim..(row + 1) * dim]);
    }
    sample
}

/// Apply `--max-train` / `--max-queries` subsetting in place, returning
/// the number of queries dropped by `--max-train`.
///
/// Truncating the train set invalidates any ground-truth entry that names
/// a row past the cut, so every query whose full published ground truth
/// (all `gt_depth` entries, not only the top-k) reaches past the cut is
/// dropped; the drop count is reported. That keeps every reported recall
/// depth honest.
fn subset(ds: &mut dataset::Dataset, args: &Args) -> usize {
    let mut dropped = 0usize;
    if let Some(max_train) = args.max_train {
        let n = max_train.min(ds.n_train);
        ds.train.truncate(n * ds.dim);
        ds.n_train = n;
        let cut = n as i64;
        let mut kept_queries = Vec::with_capacity(ds.queries.len());
        let mut kept_neighbors = Vec::with_capacity(ds.neighbors.len());
        for q in 0..ds.queries.len() / ds.dim {
            let gt = &ds.neighbors[q * ds.gt_depth..(q + 1) * ds.gt_depth];
            if gt.iter().any(|&row| row >= cut) {
                dropped += 1;
                continue;
            }
            kept_queries.extend_from_slice(&ds.queries[q * ds.dim..(q + 1) * ds.dim]);
            kept_neighbors.extend_from_slice(gt);
        }
        ds.queries = kept_queries;
        ds.neighbors = kept_neighbors;
        if dropped > 0 {
            println!(
                "--max-train {n}: dropped {dropped} queries whose ground truth reaches past the cut"
            );
        }
    }
    if let Some(max_queries) = args.max_queries {
        let m = max_queries.min(ds.queries.len() / ds.dim);
        ds.queries.truncate(m * ds.dim);
        ds.neighbors.truncate(m * ds.gt_depth);
    }
    dropped
}

/// Recall depths to report, ascending: always 1, then k, then the
/// published ground-truth depth (100) when distinct.
fn recall_depths(k: usize, gt_depth: usize) -> Vec<usize> {
    let mut depths = vec![1, k, gt_depth];
    depths.sort_unstable();
    depths.dedup();
    depths
}

/// Raw measurements from one mode's run, before scoring.
struct RawRun {
    /// Returned neighbour ids per query.
    returned: Vec<Vec<u64>>,
    /// Per-query wall latencies, unsorted.
    latencies: Vec<Duration>,
    /// Wall time of the timed search loop.
    search_wall: Duration,
    /// Ingest wall time, when the run ingested.
    ingest: Option<Duration>,
    /// Calibration wall time, when the run calibrated.
    calibration: Option<Duration>,
}

/// Score every query against the ground truth and pack the report.
fn score(args: &Args, ds: &dataset::Dataset, raw: RawRun) -> report::BenchResult {
    let n_queries = ds.queries.len() / ds.dim;
    assert_eq!(raw.returned.len(), n_queries, "one result row per query");
    let fetch = args.k.max(ds.gt_depth);
    let recalls = recall_depths(args.k.min(fetch), ds.gt_depth.min(fetch))
        .into_iter()
        .map(|r| {
            (
                r,
                report::recall_at(&raw.returned, &ds.neighbors, ds.gt_depth, r),
            )
        })
        .collect();
    report::BenchResult {
        fetch,
        recalls,
        latencies: raw.latencies,
        search_wall: raw.search_wall,
        ingest: raw.ingest,
        calibration: raw.calibration,
        rows: ds.n_train,
        dropped_queries: 0,
    }
}

/// Mode `local`: build a `TurboQuantIndex` in this process.
fn run_local(
    args: &Args,
    ds: &dataset::Dataset,
) -> Result<report::BenchResult, Box<dyn std::error::Error>> {
    let dim = ds.dim;
    let mut index = TurboQuantIndex::new(dim, BIT_WIDTH)?;

    let sample = calibration_sample(&ds.train, ds.n_train, dim, args.calibration_sample);
    let t = Instant::now();
    index.calibrate_2d(&sample, dim)?;
    let calibration = t.elapsed();

    let t = Instant::now();
    index.add(&ds.train);
    let ingest = t.elapsed();
    index.prepare();
    println!(
        "indexed {} rows in {:.2?} ({:.0} rows/s)",
        ds.n_train,
        ingest,
        ds.n_train as f64 / ingest.as_secs_f64()
    );

    let n_queries = ds.queries.len() / dim;
    let fetch = args.k.max(ds.gt_depth);
    for q in 0..WARMUP_QUERIES.min(n_queries) {
        index.search(&ds.queries[q * dim..(q + 1) * dim], fetch);
    }
    let mut returned = Vec::with_capacity(n_queries);
    let mut latencies = Vec::with_capacity(n_queries);
    let wall = Instant::now();
    for q in 0..n_queries {
        let t = Instant::now();
        let results = index.search(&ds.queries[q * dim..(q + 1) * dim], fetch);
        latencies.push(t.elapsed());
        returned.push(
            results
                .indices_for_query(0)
                .iter()
                .map(|&i| i as u64)
                .collect(),
        );
    }
    let search_wall = wall.elapsed();
    Ok(score(
        args,
        ds,
        RawRun {
            returned,
            latencies,
            search_wall,
            ingest: Some(ingest),
            calibration: Some(calibration),
        },
    ))
}

/// A TQ+ calibration pair fitted in-process, plus the fit's wall time.
struct FittedPair {
    shift: Vec<f32>,
    scale: Vec<f32>,
    fit_time: Duration,
}

/// Fit the calibration pair in-process. turbovec's fit is deterministic,
/// and the node API commits a pair fitted elsewhere (`SetCalibration`) or
/// the coordinator fits and broadcasts one (`FitCalibration`); both start
/// from this sample fit.
fn fit_pair(
    ds: &dataset::Dataset,
    sample_rows: usize,
) -> Result<FittedPair, Box<dyn std::error::Error>> {
    let sample = calibration_sample(&ds.train, ds.n_train, ds.dim, sample_rows);
    let mut fitting = TurboQuantIndex::new(ds.dim, BIT_WIDTH)?;
    let t = Instant::now();
    fitting.calibrate_2d(&sample, ds.dim)?;
    let fit_time = t.elapsed();
    Ok(FittedPair {
        shift: fitting.tqplus_shift().to_vec(),
        scale: fitting.tqplus_scale().to_vec(),
        fit_time,
    })
}

/// Plain-add ingest of every train row over the client-streaming Add RPC,
/// in `ADD_FRAME_FLOATS`-sized frames. Plain adds, not the retry-safe
/// envelope: see the module docs.
async fn ingest_node(
    admin: &mut TurboVecAdminClient<tonic::transport::Channel>,
    index_id: &str,
    ds: &dataset::Dataset,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let rows_per_frame = (ADD_FRAME_FLOATS / ds.dim).max(1);
    let t = Instant::now();
    let mut base = 0usize;
    while base < ds.n_train {
        let rows = rows_per_frame.min(ds.n_train - base);
        let frame = AddRequest {
            index_id: index_id.to_string(),
            dim: ds.dim as u32,
            vectors: ds.train[base * ds.dim..(base + rows) * ds.dim].to_vec(),
            ..Default::default()
        };
        admin.add(tokio_stream::iter(vec![frame])).await?;
        base += rows;
        if base % (rows_per_frame * 16) < rows_per_frame {
            println!("  ingested {base}/{} rows", ds.n_train);
        }
    }
    Ok(t.elapsed())
}

/// Timed unary searches, one RPC per query, returning the ids per query.
async fn search_node(
    query: &mut TurboVecQueryClient<tonic::transport::Channel>,
    index_id: &str,
    ds: &dataset::Dataset,
    fetch: usize,
) -> Result<(Vec<Vec<u64>>, Vec<Duration>, Duration), Box<dyn std::error::Error>> {
    let n_queries = ds.queries.len() / ds.dim;
    for q in 0..WARMUP_QUERIES.min(n_queries) {
        query
            .search(SearchRequest {
                index_id: index_id.to_string(),
                queries: ds.queries[q * ds.dim..(q + 1) * ds.dim].to_vec(),
                k: fetch as u32,
                allowlist: vec![],
            })
            .await?;
    }
    let mut returned = Vec::with_capacity(n_queries);
    let mut latencies = Vec::with_capacity(n_queries);
    let wall = Instant::now();
    for q in 0..n_queries {
        let request = SearchRequest {
            index_id: index_id.to_string(),
            queries: ds.queries[q * ds.dim..(q + 1) * ds.dim].to_vec(),
            k: fetch as u32,
            allowlist: vec![],
        };
        let t = Instant::now();
        let response = query.search(request).await?.into_inner();
        latencies.push(t.elapsed());
        let result = response
            .results
            .into_iter()
            .next()
            .ok_or("search returned no result row")?;
        returned.push(result.ids);
    }
    Ok((returned, latencies, wall.elapsed()))
}

/// Mode `node`: create, calibrate and fill a positional index on one
/// running node, then search it.
async fn run_node(
    args: &Args,
    ds: &dataset::Dataset,
) -> Result<report::BenchResult, Box<dyn std::error::Error>> {
    let channel = Endpoint::from_shared(format!("http://{}", args.node_addr))?
        .connect()
        .await?;
    let mut admin = TurboVecAdminClient::new(channel.clone());
    let mut query = TurboVecQueryClient::new(channel);
    println!("connected to node {}", args.node_addr);

    let created = admin
        .create_index(CreateIndexRequest {
            dim: ds.dim as u32,
            bit_width: BIT_WIDTH as u32,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await?
        .into_inner();
    let index_id = created.index_id;
    println!("created positional index {index_id}");

    let result = async {
        let pair = fit_pair(ds, args.calibration_sample)?;
        admin
            .set_calibration(SetCalibrationRequest {
                index_id: index_id.clone(),
                tqplus_shift: pair.shift,
                tqplus_scale: pair.scale,
            })
            .await?;
        println!("calibration committed ({:.2?} fit)", pair.fit_time);

        let ingest = ingest_node(&mut admin, &index_id, ds).await?;
        println!(
            "ingested {} rows in {:.2?} ({:.0} rows/s)",
            ds.n_train,
            ingest,
            ds.n_train as f64 / ingest.as_secs_f64()
        );

        let fetch = args.k.max(ds.gt_depth);
        let (returned, latencies, search_wall) =
            search_node(&mut query, &index_id, ds, fetch).await?;
        Ok::<_, Box<dyn std::error::Error>>(score(
            args,
            ds,
            RawRun {
                returned,
                latencies,
                search_wall,
                ingest: Some(ingest),
                calibration: Some(pair.fit_time),
            },
        ))
    }
    .await;

    admin
        .drop_index(DropIndexRequest {
            index_id: index_id.clone(),
        })
        .await?;
    result
}

/// Mode `coordinator`: fill the collection's single shard directly on its
/// node, then search through the coordinator's exact merge.
///
/// The coordinator has no add path, and its topology is operator-pinned at
/// startup, so this mode expects a provisioned fleet: one node holding one
/// empty positional index that the coordinator's node table names
/// (benchmarks/README.md shows the two-terminal local setup and the
/// compose-demo equivalent). With `--provision` the harness creates that
/// empty index on `--node-addr` itself when the node holds none, which is
/// enough for an ephemeral local fleet. A multi-shard or populated
/// collection is refused rather than silently benchmarked stale.
async fn run_coordinator(
    args: &Args,
    ds: &dataset::Dataset,
) -> Result<report::BenchResult, Box<dyn std::error::Error>> {
    // Optional self-provisioning for ephemeral fleets: the coordinator
    // resolves a nameless node-table entry to the node's sole index at bind
    // time, so creating that index now, before the first coordinator call,
    // is enough. Durable fleets pin index id and generation and must be
    // provisioned externally (see benchmarks/README.md).
    if args.provision {
        let channel = Endpoint::from_shared(format!("http://{}", args.node_addr))?
            .connect()
            .await?;
        let mut admin = TurboVecAdminClient::new(channel.clone());
        let mut query = TurboVecQueryClient::new(channel);
        let existing = query
            .list_indexes(ListIndexesRequest {})
            .await?
            .into_inner()
            .indexes;
        match existing.len() {
            0 => {
                let created = admin
                    .create_index(CreateIndexRequest {
                        dim: ds.dim as u32,
                        bit_width: BIT_WIDTH as u32,
                        kind: IndexKind::Positional as i32,
                        lazy: false,
                    })
                    .await?
                    .into_inner();
                println!(
                    "provisioned positional index {} on {}",
                    created.index_id, args.node_addr
                );
            }
            1 => println!(
                "reusing existing index {} on {}",
                existing[0].index_id, args.node_addr
            ),
            n => {
                return Err(format!(
                    "--provision expects the node to hold at most one index; it holds {n}"
                )
                .into())
            }
        }
    }

    let channel = Endpoint::from_shared(format!("http://{}", args.coordinator_addr))?
        .connect()
        .await?;
    let mut coordinator = CoordinatorClient::new(channel);
    println!("connected to coordinator {}", args.coordinator_addr);

    let listing = coordinator
        .list_nodes(ListNodesRequest {})
        .await?
        .into_inner();
    if listing.shards.len() != 1 {
        return Err(format!(
            "coordinator mode expects a one-shard collection before ingest; found {} shards \
             (a split collection is labelled and refuses adds)",
            listing.shards.len()
        )
        .into());
    }
    let shard = &listing.shards[0];
    let shard_ref = shard
        .shard
        .as_ref()
        .ok_or("ListNodes returned a shard with no ref")?;
    let node_addr = shard_ref.address.clone();
    let info = shard
        .info
        .as_ref()
        .ok_or_else(|| format!("shard node {node_addr} is unreachable: {}", shard.error))?;
    if info.len != 0 {
        return Err(format!(
            "shard {} on {node_addr} already holds {} rows; coordinator mode ingests only into \
             an empty collection",
            shard_ref.index_id, info.len
        )
        .into());
    }
    if info.dim != ds.dim as u32 || info.bit_width != BIT_WIDTH as u32 {
        return Err(format!(
            "shard index is dim {} bit width {}; dataset {} needs dim {} bit width {BIT_WIDTH}",
            info.dim, info.bit_width, ds.name, ds.dim
        )
        .into());
    }
    let index_id = info.index_id.clone();
    println!("collection shard: {index_id} on {node_addr}");

    // Calibrate through the coordinator: it fits one pair from the sample
    // and commits it to every shard (here: the one).
    let sample = calibration_sample(&ds.train, ds.n_train, ds.dim, args.calibration_sample);
    let t = Instant::now();
    coordinator
        .fit_calibration(FitCalibrationRequest {
            sample,
            dim: ds.dim as u32,
            bit_width: BIT_WIDTH as u32,
        })
        .await?;
    let calibration = t.elapsed();
    println!("calibration fitted and committed ({calibration:.2?})");

    // Ingest goes to the shard's node directly; the coordinator only ever
    // serves reads.
    let node_channel = Endpoint::from_shared(node_addr.clone())?.connect().await?;
    let mut admin = TurboVecAdminClient::new(node_channel.clone());
    let ingest = ingest_node(&mut admin, &index_id, ds).await?;
    println!(
        "ingested {} rows in {:.2?} ({:.0} rows/s)",
        ds.n_train,
        ingest,
        ds.n_train as f64 / ingest.as_secs_f64()
    );

    let n_queries = ds.queries.len() / ds.dim;
    let fetch = args.k.max(ds.gt_depth);
    for q in 0..WARMUP_QUERIES.min(n_queries) {
        coordinator
            .search(CollectionSearchRequest {
                queries: ds.queries[q * ds.dim..(q + 1) * ds.dim].to_vec(),
                k: fetch as u32,
                ..Default::default()
            })
            .await?;
    }
    let mut returned = Vec::with_capacity(n_queries);
    let mut latencies = Vec::with_capacity(n_queries);
    let wall = Instant::now();
    for q in 0..n_queries {
        let request = CollectionSearchRequest {
            queries: ds.queries[q * ds.dim..(q + 1) * ds.dim].to_vec(),
            k: fetch as u32,
            ..Default::default()
        };
        let t = Instant::now();
        let response = coordinator.search(request).await?.into_inner();
        latencies.push(t.elapsed());
        let result = response
            .results
            .into_iter()
            .next()
            .ok_or("search returned no result row")?;
        // A label is the row's stable external id; an unlabelled shard
        // falls back to the slot, which is the train row index here
        // because the harness ingested the rows in order.
        returned.push(
            result
                .neighbours
                .iter()
                .map(|n| n.label.unwrap_or(n.slot))
                .collect(),
        );
    }
    let search_wall = wall.elapsed();
    Ok(score(
        args,
        ds,
        RawRun {
            returned,
            latencies,
            search_wall,
            ingest: Some(ingest),
            calibration: Some(calibration),
        },
    ))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let path = dataset::ensure_cached(&args.dataset, &args.cache_dir)?;
    let mut ds = dataset::load(&args.dataset, &path)?;
    println!(
        "dataset {}: {} x {} train, {} queries, distance {}, ground truth depth {}",
        ds.name,
        ds.n_train,
        ds.dim,
        ds.queries.len() / ds.dim,
        ds.distance,
        ds.gt_depth,
    );
    let dropped = subset(&mut ds, &args);
    let n_queries = ds.queries.len() / ds.dim;
    if ds.n_train == 0 || n_queries == 0 {
        return Err("nothing to benchmark after subsetting".into());
    }

    let mut result = match args.mode {
        Mode::Local => run_local(&args, &ds)?,
        Mode::Node => run_node(&args, &ds).await?,
        Mode::Coordinator => run_coordinator(&args, &ds).await?,
    };
    result.dropped_queries = dropped;

    report::emit(
        &report::RunContext {
            dataset: &ds.name,
            distance: &ds.distance,
            mode: args.mode.name(),
            dim: ds.dim,
            bit_width: BIT_WIDTH,
            k: args.k,
            queries: n_queries,
            calibration_sample: args.calibration_sample.min(ds.n_train),
        },
        result,
        args.out.as_deref(),
    )
}
