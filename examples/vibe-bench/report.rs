//! Benchmark result math and reporting: recall against the VIBE ground
//! truth, latency percentiles, and the human/JSON output.

use std::time::Duration;

/// Outcome of one benchmark run.
pub struct BenchResult {
    /// Neighbours the engine was asked for per query: `max(k, gt_depth)`,
    /// so every reported recall comes from the same timed searches.
    pub fetch: usize,
    /// recall@(depth) entries actually reported, ascending by depth.
    pub recalls: Vec<(usize, f64)>,
    /// Per-query wall latencies, unsorted.
    pub latencies: Vec<Duration>,
    /// Wall time of the whole timed search loop; QPS derives from it.
    pub search_wall: Duration,
    /// Ingest wall time (calibration excluded), when the run ingested.
    pub ingest: Option<Duration>,
    /// Calibration wall time, when the run calibrated.
    pub calibration: Option<Duration>,
    /// Rows ingested.
    pub rows: usize,
    /// Queries whose ground truth was dropped by `--max-train`.
    pub dropped_queries: usize,
}

/// recall@r: averaged over queries, the fraction of the query's true top-r
/// present in the returned top-r. Returned ids may be positional slots or
/// labels; both identify the same train row here.
pub fn recall_at(returned: &[Vec<u64>], neighbors: &[i64], gt_depth: usize, r: usize) -> f64 {
    let mut total = 0.0f64;
    for (qi, ids) in returned.iter().enumerate() {
        let gt = &neighbors[qi * gt_depth..qi * gt_depth + r];
        let hits = ids
            .iter()
            .take(r)
            .filter(|id| gt.contains(&(**id as i64)))
            .count();
        total += hits as f64 / r as f64;
    }
    total / returned.len().max(1) as f64
}

/// Nearest-rank percentile over sorted durations, matching the other
/// examples in this repository.
fn percentile(sorted: &[Duration], p: usize) -> Duration {
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Everything the report needs that the modes share.
pub struct RunContext<'a> {
    pub dataset: &'a str,
    pub distance: &'a str,
    pub mode: &'a str,
    pub dim: usize,
    pub bit_width: usize,
    pub k: usize,
    pub queries: usize,
    pub calibration_sample: usize,
}

/// Print the human-readable table and emit the JSON form to `out` (a file
/// path) or stdout.
pub fn emit(
    ctx: &RunContext,
    mut result: BenchResult,
    out: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    result.latencies.sort_unstable();
    let lat = &result.latencies;
    let mean = millis(lat.iter().sum::<Duration>()) / lat.len().max(1) as f64;
    let (p50, p95, p99) = (
        percentile(lat, 50),
        percentile(lat, 95),
        percentile(lat, 99),
    );
    let qps = lat.len() as f64 / result.search_wall.as_secs_f64();

    println!();
    println!("VIBE benchmark: {}", ctx.dataset);
    println!(
        "mode {} | distance {} (inner product{}) | dim {} | bit width {}",
        ctx.mode,
        ctx.distance,
        if ctx.distance == "cosine" {
            " on L2-normalized rows"
        } else {
            ""
        },
        ctx.dim,
        ctx.bit_width,
    );
    println!(
        "rows {} | queries {} ({} dropped by --max-train) | fetch {} | k {}",
        result.rows, ctx.queries, result.dropped_queries, result.fetch, ctx.k,
    );
    if let Some(c) = result.calibration {
        println!(
            "calibration: {:.2?} from a {}-row sample",
            c, ctx.calibration_sample
        );
    }
    if let Some(i) = result.ingest {
        println!(
            "ingest: {:.2?} ({:.0} rows/s)",
            i,
            result.rows as f64 / i.as_secs_f64()
        );
    }
    for (r, recall) in &result.recalls {
        println!("recall@{r:<4} {recall:.4}");
    }
    println!(
        "latency ms: mean {mean:.3} p50 {:.3} p95 {:.3} p99 {:.3}",
        millis(p50),
        millis(p95),
        millis(p99),
    );
    println!("QPS: {qps:.0}");

    let recalls: serde_json::Map<String, serde_json::Value> = result
        .recalls
        .iter()
        .map(|(r, recall)| (r.to_string(), serde_json::json!(recall)))
        .collect();
    let json = serde_json::json!({
        "dataset": ctx.dataset,
        "distance": ctx.distance,
        "mode": ctx.mode,
        "dim": ctx.dim,
        "bit_width": ctx.bit_width,
        "rows": result.rows,
        "queries": ctx.queries,
        "dropped_queries": result.dropped_queries,
        "k": ctx.k,
        "fetch": result.fetch,
        "calibration_sample": ctx.calibration_sample,
        "recall": recalls,
        "latency_ms": {
            "mean": mean,
            "p50": millis(p50),
            "p95": millis(p95),
            "p99": millis(p99),
        },
        "qps": qps,
        "ingest_seconds": result.ingest.map(|d| d.as_secs_f64()),
        "calibration_seconds": result.calibration.map(|d| d.as_secs_f64()),
    });
    let text = serde_json::to_string_pretty(&json)?;
    match out {
        Some(path) => {
            std::fs::write(path, format!("{text}\n"))?;
            println!("json written to {}", path.display());
        }
        None => println!("{text}"),
    }
    Ok(())
}
