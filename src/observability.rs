//! Small dependency-free Prometheus metrics endpoint.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    node_searches: AtomicU64,
    coordinator_searches: AtomicU64,
    search_errors: AtomicU64,
    active_scans: AtomicU64,
    candidates_emitted: AtomicU64,
    blocks_scanned: AtomicU64,
    rows_ingested: AtomicU64,
    topology_generation: AtomicU64,
}

pub struct ActiveScan {
    inner: Arc<MetricsInner>,
}

impl Drop for ActiveScan {
    fn drop(&mut self) {
        self.inner.active_scans.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Metrics {
    pub fn scan_started(&self) -> ActiveScan {
        self.inner.active_scans.fetch_add(1, Ordering::Relaxed);
        ActiveScan {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn node_search_finished(&self, emitted: u64, blocks: u64) {
        self.inner.node_searches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .candidates_emitted
            .fetch_add(emitted, Ordering::Relaxed);
        self.inner
            .blocks_scanned
            .fetch_add(blocks, Ordering::Relaxed);
    }

    pub fn coordinator_search_finished(&self) {
        self.inner
            .coordinator_searches
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn search_failed(&self) {
        self.inner.search_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn rows_ingested(&self, rows: u64) {
        self.inner.rows_ingested.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn set_topology_generation(&self, generation: u64) {
        self.inner
            .topology_generation
            .store(generation, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        let mut body = String::new();
        for (name, help, kind, value) in [
            (
                "turbovec_node_searches_total",
                "Completed node search scans.",
                "counter",
                self.inner.node_searches.load(Ordering::Relaxed),
            ),
            (
                "turbovec_coordinator_searches_total",
                "Completed distributed searches.",
                "counter",
                self.inner.coordinator_searches.load(Ordering::Relaxed),
            ),
            (
                "turbovec_search_errors_total",
                "Searches that ended with an error.",
                "counter",
                self.inner.search_errors.load(Ordering::Relaxed),
            ),
            (
                "turbovec_active_scans",
                "Node scans currently consuming an admission slot.",
                "gauge",
                self.inner.active_scans.load(Ordering::Relaxed),
            ),
            (
                "turbovec_candidates_emitted_total",
                "Candidates emitted by node streaming scans.",
                "counter",
                self.inner.candidates_emitted.load(Ordering::Relaxed),
            ),
            (
                "turbovec_blocks_scanned_total",
                "Chunks scored by node streaming scans.",
                "counter",
                self.inner.blocks_scanned.load(Ordering::Relaxed),
            ),
            (
                "turbovec_rows_ingested_total",
                "Rows accepted by node ingest streams.",
                "counter",
                self.inner.rows_ingested.load(Ordering::Relaxed),
            ),
            (
                "turbovec_topology_generation",
                "Active coordinator topology generation.",
                "gauge",
                self.inner.topology_generation.load(Ordering::Relaxed),
            ),
        ] {
            let _ = writeln!(body, "# HELP {name} {help}");
            let _ = writeln!(body, "# TYPE {name} {kind}");
            let _ = writeln!(body, "{name} {value}");
        }
        body.push_str("# EOF\n");
        body
    }

    pub async fn start(self, address: SocketAddr) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let listener = tokio::net::TcpListener::bind(address).await?;
        tracing::info!(%address, "metrics endpoint listening");
        Ok(tokio::spawn(async move {
            if let Err(error) = self.serve(listener).await {
                tracing::error!(%error, "metrics endpoint stopped");
            }
        }))
    }

    async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        loop {
            let (mut socket, _) = listener.accept().await?;
            let metrics = self.clone();
            tokio::spawn(async move {
                let mut request = [0u8; 2048];
                let read = socket.read(&mut request).await.unwrap_or(0);
                let is_metrics = request[..read].starts_with(b"GET /metrics ");
                let (status, content_type, body) = if is_metrics {
                    (
                        "200 OK",
                        "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        metrics.render(),
                    )
                } else {
                    (
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        "not found\n".to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn openmetrics_snapshot_reports_counters_and_gauges() {
        let metrics = Metrics::default();
        metrics.rows_ingested(7);
        metrics.set_topology_generation(3);
        let active = metrics.scan_started();
        let snapshot = metrics.render();
        assert!(snapshot.contains("turbovec_rows_ingested_total 7"));
        assert!(snapshot.contains("turbovec_topology_generation 3"));
        assert!(snapshot.contains("turbovec_active_scans 1"));
        drop(active);
        assert!(metrics.render().contains("turbovec_active_scans 0"));
    }
}
