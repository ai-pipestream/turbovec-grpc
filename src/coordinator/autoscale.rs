//! The grow-only autoscaler: placement the coordinator decides for itself.
//!
//! An operator's `Split` names its targets; with the autoscaler enabled the
//! operator instead keeps the spare pool stocked, and the coordinator names
//! them. When a serving shard grows past `max_rows_per_shard` and the pool
//! holds a spare, the shard is split onto its own node and that spare —
//! through `stage_split`, the same encoded-row path the RPC drives, so the
//! targets are validated and flushed before the new topology generation
//! publishes and in-flight searches finish on the old one. Once the new
//! generation serves, the quiesced source index is dropped and its node
//! returns to empty-spare state.
//!
//! The policy is deliberately narrow:
//!
//! - It only grows. A live shard is never moved for balance, and scale-in
//!   (`Join`) stays operator-driven.
//! - One split per tick, so a collection far past the ceiling grows one
//!   shard per interval instead of in a burst.
//! - It acts only on a servable collection. One with an unreachable or
//!   disagreeing shard is operator territory, and the tick leaves it alone.
//! - With no spare registered there is nowhere to grow into; the tick says
//!   so at info level and does nothing.
//!
//! Concurrency with an operator's Split or Join is by generation: the tick
//! plans against one generation and abandons its staged split rather than
//! publish over a topology that moved under it. A split it abandons is torn
//! back down, because a staged target that is never published is an index
//! nobody will ever query.

use std::time::Duration;

use super::{bind, CoordinatorService, ShardConfig, ShardRef};
use crate::proto::{DropIndexRequest, ListIndexesRequest};

/// Autoscaler knobs. The ceiling is the enable switch: there is no
/// autoscaler without a row count to hold shards under.
#[derive(Clone, Copy, Debug)]
pub struct AutoscalePolicy {
    /// Rows a serving shard may hold before the autoscaler splits it.
    pub max_rows_per_shard: u64,

    /// How often the policy looks at the collection.
    pub interval: Duration,
}

impl AutoscalePolicy {
    pub fn new(max_rows_per_shard: u64, interval: Duration) -> Self {
        assert!(
            max_rows_per_shard > 0,
            "max_rows_per_shard must be positive"
        );
        assert!(!interval.is_zero(), "interval must be positive");
        Self {
            max_rows_per_shard,
            interval,
        }
    }

    /// Read the policy from the environment. `Ok(None)` is the default: the
    /// autoscaler runs only when `TURBOVEC_AUTOSCALE_MAX_ROWS_PER_SHARD` is
    /// set. `TURBOVEC_AUTOSCALE_INTERVAL_MS` defaults to 30 seconds.
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(max_rows_per_shard) =
            crate::config::optional_positive_u64("TURBOVEC_AUTOSCALE_MAX_ROWS_PER_SHARD")?
        else {
            return Ok(None);
        };
        let interval_ms = crate::config::positive_usize("TURBOVEC_AUTOSCALE_INTERVAL_MS", 30_000)?;
        Ok(Some(Self::new(
            max_rows_per_shard,
            Duration::from_millis(interval_ms as u64),
        )))
    }
}

impl CoordinatorService {
    /// Run the grow-only policy on its own task until the task is aborted.
    pub fn spawn_autoscaler(&self, policy: AutoscalePolicy) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            tracing::info!(
                max_rows_per_shard = policy.max_rows_per_shard,
                interval_ms = policy.interval.as_millis() as u64,
                "autoscaler running"
            );
            let mut ticks = tokio::time::interval(policy.interval);
            loop {
                ticks.tick().await;
                service.autoscale_tick(&policy).await;
            }
        })
    }

    /// One look at the collection, growing it by one shard when the policy
    /// says to. Nothing in here may fail the loop: every outcome short of a
    /// published split is a log line.
    async fn autoscale_tick(&self, policy: &AutoscalePolicy) {
        let (generation, table) = self.topology();
        if table.is_empty() {
            return;
        }
        let probes = self.probe_all(&table).await;
        let mut shards = Vec::with_capacity(probes.len());
        for probe in probes {
            match probe {
                Ok(probe) => shards.push(probe),
                // A collection that is not servable is operator territory.
                Err(_) => return,
            }
        }
        let Ok(pinned) = bind(generation, shards) else {
            return;
        };
        // Table order, so the shard chosen is deterministic and a second
        // over-ceiling shard waits for its own tick.
        let Some((at, source)) = pinned
            .shards
            .iter()
            .enumerate()
            .find(|(_, probe)| probe.info.len > policy.max_rows_per_shard)
        else {
            return;
        };

        let spare = self
            .spares
            .lock()
            .expect("coordinator spare pool lock poisoned")
            .first()
            .cloned();
        let Some(spare) = spare else {
            tracing::info!(
                topology_generation = generation,
                shard = %source.address,
                rows = source.info.len,
                max_rows_per_shard = policy.max_rows_per_shard,
                "shard over the row ceiling but the spare pool is empty; not splitting"
            );
            return;
        };
        // A spare that does not answer would fail the split mid-stage, after
        // rows had already moved to the first target. Find out first.
        let spare_live = match self.query_client(&spare) {
            Ok(mut client) => client
                .list_indexes(ListIndexesRequest {})
                .await
                .map(|_| ())
                .map_err(|status| status.message().to_string()),
            Err(status) => Err(status.message().to_string()),
        };
        if let Err(error) = spare_live {
            tracing::info!(
                topology_generation = generation,
                spare = %spare,
                error,
                "spare did not answer; not splitting this tick"
            );
            return;
        }

        // The source node keeps half its rows and the spare takes the other
        // half, so only half the rows cross the network. Targeting the
        // source node is ordinary Split semantics: a fresh index beside the
        // quiesced one.
        let targets = vec![source.address.clone(), spare.clone()];
        let source_config = ShardConfig::with_index(&source.address, &source.index_id);
        let (source, staged, counts) = match self.stage_split(&source_config, &targets, &[]).await {
            Ok(staged) => staged,
            Err(status) => {
                tracing::warn!(
                    topology_generation = generation,
                    source = %source_config.address,
                    targets = ?targets,
                    error = %status.message(),
                    "autoscaler split failed while staging; targets may hold partial results"
                );
                return;
            }
        };

        let published = async {
            self.flush_before_durable_rebind(&staged).await?;
            let configs = self.configs_for_topology(&staged).await?;
            // The collection keeps its other shards: the source's slot in the
            // table becomes the targets, in place, so table order — and with
            // it the tie-break between equal scores — does not move.
            let mut next = Vec::with_capacity(table.len() + staged.len() - 1);
            next.extend_from_slice(&table[..at]);
            next.extend(configs);
            next.extend_from_slice(&table[at + 1..]);
            self.rebind_if_unchanged(generation, next)
        }
        .await;
        let new_generation = match published {
            Ok(new_generation) => new_generation,
            Err(status) => {
                self.drop_staged(&staged).await;
                tracing::info!(
                    topology_generation = generation,
                    error = %status.message(),
                    "autoscaler abandoned its staged split"
                );
                return;
            }
        };
        let rows_moved: u64 = counts.iter().sum();
        tracing::info!(
            old_generation = generation,
            new_generation,
            source = %source.address,
            source_index = %source.index_id,
            targets = ?targets,
            rows_moved,
            "autoscaler published a split"
        );

        // The source goes away only once the new generation proves it can
        // serve. A drop that fails is left behind with a warning: the
        // quiesced index serves nothing and the next Split can still name
        // the node.
        if let Err(status) = self.collection().await {
            tracing::warn!(
                new_generation,
                error = %status.message(),
                "collection did not come up servable after the autosplit; leaving the source in place"
            );
            return;
        }
        match self.admin_client(&source.address) {
            Ok(mut client) => match client
                .drop_index(DropIndexRequest {
                    index_id: source.index_id.clone(),
                })
                .await
            {
                Ok(_) => tracing::info!(
                    new_generation,
                    node = %source.address,
                    index_id = %source.index_id,
                    "dropped the quiesced autosplit source"
                ),
                Err(status) => tracing::warn!(
                    new_generation,
                    node = %source.address,
                    index_id = %source.index_id,
                    error = %status.message(),
                    "could not drop the quiesced autosplit source; leaving it"
                ),
            },
            Err(status) => tracing::warn!(
                new_generation,
                node = %source.address,
                index_id = %source.index_id,
                error = %status.message(),
                "could not reach the source node to drop the quiesced index; leaving it"
            ),
        }
    }

    /// Best-effort teardown of staged targets a split will not publish.
    /// Failures are warnings, not loop failures.
    async fn drop_staged(&self, staged: &[ShardRef]) {
        for shard in staged {
            let result = match self.admin_client(&shard.address) {
                Ok(mut client) => client
                    .drop_index(DropIndexRequest {
                        index_id: shard.index_id.clone(),
                    })
                    .await
                    .map(|_| ()),
                Err(status) => Err(status),
            };
            if let Err(status) = result {
                tracing::warn!(
                    node = %shard.address,
                    index_id = %shard.index_id,
                    error = %status.message(),
                    "could not drop an abandoned autosplit target; leaving it"
                );
            }
        }
    }
}
