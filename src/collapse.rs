//! Exact parent collapse: one hit per parent, best chunk score.
//!
//! `k` under collapse is top-k **parents**, not top-k chunks. Ranking every
//! admitted chunk and then taking the first `k` distinct parents in score
//! order is exact for a single index: a parent that belongs in the local
//! top-k has its best chunk somewhere in that ranking, and no other parent
//! can push it out by flooding the list with siblings.
//!
//! The coordinator applies the same rule after merging per-shard parent
//! lists. A global top-k parent's best chunk lives on some shard S; on S
//! that score equals the global parent score; if it missed S's local top-k
//! parents, S would already have `k` parents at least as good, so it would
//! not be global top-k. The same parent can appear on more than one shard,
//! so the merge keeps the max score and re-collapses.
//!
//! Tie order is score descending, then an optional shard rank, then the
//! chunk-row label. Floors are not involved; this is a finished list.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::proto::DocumentHit;

/// Compare two hits the way every collapse and merge must: better score
/// first, then earlier shard, then smaller chunk-row label.
pub fn cmp_hits(a: &DocumentHit, a_shard: usize, b: &DocumentHit, b_shard: usize) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then_with(|| a_shard.cmp(&b_shard))
        .then_with(|| a.label.cmp(&b.label))
}

/// Keep the first `k` distinct parents in `cmp_hits` order.
///
/// Hits of the same `parent_label` after the winner increment the winner's
/// `collapsed` by one plus the discarded hit's own `collapsed`, so a
/// coordinator merge of already-collapsed shard lists still counts every
/// sibling that lost.
pub fn collapse_parents(hits: impl IntoIterator<Item = DocumentHit>, k: usize) -> Vec<DocumentHit> {
    collapse_ranked(hits.into_iter().map(|hit| (0usize, hit)), k)
}

/// Coordinator merge: optional parent collapse after a shard-aware sort.
///
/// `ranked` is `(shard_rank, hit)`. When `collapse` is false this is the
/// existing collection merge — score, then shard order, then label —
/// truncated to `k` chunk hits. When `collapse` is true it is top-k
/// parents under the same order.
pub fn merge_hits(
    ranked: impl IntoIterator<Item = (usize, DocumentHit)>,
    k: usize,
    collapse: bool,
) -> Vec<DocumentHit> {
    if collapse {
        collapse_ranked(ranked, k)
    } else {
        let mut ranked: Vec<(usize, DocumentHit)> = ranked.into_iter().collect();
        ranked.sort_by(|a, b| cmp_hits(&a.1, a.0, &b.1, b.0));
        ranked.truncate(k);
        ranked.into_iter().map(|(_, hit)| hit).collect()
    }
}

fn collapse_ranked(
    ranked: impl IntoIterator<Item = (usize, DocumentHit)>,
    k: usize,
) -> Vec<DocumentHit> {
    if k == 0 {
        return Vec::new();
    }
    let mut ranked: Vec<(usize, DocumentHit)> = ranked.into_iter().collect();
    ranked.sort_by(|a, b| cmp_hits(&a.1, a.0, &b.1, b.0));
    let mut out: Vec<DocumentHit> = Vec::new();
    let mut index: HashMap<u64, usize> = HashMap::new();
    for (_, hit) in ranked {
        match index.get(&hit.parent_label) {
            Some(&i) => out[i].collapsed += 1 + hit.collapsed,
            None if out.len() < k => {
                index.insert(hit.parent_label, out.len());
                out.push(hit);
            }
            None => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(parent: u64, label: u64, score: f32, collapsed: u32) -> DocumentHit {
        DocumentHit {
            score,
            label,
            id: format!("p{parent}"),
            chunk_id: format!("c{label}"),
            parent_label: parent,
            collapsed,
            parent_chunks: 0,
        }
    }

    #[test]
    fn first_k_distinct_parents_win() {
        let hits = vec![
            hit(1, 10, 0.9, 0),
            hit(1, 11, 0.8, 0),
            hit(1, 12, 0.7, 0),
            hit(2, 20, 0.6, 0),
        ];
        let collapsed = collapse_parents(hits, 2);
        assert_eq!(collapsed.len(), 2);
        assert_eq!(collapsed[0].parent_label, 1);
        assert_eq!(collapsed[0].label, 10);
        assert_eq!(collapsed[0].collapsed, 2);
        assert_eq!(collapsed[1].parent_label, 2);
        assert_eq!(collapsed[1].label, 20);
        assert_eq!(collapsed[1].collapsed, 0);
    }

    #[test]
    fn equal_scores_break_ties_by_shard_then_label() {
        let a = hit(1, 2, 0.5, 0);
        let b = hit(2, 1, 0.5, 0);
        let merged = merge_hits(vec![(1, a.clone()), (0, b.clone())], 2, true);
        assert_eq!(merged[0].parent_label, 2);
        assert_eq!(merged[1].parent_label, 1);

        let same_shard = collapse_parents(vec![a, b], 2);
        assert_eq!(same_shard[0].label, 1);
        assert_eq!(same_shard[1].label, 2);
    }

    #[test]
    fn merge_recounts_already_collapsed_siblings() {
        let a = hit(1, 10, 0.9, 2);
        let b = hit(1, 11, 0.8, 1);
        let merged = merge_hits(vec![(0, a), (1, b)], 1, true);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].label, 10);
        assert_eq!(merged[0].collapsed, 2 + 1 + 1);
    }

    #[test]
    fn uncollapsed_merge_keeps_sibling_chunks() {
        let hits = vec![
            (0, hit(1, 10, 0.9, 0)),
            (0, hit(1, 11, 0.8, 0)),
            (1, hit(2, 20, 0.7, 0)),
        ];
        let merged = merge_hits(hits, 2, false);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].label, 10);
        assert_eq!(merged[1].label, 11);
    }
}
