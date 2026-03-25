// Vendored and adapted from quickwit-oss/quickwit (quickwit-indexing crate).
// Original: quickwit/quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs
//
// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications for SearchDB:
// - Replaced quickwit SplitMetadata with SegmentMeta (our lightweight equivalent)
// - Removed dependency on quickwit-config, quickwit-metastore, tracing, time crate
// - Simplified MergeOperation to return Vec<Vec<SegmentMeta>> (groups of segments to merge)
// - Removed maturation_period (SearchDB uses size-only maturity for now)
// - Removed time_range-based ordering (SearchDB segments don't carry time ranges yet)

use std::cmp::Ordering;
use std::ops::Range;

/// Metadata about a segment, used by the merge policy to decide what to merge.
/// This is a pure data struct with no tantivy dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Opaque identifier for the segment. In practice, a tantivy SegmentId string.
    pub segment_id: String,
    /// Number of documents (alive) in this segment.
    pub num_docs: usize,
    /// Number of times this segment has been through a merge operation.
    /// Fresh segments from indexing have num_merge_ops = 0.
    pub num_merge_ops: usize,
}

/// Configuration for the StableLogMergePolicy.
#[derive(Debug, Clone)]
pub struct StableLogMergePolicyConfig {
    /// Minimum number of docs for the first level. Segments with fewer docs than
    /// this are all grouped into the same (smallest) level.
    pub min_level_num_docs: usize,
    /// Minimum number of segments in a level before a merge is triggered.
    pub merge_factor: usize,
    /// Maximum number of segments that can be merged in a single operation.
    pub max_merge_factor: usize,
    /// Target number of docs per segment. Segments at or above this size are
    /// considered "mature" and excluded from merging.
    pub split_num_docs_target: usize,
}

impl Default for StableLogMergePolicyConfig {
    fn default() -> Self {
        Self {
            min_level_num_docs: 100_000,
            merge_factor: 10,
            max_merge_factor: 12,
            split_num_docs_target: 10_000_000,
        }
    }
}

/// `StableLogMergePolicy` groups segments into logarithmic tiers by document
/// count and merges within each tier when enough segments accumulate.
///
/// Ported from quickwit's `StableLogMergePolicy`. The algorithm:
///
/// 1. Exclude "mature" segments (num_docs >= split_num_docs_target).
/// 2. Sort remaining segments by num_docs ascending, then by segment_id for
///    determinism.
/// 3. Assign segments to levels. Level boundaries grow by 3x:
///    - Level 0 holds segments with num_docs < max(first_segment.num_docs * 3, min_level_num_docs)
///    - Each subsequent level: boundary = previous_boundary_segment.num_docs * 3
/// 4. Within each level (starting from the largest), select a merge candidate:
///    - Walk from the end of the level backwards, accumulating segments.
///    - Stop when we hit merge_factor segments OR the total doc count would
///      exceed split_num_docs_target.
///    - If we have fewer than merge_factor segments AND total docs < split_num_docs_target,
///      the candidate is "too small" and we skip it.
#[derive(Debug, Clone, Default)]
pub struct StableLogMergePolicy {
    config: StableLogMergePolicyConfig,
}

impl StableLogMergePolicy {
    pub fn new(config: StableLogMergePolicyConfig) -> Self {
        Self { config }
    }

    /// Given a set of segments, return groups of segments that should be merged.
    /// Each inner Vec is one merge operation (the segments in it should be merged together).
    /// Segments not included in any merge operation are left alone.
    ///
    /// Mature segments (num_docs >= split_num_docs_target) are excluded from
    /// merge consideration entirely.
    pub fn operations(&self, segments: &[SegmentMeta]) -> Vec<Vec<SegmentMeta>> {
        if segments.len() < 2 {
            return Vec::new();
        }

        // Separate mature segments from candidates
        let mut candidates: Vec<SegmentMeta> = segments
            .iter()
            .filter(|s| s.num_docs < self.config.split_num_docs_target)
            .cloned()
            .collect();

        if candidates.len() < 2 {
            return Vec::new();
        }

        // Sort by num_docs ascending, then by segment_id for determinism
        candidates.sort_unstable_by(cmp_segments);

        let levels = self.build_levels(&candidates);
        let mut merge_ops: Vec<Vec<SegmentMeta>> = Vec::new();

        // Process levels from largest to smallest. We process in reverse so that
        // drain() indices remain valid (we drain from higher indices first).
        for level_range in levels.into_iter().rev() {
            if let Some(merge_range) = self.merge_candidate_from_level(&candidates, level_range) {
                let merged: Vec<SegmentMeta> = candidates.drain(merge_range).collect();
                merge_ops.push(merged);
            }
        }

        merge_ops
    }

    /// Returns true if a segment is "mature" (should never be merged again).
    #[allow(dead_code)]
    pub fn is_mature(&self, num_docs: usize) -> bool {
        num_docs >= self.config.split_num_docs_target
    }

    /// Group segments into levels by document count.
    /// Assumes segments are sorted by num_docs ascending.
    ///
    /// Level boundaries grow by 3x from the first segment's doc count or
    /// min_level_num_docs, whichever is larger.
    fn build_levels(&self, segments: &[SegmentMeta]) -> Vec<Range<usize>> {
        if segments.is_empty() {
            return Vec::new();
        }

        let mut levels: Vec<Range<usize>> = Vec::new();
        let mut current_level_start = 0;
        let mut current_level_max_docs =
            (segments[0].num_docs * 3).max(self.config.min_level_num_docs);

        for (i, segment) in segments.iter().enumerate() {
            if segment.num_docs >= current_level_max_docs {
                levels.push(current_level_start..i);
                current_level_start = i;
                current_level_max_docs = 3 * segment.num_docs;
            }
        }
        levels.push(current_level_start..segments.len());
        levels
    }

    /// Within a level, select the best contiguous range of segments to merge.
    /// Works from the end of the level backwards (largest segments first within
    /// the level) to prefer merging the biggest segments together.
    fn merge_candidate_from_level(
        &self,
        segments: &[SegmentMeta],
        level_range: Range<usize>,
    ) -> Option<Range<usize>> {
        let merge_end = level_range.end;
        let mut merge_start = merge_end;

        for i in level_range.rev() {
            if self.candidate_size(&segments[merge_start..merge_end])
                == MergeCandidateSize::OneMoreWouldBeTooBig
            {
                break;
            }
            merge_start = i;
        }

        if self.candidate_size(&segments[merge_start..merge_end]) == MergeCandidateSize::TooSmall {
            return None;
        }

        Some(merge_start..merge_end)
    }

    /// Evaluate whether a merge candidate (set of segments) is too small, valid,
    /// or already at the maximum size.
    fn candidate_size(&self, segments: &[SegmentMeta]) -> MergeCandidateSize {
        // Need at least 2 segments to merge
        if segments.len() <= 1 {
            return MergeCandidateSize::TooSmall;
        }

        // Already at max_merge_factor
        if segments.len() >= self.config.max_merge_factor {
            return MergeCandidateSize::OneMoreWouldBeTooBig;
        }

        let total_docs: usize = segments.iter().map(|s| s.num_docs).sum();

        // Would exceed target doc count
        if total_docs >= self.config.split_num_docs_target {
            return MergeCandidateSize::OneMoreWouldBeTooBig;
        }

        // Not enough segments to trigger a merge yet
        if segments.len() < self.config.merge_factor {
            return MergeCandidateSize::TooSmall;
        }

        MergeCandidateSize::Valid
    }
}

/// Total ordering for segments: ascending by num_docs, then by segment_id
/// for determinism.
fn cmp_segments(a: &SegmentMeta, b: &SegmentMeta) -> Ordering {
    a.num_docs
        .cmp(&b.num_docs)
        .then_with(|| a.segment_id.cmp(&b.segment_id))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MergeCandidateSize {
    /// Too few segments to warrant a merge.
    TooSmall,
    /// The candidate has enough segments and is within size limits.
    Valid,
    /// Adding one more segment would exceed max_merge_factor or split_num_docs_target.
    OneMoreWouldBeTooBig,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create segments with given doc counts.
    fn make_segments(doc_counts: &[usize]) -> Vec<SegmentMeta> {
        doc_counts
            .iter()
            .enumerate()
            .map(|(i, &num_docs)| SegmentMeta {
                segment_id: format!("seg_{:02}", i),
                num_docs,
                num_merge_ops: 0,
            })
            .collect()
    }

    fn merge_op_ids(ops: &[Vec<SegmentMeta>]) -> Vec<Vec<String>> {
        ops.iter()
            .map(|group| {
                let mut ids: Vec<String> = group.iter().map(|s| s.segment_id.clone()).collect();
                ids.sort();
                ids
            })
            .collect()
    }

    // --- Level building tests ---

    #[test]
    fn test_build_levels_empty() {
        let policy = StableLogMergePolicy::default();
        let segments = make_segments(&[]);
        let levels = policy.build_levels(&segments);
        assert!(levels.is_empty());
    }

    #[test]
    fn test_build_levels_single_level() {
        let policy = StableLogMergePolicy::default();
        // All segments are small (< min_level_num_docs = 100_000), so they
        // should all be in one level.
        let segments = make_segments(&[100, 200, 300, 500, 1000]);
        let levels = policy.build_levels(&segments);
        assert_eq!(levels, vec![0..5]);
    }

    #[test]
    fn test_build_levels_two_levels() {
        let policy = StableLogMergePolicy::default();
        // Small segments in level 0, large segments cross into level 1.
        // min_level_num_docs = 100_000. 100_000 * 3 = 300_000 is first boundary.
        // 100_000 < 300_000 so it's in level 0.
        // 800_000 >= 300_000, so it starts level 1.
        let segments = make_segments(&[100_000, 100_000, 100_000, 800_000, 900_000]);
        let levels = policy.build_levels(&segments);
        assert_eq!(levels, vec![0..3, 3..5]);
    }

    #[test]
    fn test_build_levels_three_levels() {
        let policy = StableLogMergePolicy::default();
        // 100_000 < max(100_000*3, 100_000) = 300_000 -> level 0
        // 800_000 >= 300_000 -> level 1. Boundary becomes 800_000*3 = 2_400_000
        // 1_600_000 < 2_400_000 -> still level 1
        // 3_000_000 >= 2_400_000 -> level 2
        let segments = make_segments(&[
            100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 800_000,
            1_600_000, 3_000_000,
        ]);
        let levels = policy.build_levels(&segments);
        assert_eq!(levels, vec![0..8, 8..10, 10..11]);
    }

    // --- Merge operation tests ---

    #[test]
    fn test_no_merge_single_segment() {
        let policy = StableLogMergePolicy::default();
        let segments = make_segments(&[1000]);
        let ops = policy.operations(&segments);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_no_merge_too_few_segments() {
        let policy = StableLogMergePolicy::default();
        // 7 segments is below merge_factor (10), so no merge
        let segments = make_segments(&[100; 7]);
        let ops = policy.operations(&segments);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_merge_exactly_merge_factor() {
        let policy = StableLogMergePolicy::default();
        // Exactly 10 segments at the same level -> triggers one merge
        let segments = make_segments(&[100; 10]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].len(), 10);
    }

    #[test]
    fn test_merge_more_than_merge_factor() {
        let policy = StableLogMergePolicy::default();
        // 13 segments: policy should merge up to max_merge_factor (12) from
        // the end, leaving 1 segment unmerged.
        let segments = make_segments(&[100; 13]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].len(), 12);
    }

    #[test]
    fn test_mature_segments_excluded() {
        let policy = StableLogMergePolicy::default();
        // 10 small segments + 1 mature segment (>= 10_000_000).
        // The mature segment should be excluded from merge consideration.
        let mut doc_counts = vec![100_000; 10];
        doc_counts.push(10_000_000);
        let segments = make_segments(&doc_counts);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        // The merge should contain only the 10 small segments
        assert_eq!(ops[0].len(), 10);
        for seg in &ops[0] {
            assert!(seg.num_docs < 10_000_000);
        }
    }

    #[test]
    fn test_no_merge_all_mature() {
        let policy = StableLogMergePolicy::default();
        let segments = make_segments(&[10_000_000, 10_000_001]);
        let ops = policy.operations(&segments);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_merge_stops_at_target_doc_count() {
        let policy = StableLogMergePolicy::default();
        // Two segments that together exceed split_num_docs_target.
        // They should still merge because there are only 2 and total >= target
        // triggers OneMoreWouldBeTooBig (but they already form a valid pair).
        let segments = make_segments(&[5_000_000, 5_000_000]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].len(), 2);
    }

    #[test]
    fn test_merge_large_segments_partial() {
        let policy = StableLogMergePolicy::default();
        // 3 segments near the target. Two together exceed target, so the policy
        // should merge just the 2 largest (from the end of the sorted list).
        let segments = make_segments(&[9_999_997, 9_999_998, 9_999_999]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        // Should merge the 2 segments whose combined docs >= target
        assert_eq!(ops[0].len(), 2);
    }

    #[test]
    fn test_segments_across_levels() {
        let policy = StableLogMergePolicy::default();
        // Mix of small and medium segments across two levels.
        // Level 0: 10 segments of 100 docs each (below min_level_num_docs)
        // Level 1: 3 segments of 100_000 docs
        // Level 0 has merge_factor (10) segments -> should merge.
        // Level 1 has only 3 < merge_factor -> no merge.
        let mut doc_counts = vec![100; 10];
        doc_counts.extend_from_slice(&[100_000, 100_000, 100_000]);
        let segments = make_segments(&doc_counts);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        // The merge should be the 10 small segments
        assert_eq!(ops[0].len(), 10);
    }

    #[test]
    fn test_mixed_sizes_below_min_level() {
        let policy = StableLogMergePolicy::default();
        // All segments are below min_level_num_docs (100_000), so they
        // all land in level 0. With 10 segments -> triggers merge.
        let segments = make_segments(&[
            100, 1000, 10_000, 10_000, 10_000, 10_000, 10_000, 40_000, 40_000, 40_000,
        ]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].len(), 10);
    }

    #[test]
    fn test_segments_above_min_level_no_merge() {
        let policy = StableLogMergePolicy::default();
        // 8 segments that span two levels but neither level has merge_factor
        let segments = make_segments(&[
            100_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000,
        ]);
        let ops = policy.operations(&segments);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_is_mature() {
        let policy = StableLogMergePolicy::default();
        assert!(!policy.is_mature(0));
        assert!(!policy.is_mature(9_999_999));
        assert!(policy.is_mature(10_000_000));
        assert!(policy.is_mature(20_000_000));
    }

    // --- Custom config tests ---

    #[test]
    fn test_custom_config_smaller_factor() {
        let config = StableLogMergePolicyConfig {
            min_level_num_docs: 100,
            merge_factor: 3,
            max_merge_factor: 5,
            split_num_docs_target: 10_000,
        };
        let policy = StableLogMergePolicy::new(config);
        // 4 small segments with merge_factor=3 -> should merge 3+ segments
        let segments = make_segments(&[10, 20, 30, 40]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert!(ops[0].len() >= 3);
        assert!(ops[0].len() <= 5);
    }

    #[test]
    fn test_custom_config_respects_max_merge_factor() {
        let config = StableLogMergePolicyConfig {
            min_level_num_docs: 100,
            merge_factor: 3,
            max_merge_factor: 4,
            split_num_docs_target: 1_000_000,
        };
        let policy = StableLogMergePolicy::new(config);
        // 6 segments with max_merge_factor=4 -> merges at most 4
        let segments = make_segments(&[10, 20, 30, 40, 50, 60]);
        let ops = policy.operations(&segments);
        assert_eq!(ops.len(), 1);
        assert!(ops[0].len() <= 4);
    }

    #[test]
    fn test_deterministic_output() {
        let policy = StableLogMergePolicy::default();
        let segments = make_segments(&[100; 12]);

        let ops1 = policy.operations(&segments);
        let ops2 = policy.operations(&segments);

        assert_eq!(merge_op_ids(&ops1), merge_op_ids(&ops2));
    }

    #[test]
    fn test_operations_returns_no_duplicates() {
        let policy = StableLogMergePolicy::default();
        let segments = make_segments(&[100; 15]);
        let ops = policy.operations(&segments);

        // Collect all segment IDs across all merge operations
        let mut all_ids: Vec<String> = ops
            .iter()
            .flat_map(|group| group.iter().map(|s| s.segment_id.clone()))
            .collect();
        let before_dedup = all_ids.len();
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(
            all_ids.len(),
            before_dedup,
            "merge operations should not contain duplicate segment IDs"
        );
    }
}
