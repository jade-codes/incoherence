//! Temporal coherence tracking.
//!
//! Tracks how institutional coherence scores evolve over time,
//! detecting when contradictions emerge and whether they worsen or resolve.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::institution::Topic;

/// A coherence measurement for a specific institution, topic, and time period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoherenceSnapshot {
    pub institution_id: String,
    pub topic: Topic,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub coherence_score: f32,
    pub num_claims: usize,
    pub num_outcomes: usize,
    pub num_contradictions: usize,
}

/// A timeline of coherence snapshots for one institution + topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoherenceTimeline {
    pub institution_id: String,
    pub topic: Topic,
    pub snapshots: Vec<CoherenceSnapshot>,
}

impl CoherenceTimeline {
    pub fn new(institution_id: String, topic: Topic) -> Self {
        Self {
            institution_id,
            topic,
            snapshots: Vec::new(),
        }
    }

    pub fn add(&mut self, snapshot: CoherenceSnapshot) {
        self.snapshots.push(snapshot);
        self.snapshots
            .sort_by_key(|s| s.period_start);
    }

    /// Overall trend: positive means improving coherence, negative means degrading.
    pub fn trend(&self) -> Option<f32> {
        if self.snapshots.len() < 2 {
            return None;
        }
        let first = self.snapshots.first().unwrap().coherence_score;
        let last = self.snapshots.last().unwrap().coherence_score;
        Some(last - first)
    }

    /// The worst (lowest) coherence score in the timeline.
    pub fn worst_score(&self) -> Option<f32> {
        self.snapshots
            .iter()
            .map(|s| s.coherence_score)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Periods where coherence dropped below a threshold.
    pub fn crisis_periods(&self, threshold: f32) -> Vec<&CoherenceSnapshot> {
        self.snapshots
            .iter()
            .filter(|s| s.coherence_score < threshold)
            .collect()
    }

    /// Total contradictions accumulated over the entire timeline.
    pub fn total_contradictions(&self) -> usize {
        self.snapshots.iter().map(|s| s.num_contradictions).sum()
    }
}

/// Aggregate coherence across all topics for an institution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstitutionProfile {
    pub institution_id: String,
    pub timelines: Vec<CoherenceTimeline>,
}

impl InstitutionProfile {
    pub fn new(institution_id: String) -> Self {
        Self {
            institution_id,
            timelines: Vec::new(),
        }
    }

    /// Mean coherence across all topics at the latest snapshot.
    pub fn current_coherence(&self) -> Option<f32> {
        let scores: Vec<f32> = self
            .timelines
            .iter()
            .filter_map(|t| t.snapshots.last())
            .map(|s| s.coherence_score)
            .collect();

        if scores.is_empty() {
            return None;
        }
        Some(scores.iter().sum::<f32>() / scores.len() as f32)
    }

    /// Topics with the most contradictions (worst first).
    pub fn worst_topics(&self) -> Vec<(Topic, usize)> {
        let mut topics: Vec<(Topic, usize)> = self
            .timelines
            .iter()
            .map(|t| (t.topic, t.total_contradictions()))
            .collect();
        topics.sort_by(|a, b| b.1.cmp(&a.1));
        topics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(start: &str, score: f32, contradictions: usize) -> CoherenceSnapshot {
        CoherenceSnapshot {
            institution_id: "hull-cc".to_string(),
            topic: Topic::Housing,
            period_start: NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap(),
            period_end: NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap(),
            coherence_score: score,
            num_claims: 5,
            num_outcomes: 3,
            num_contradictions: contradictions,
        }
    }

    #[test]
    fn test_timeline_trend_degrading() {
        let mut tl = CoherenceTimeline::new("hull-cc".to_string(), Topic::Housing);
        tl.add(make_snapshot("2020-01-01", 0.8, 1));
        tl.add(make_snapshot("2021-01-01", 0.5, 3));
        tl.add(make_snapshot("2022-01-01", 0.3, 5));

        let trend = tl.trend().unwrap();
        assert!(trend < 0.0, "should show degrading coherence");
    }

    #[test]
    fn test_crisis_periods() {
        let mut tl = CoherenceTimeline::new("hull-cc".to_string(), Topic::Housing);
        tl.add(make_snapshot("2020-01-01", 0.8, 0));
        tl.add(make_snapshot("2021-01-01", 0.3, 4));
        tl.add(make_snapshot("2022-01-01", 0.2, 6));

        let crises = tl.crisis_periods(0.5);
        assert_eq!(crises.len(), 2);
    }
}
