//! Hull Incoherence Detector — Rust analysis core
//!
//! Uses the Geometry of Trust protocol to detect contradictions between
//! institutional claims and observed outcomes in Hull & East Riding.

pub mod institution;
pub mod report;
pub mod timeline;

use std::collections::HashMap;

use got_core::geometry::CausalGeometry;
use got_incoherence::coherence::{CoherenceAnalysis, CoherenceConfig};
use got_incoherence::embeddings::{EmbeddingSource, PrecomputedEmbeddings};

/// Run coherence analysis over a set of institutional claims and outcomes.
///
/// Embeds claims and outcomes into the same vector space, then uses GoT's
/// `analyse_value_system` to detect contradictions and score overall coherence.
pub fn analyse_institutional_coherence(
    embeddings: HashMap<String, Vec<f32>>,
    hidden_dim: usize,
    config: &CoherenceConfig,
) -> Result<CoherenceAnalysis, AnalysisError> {
    let geometry = CausalGeometry::identity(hidden_dim);
    let source = PrecomputedEmbeddings::new(embeddings)
        .map_err(|e| AnalysisError::Incoherence(e.to_string()))?;
    let terms: Vec<String> = source.available_terms();
    let term_refs: Vec<&str> = terms.iter().map(|s: &String| s.as_str()).collect();

    let report =
        got_incoherence::analyse_value_system(&term_refs, &source, &geometry, config)
            .map_err(|e| AnalysisError::Incoherence(e.to_string()))?;

    Ok(report.analysis)
}

/// Configuration for the Hull analysis pipeline, loaded from `values.toml`.
pub fn default_config() -> CoherenceConfig {
    // Calibrated for sentence-transformer embeddings (all-mpnet-base-v2).
    // These thresholds differ from GoT's defaults (-0.5 / 0.8) because
    // sentence-transformer cosine distributions are narrower than LLM
    // unembedding cosine distributions.
    CoherenceConfig {
        antonym_threshold: -0.15,
        synonym_threshold: 0.20,
        severity_scale: Some(0.10),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AnalysisError {
    #[error("GoT incoherence analysis failed: {0}")]
    Incoherence(String),

    #[error("Value taxonomy error: {0}")]
    Taxonomy(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_thresholds() {
        let cfg = default_config();
        assert!(cfg.antonym_threshold < 0.0);
        assert!(cfg.synonym_threshold > 0.0);
        assert!(cfg.severity_scale.is_some());
    }

    #[test]
    fn test_analyse_synthetic_coherent() {
        // Two terms pointing in similar directions should be coherent.
        let mut embeddings = HashMap::new();
        let dim = 8;
        embeddings.insert(
            "invest in housing".to_string(),
            vec![0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        );
        embeddings.insert(
            "build affordable homes".to_string(),
            vec![0.7, 0.7, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0],
        );

        let config = CoherenceConfig {
            antonym_threshold: -0.3,
            synonym_threshold: 0.8,
            severity_scale: None,
        };

        let result = analyse_institutional_coherence(embeddings, dim, &config);
        assert!(result.is_ok());
        let analysis = result.unwrap();
        assert!(analysis.contradictions.is_empty());
    }

    #[test]
    fn test_analyse_synthetic_contradictory() {
        // Two terms pointing in opposite directions should produce contradictions.
        let mut embeddings = HashMap::new();
        let dim = 4;
        embeddings.insert("prioritise housing".to_string(), vec![1.0, 0.0, 0.0, 0.0]);
        embeddings.insert(
            "cut housing budget".to_string(),
            vec![-1.0, 0.0, 0.0, 0.0],
        );

        let config = CoherenceConfig {
            antonym_threshold: -0.3,
            synonym_threshold: 0.8,
            severity_scale: None,
        };

        let result = analyse_institutional_coherence(embeddings, dim, &config);
        assert!(result.is_ok());
        let analysis = result.unwrap();
        assert!(
            !analysis.contradictions.is_empty(),
            "opposite vectors should produce contradictions"
        );
    }
}
