//! Institutional value mapping types.
//!
//! Maps council entities, claims, and outcomes into structures that can
//! be fed through GoT's coherence analysis.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// An institution whose claims and actions are being tracked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Institution {
    pub id: String,
    pub name: String,
    pub kind: InstitutionKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstitutionKind {
    Council,
    NhsTrust,
    HousingAssociation,
    Developer,
    GovernmentAgency,
}

/// A claim made by an institution (stated value / commitment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claim {
    pub id: String,
    pub institution_id: String,
    pub date: NaiveDate,
    pub source_url: Option<String>,
    pub source_type: SourceType,
    pub exact_quote: Option<String>,
    pub paraphrased: String,
    pub topic: Topic,
    pub embedding: Option<Vec<f32>>,
    pub confidence: f32,
}

/// An observed outcome (what actually happened).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub id: String,
    pub institution_id: Option<String>,
    pub date: NaiveDate,
    pub source_url: Option<String>,
    pub source_type: SourceType,
    pub description: String,
    pub topic: Topic,
    pub metric: Option<Metric>,
    pub embedding: Option<Vec<f32>>,
}

/// A quantitative measurement attached to an outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Improved,
    Worsened,
    Unchanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceType {
    MeetingMinutes,
    PressRelease,
    StrategyDocument,
    Speech,
    OnsData,
    CouncilStats,
    FoiResponse,
    NewsReport,
}

/// Topic areas matching the value taxonomy in values.toml.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Topic {
    Housing,
    Health,
    Poverty,
    Transparency,
    FiscalResponsibility,
    Community,
    Democracy,
    Regeneration,
    Growth,
    Climate,
    Flooding,
    Education,
    Transport,
}

impl Topic {
    pub fn all() -> &'static [Topic] {
        &[
            Topic::Housing,
            Topic::Health,
            Topic::Poverty,
            Topic::Transparency,
            Topic::FiscalResponsibility,
            Topic::Community,
            Topic::Democracy,
            Topic::Regeneration,
            Topic::Growth,
            Topic::Climate,
            Topic::Flooding,
            Topic::Education,
            Topic::Transport,
        ]
    }
}

/// A causal link between a claim and an outcome, scored by GoT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalLink {
    pub claim_id: String,
    pub outcome_id: String,
    pub relationship: Relationship,
    pub evidence: Option<String>,
    pub coherence_score: Option<f32>,
    pub severity: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Relationship {
    Fulfilled,
    Contradicted,
    Partial,
    Unrelated,
}

/// A value definition loaded from values.toml.
#[derive(Debug, Clone, Deserialize)]
pub struct ValueDef {
    pub name: String,
    pub description: String,
    pub cluster: String,
    pub antonyms: Vec<String>,
}

/// The full value taxonomy loaded from values.toml.
#[derive(Debug, Clone, Deserialize)]
pub struct ValueTaxonomy {
    pub meta: TaxonomyMeta,
    pub values: Vec<ValueDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaxonomyMeta {
    pub region: String,
    pub period: String,
    pub version: u32,
}

impl ValueTaxonomy {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let taxonomy: ValueTaxonomy = toml::from_str(&content)?;
        Ok(taxonomy)
    }

    pub fn values_for_cluster(&self, cluster: &str) -> Vec<&ValueDef> {
        self.values
            .iter()
            .filter(|v| v.cluster == cluster)
            .collect()
    }

    pub fn clusters(&self) -> Vec<String> {
        let mut clusters: Vec<String> = self.values.iter().map(|v| v.cluster.clone()).collect();
        clusters.sort();
        clusters.dedup();
        clusters
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_all_covers_all_variants() {
        assert_eq!(Topic::all().len(), 13);
    }

    #[test]
    fn test_load_values_toml() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("values.toml");

        if path.exists() {
            let taxonomy = ValueTaxonomy::load(&path).expect("should parse values.toml");
            assert!(!taxonomy.values.is_empty());
            assert!(!taxonomy.clusters().is_empty());

            // Every value should have at least one antonym
            for val in &taxonomy.values {
                assert!(
                    !val.antonyms.is_empty(),
                    "{} has no antonyms",
                    val.name
                );
            }
        }
    }
}
