//! Static report generation.
//!
//! Generates HTML and markdown reports from coherence analysis results,
//! suitable for sharing as evidence packs.

use got_incoherence::coherence::CoherenceAnalysis;
use got_incoherence::report::{render_json, render_text};

use crate::timeline::{CoherenceTimeline, InstitutionProfile};

/// Generate a plain-text summary of a coherence analysis.
pub fn text_report(analysis: &CoherenceAnalysis) -> String {
    render_text(analysis)
}

/// Generate a JSON representation of a coherence analysis.
pub fn json_report(analysis: &CoherenceAnalysis) -> Result<String, serde_json::Error> {
    render_json(analysis)
}

/// Generate a markdown report for an institution's overall profile.
pub fn markdown_profile(profile: &InstitutionProfile) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "# Incoherence Report: {}\n\n",
        profile.institution_id
    ));

    if let Some(score) = profile.current_coherence() {
        out.push_str(&format!(
            "**Overall coherence**: {:.0}% ({})\n\n",
            score * 100.0,
            coherence_label(score)
        ));
    }

    let worst = profile.worst_topics();
    if !worst.is_empty() {
        out.push_str("## Worst topics by contradiction count\n\n");
        out.push_str("| Topic | Contradictions |\n|---|---|\n");
        for (topic, count) in &worst {
            out.push_str(&format!("| {:?} | {} |\n", topic, count));
        }
        out.push('\n');
    }

    for tl in &profile.timelines {
        out.push_str(&markdown_timeline(tl));
    }

    out
}

fn markdown_timeline(tl: &CoherenceTimeline) -> String {
    let mut out = String::new();

    out.push_str(&format!("### {:?}\n\n", tl.topic));

    if let Some(trend) = tl.trend() {
        let label = if trend > 0.05 {
            "improving"
        } else if trend < -0.05 {
            "degrading"
        } else {
            "stable"
        };
        out.push_str(&format!("Trend: **{}** ({:+.0}%)\n\n", label, trend * 100.0));
    }

    if !tl.snapshots.is_empty() {
        out.push_str("| Period | Coherence | Claims | Outcomes | Contradictions |\n");
        out.push_str("|---|---|---|---|---|\n");
        for s in &tl.snapshots {
            out.push_str(&format!(
                "| {} to {} | {:.0}% | {} | {} | {} |\n",
                s.period_start,
                s.period_end,
                s.coherence_score * 100.0,
                s.num_claims,
                s.num_outcomes,
                s.num_contradictions,
            ));
        }
        out.push('\n');
    }

    out
}

/// Generate a standalone HTML report embedding the analysis data.
pub fn html_report(profile: &InstitutionProfile) -> String {
    let md = markdown_profile(profile);
    let json_data = serde_json::to_string_pretty(profile).unwrap_or_default();

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hull Incoherence Report: {institution}</title>
    <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }}
        table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
        th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
        th {{ background: #f5f5f5; }}
        .score-high {{ color: #16a34a; }}
        .score-mid {{ color: #ca8a04; }}
        .score-low {{ color: #dc2626; }}
        pre {{ background: #f5f5f5; padding: 1rem; overflow-x: auto; border-radius: 4px; }}
    </style>
</head>
<body>
    <article>{content}</article>
    <details>
        <summary>Raw analysis data (JSON)</summary>
        <pre><code>{json}</code></pre>
    </details>
</body>
</html>"#,
        institution = profile.institution_id,
        content = md,
        json = json_data,
    )
}

fn coherence_label(score: f32) -> &'static str {
    if score >= 0.8 {
        "coherent"
    } else if score >= 0.5 {
        "mixed signals"
    } else if score >= 0.3 {
        "incoherent"
    } else {
        "severely incoherent"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::institution::Topic;
    use crate::timeline::CoherenceSnapshot;
    use chrono::NaiveDate;

    #[test]
    fn test_coherence_labels() {
        assert_eq!(coherence_label(0.9), "coherent");
        assert_eq!(coherence_label(0.6), "mixed signals");
        assert_eq!(coherence_label(0.4), "incoherent");
        assert_eq!(coherence_label(0.1), "severely incoherent");
    }

    #[test]
    fn test_markdown_profile_not_empty() {
        let mut profile = InstitutionProfile::new("hull-cc".to_string());
        let mut tl = CoherenceTimeline::new("hull-cc".to_string(), Topic::Housing);
        tl.add(CoherenceSnapshot {
            institution_id: "hull-cc".to_string(),
            topic: Topic::Housing,
            period_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            period_end: NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
            coherence_score: 0.4,
            num_claims: 10,
            num_outcomes: 5,
            num_contradictions: 3,
        });
        profile.timelines.push(tl);

        let md = markdown_profile(&profile);
        assert!(md.contains("hull-cc"));
        assert!(md.contains("Housing"));
    }
}
