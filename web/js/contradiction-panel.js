/**
 * Contradiction detail panel.
 */

function sourceLink(url, label) {
  if (!url) return "";
  // Clean up internal API URLs
  if (url.includes("webservices.esd.org.uk") || url.includes("fingertips.phe.org.uk/api")) {
    return `<a href="${url}" target="_blank" rel="noopener" class="source-link" title="${url}">API source</a>`;
  }
  try {
    const host = new URL(url).hostname.replace("www.", "");
    return `<a href="${url}" target="_blank" rel="noopener" class="source-link" title="${url}">${label || host}</a>`;
  } catch {
    return "";
  }
}

export function renderContradictions(selector, data) {
  const container = document.querySelector(selector);
  container.innerHTML = "";

  if (!data || !data.length) {
    container.innerHTML =
      '<p style="color: #888; text-align: center; padding: 2rem;">No contradictions detected.</p>';
    return;
  }

  data.sort((a, b) => (b.severity || 0) - (a.severity || 0));

  const summary = document.createElement("div");
  summary.style.cssText = "margin-bottom: 1rem; padding: 1rem; background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px;";
  const avgSeverity = data.reduce((s, d) => s + (d.severity || 0), 0) / data.length;
  summary.innerHTML = `
    <strong>${data.length}</strong> contradictions detected.
    Average severity: <strong style="color: ${avgSeverity > 0.6 ? '#ef4444' : avgSeverity > 0.3 ? '#eab308' : '#22c55e'}">${(avgSeverity * 100).toFixed(0)}%</strong>
  `;
  container.appendChild(summary);

  data.forEach((c) => {
    const card = document.createElement("div");
    card.className = "contradiction-card";

    const severityClass =
      c.severity > 0.6 ? "severity-high" : c.severity > 0.3 ? "severity-mid" : "severity-low";

    const claimLink = sourceLink(c.claim_url, "source");
    const outcomeLink = sourceLink(c.outcome_url, "source");

    card.innerHTML = `
      <span class="severity ${severityClass}">${(c.severity * 100).toFixed(0)}% severity</span>
      <span class="topic-badge">${c.topic || ""}</span>
      <div class="claim">"${c.claim_text}" ${claimLink}</div>
      <div class="outcome">${c.outcome_text} ${outcomeLink}</div>
      <div class="meta">
        Claim: ${c.claim_date || "unknown"} &middot;
        Outcome: ${c.outcome_date || "unknown"}
      </div>
    `;
    container.appendChild(card);
  });
}
