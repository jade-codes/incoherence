/**
 * Chat interface for querying the knowledge graph.
 *
 * In static mode, performs keyword search client-side against
 * pre-exported timeline and contradictions data.
 * In dynamic mode, sends questions to /api/chat.
 */

const API_BASE = "/api";
const IS_STATIC = !window.__FASTAPI__;

// Pre-loaded data for client-side search (static mode)
let staticData = null;

const TOPIC_KEYWORDS = {
  housing: ["housing", "home", "dwell", "rent", "homeless", "affordable"],
  health: ["health", "life expectancy", "mortality", "smoking", "obesity", "hospital", "mental health", "disease", "nhs"],
  poverty: ["poverty", "depriv", "imd", "food bank", "fuel poverty", "low income", "child poverty"],
  climate: ["climate", "carbon", "net zero", "emission", "green"],
  education: ["education", "school", "attainment", "gcse", "neet"],
  transport: ["transport", "a63", "bus", "road", "cycling"],
  regeneration: ["regeneration", "city of culture", "investment", "enterprise"],
  economy: ["economy", "employment", "unemployment", "earnings", "wages", "jobs"],
  flooding: ["flood", "tidal", "drainage"],
};

const STOP_WORDS = new Set([
  "what", "are", "the", "in", "for", "how", "does", "do", "is", "show",
  "me", "about", "of", "on", "to", "a", "an", "and", "or", "did", "has",
  "have", "with", "tell", "any", "all", "been", "their", "they", "this",
  "that", "which", "from", "where", "who", "why", "can", "could",
]);

async function loadStaticData() {
  if (staticData) return staticData;
  const ext = IS_STATIC ? ".json" : "";
  const [timeline, contradictions] = await Promise.all([
    fetch(`${API_BASE}/timeline${ext}`).then((r) => r.json()),
    fetch(`${API_BASE}/contradictions${ext}`).then((r) => r.json()),
  ]);
  staticData = { timeline, contradictions };
  return staticData;
}

function clientSideSearch(query, data) {
  const queryLower = query.toLowerCase();

  // Detect topics
  const matchedTopics = [];
  for (const [topic, keywords] of Object.entries(TOPIC_KEYWORDS)) {
    if (keywords.some((kw) => queryLower.includes(kw))) {
      matchedTopics.push(topic);
    }
  }

  // Detect entity
  let entityFilter = null;
  if (queryLower.includes("hull") && !queryLower.includes("east riding")) {
    entityFilter = "hull-cc";
  } else if (queryLower.includes("east riding")) {
    entityFilter = "east-riding";
  }

  // Extract search terms
  const searchTerms = queryLower
    .match(/\w+/g)
    ?.filter((w) => !STOP_WORDS.has(w) && w.length > 2) || [];

  // Score and filter claims
  const claims = (data.timeline.claims || [])
    .filter((c) => {
      if (matchedTopics.length && !matchedTopics.includes(c.topic)) return false;
      if (entityFilter && c.entity_id !== entityFilter) return false;
      return true;
    })
    .map((c) => {
      const text = (c.text || "").toLowerCase();
      const score = searchTerms.reduce((s, t) => s + (text.includes(t) ? 1 : 0), 0);
      return { ...c, _score: score };
    })
    .filter((c) => c._score > 0 || !searchTerms.length)
    .sort((a, b) => b._score - a._score)
    .slice(0, 10);

  // Score and filter outcomes
  const outcomes = (data.timeline.outcomes || [])
    .filter((o) => {
      if (matchedTopics.length && !matchedTopics.includes(o.topic)) return false;
      if (entityFilter && o.entity_id !== entityFilter && o.entity_id != null) return false;
      return true;
    })
    .map((o) => {
      const text = (o.text || "").toLowerCase();
      let score = searchTerms.reduce((s, t) => s + (text.includes(t) ? 1 : 0), 0);
      if (o.direction) score += 0.5;
      return { ...o, _score: score };
    })
    .filter((o) => o._score > 0 || !searchTerms.length)
    .sort((a, b) => b._score - a._score)
    .slice(0, 10);

  // Score and filter contradictions
  const contradictions = (data.contradictions || [])
    .filter((c) => {
      if (matchedTopics.length && !matchedTopics.includes(c.topic)) return false;
      if (entityFilter) {
        // contradictions don't have entity_id directly, match via topic
      }
      return true;
    })
    .map((c) => {
      const text = ((c.claim_text || "") + " " + (c.outcome_text || "")).toLowerCase();
      let score = searchTerms.reduce((s, t) => s + (text.includes(t) ? 1 : 0), 0);
      score += c.severity || 0;
      return { ...c, _score: score };
    })
    .sort((a, b) => b._score - a._score)
    .slice(0, 10);

  // Build summary
  let summary;
  if (contradictions.length) {
    const topicStr = matchedTopics.length ? matchedTopics.join(", ") : "all topics";
    summary = `Found ${contradictions.length} contradiction(s) for ${topicStr} (worst severity: ${(contradictions[0].severity * 100).toFixed(0)}%).`;
  } else if (claims.length || outcomes.length) {
    summary = `Found ${claims.length} claim(s) and ${outcomes.length} outcome(s).`;
  } else {
    summary = "No results found for that query.";
  }

  return { summary, claims, outcomes, contradictions };
}

export function initChat() {
  const form = document.getElementById("chat-form");
  const input = document.getElementById("chat-input");
  const messages = document.getElementById("chat-messages");

  if (!form) return;

  // Clickable suggestion chips
  messages.querySelectorAll(".chat-system li").forEach((li) => {
    li.addEventListener("click", () => {
      input.value = li.textContent.replace(/^"|"$/g, "");
      form.dispatchEvent(new Event("submit"));
    });
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const query = input.value.trim();
    if (!query) return;

    // Show user message
    appendMsg("user", query);
    input.value = "";
    input.disabled = true;

    // Show typing indicator
    const typing = appendMsg("system", "Searching...");

    try {
      let data;
      if (IS_STATIC) {
        const loaded = await loadStaticData();
        data = clientSideSearch(query, loaded);
      } else {
        const resp = await fetch(`${API_BASE}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query }),
        });
        if (!resp.ok) throw new Error(`API error: ${resp.status}`);
        data = await resp.json();
      }

      typing.remove();
      renderResponse(data);
    } catch (err) {
      typing.remove();
      appendMsg("system", `Error: ${err.message}`);
    } finally {
      input.disabled = false;
      input.focus();
    }
  });

  function appendMsg(role, text) {
    const div = document.createElement("div");
    div.className = `chat-msg chat-${role}`;
    div.innerHTML = text;
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    return div;
  }

  function renderResponse(data) {
    let html = "";

    // Summary
    if (data.summary) {
      html += `<p>${data.summary}</p>`;
    }

    // Contradictions
    if (data.contradictions && data.contradictions.length) {
      html += `<div class="chat-section"><strong>Contradictions found (${data.contradictions.length}):</strong>`;
      for (const c of data.contradictions.slice(0, 5)) {
        const sevClass =
          c.severity > 0.6
            ? "severity-high"
            : c.severity > 0.3
              ? "severity-mid"
              : "severity-low";
        const claimLink = c.claim_url ? ` <a href="${c.claim_url}" target="_blank" class="source-link">source</a>` : "";
        const outcomeLink = c.outcome_url ? ` <a href="${c.outcome_url}" target="_blank" class="source-link">source</a>` : "";
        html += `
          <div class="chat-card">
            <span class="severity ${sevClass}">${(c.severity * 100).toFixed(0)}%</span>
            <div class="chat-claim">"${c.claim_text}"${claimLink}</div>
            <div class="chat-outcome">${c.outcome_text}${outcomeLink}</div>
          </div>`;
      }
      html += "</div>";
    }

    // Claims
    if (data.claims && data.claims.length) {
      html += `<div class="chat-section"><strong>Claims (${data.claims.length}):</strong>`;
      for (const c of data.claims.slice(0, 5)) {
        const link = c.source_url ? ` <a href="${c.source_url}" target="_blank" class="source-link">source</a>` : "";
        html += `<div class="chat-item chat-item-claim">${c.date}: ${c.text}${link}</div>`;
      }
      if (data.claims.length > 5) {
        html += `<div class="chat-more">...and ${data.claims.length - 5} more</div>`;
      }
      html += "</div>";
    }

    // Outcomes
    if (data.outcomes && data.outcomes.length) {
      html += `<div class="chat-section"><strong>Outcomes (${data.outcomes.length}):</strong>`;
      for (const o of data.outcomes.slice(0, 5)) {
        const dir = o.direction
          ? `<span class="detail-dir detail-dir-${o.direction}">${o.direction}</span> `
          : "";
        const link = o.source_url ? ` <a href="${o.source_url}" target="_blank" class="source-link">source</a>` : "";
        html += `<div class="chat-item chat-item-outcome">${dir}${o.text}${link}</div>`;
      }
      if (data.outcomes.length > 5) {
        html += `<div class="chat-more">...and ${data.outcomes.length - 5} more</div>`;
      }
      html += "</div>";
    }

    if (!html) {
      html = "<p>No results found. Try a different question.</p>";
    }

    appendMsg("assistant", html);
  }
}
