/**
 * Incoherence Detector — Dashboard App
 */

import { renderKnowledgeGraph } from "./knowledge-graph.js";
import { renderTimeline } from "./timeline.js";
import { renderCoherenceChart } from "./coherence-chart.js";
import { renderContradictions } from "./contradiction-panel.js";
import { initChat } from "./chat.js";

// Detect if we're served by FastAPI (live API) or static hosting (pre-rendered JSON).
// Try the live endpoint first — if it 404s with .json-less URL, fall back to static.
let IS_STATIC = null; // determined on first fetch
const API_BASE = "/api";

// Store loaded data globally so tabs can re-render
let dashboardData = {};

// Tab switching — render deferred content when tab becomes visible
document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document
      .querySelectorAll(".tab-content")
      .forEach((c) => c.classList.remove("active"));

    tab.classList.add("active");
    document.getElementById(`tab-${tab.dataset.tab}`).classList.add("active");

    // Render deferred charts after the tab is visible
    if (tab.dataset.tab === "coherence" && dashboardData.coherence) {
      requestAnimationFrame(() => {
        renderCoherenceChart("#coherence-chart", dashboardData.coherence);
      });
    }
    if (tab.dataset.tab === "timeline" && !dashboardData.timelineLoaded) {
      loadTimeline();
    }
  });
});

async function fetchJSON(path) {
  // Auto-detect live vs static on first call
  if (IS_STATIC === null) {
    try {
      const probe = await fetch(`${API_BASE}/config`);
      IS_STATIC = !probe.ok;
    } catch {
      IS_STATIC = true;
    }
  }
  const url = IS_STATIC
    ? `${API_BASE}${path.split("?")[0]}.json`
    : `${API_BASE}${path}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`API error: ${resp.status}`);
  return resp.json();
}

async function loadTimeline() {
  const topicSelect = document.getElementById("timeline-topic");
  const topic = topicSelect.value;
  const data = await fetchJSON(`/timeline?topic=${topic}`);
  renderTimeline("#timeline-chart", data);
  dashboardData.timelineLoaded = true;
}

async function loadConfig() {
  try {
    const config = await fetchJSON("/config");
    // Update page title
    const titleEl = document.getElementById("site-title");
    if (titleEl && config.name) {
      titleEl.textContent = `${config.name} Incoherence Detector`;
      document.title = `${config.name} Incoherence Detector`;
    }
    // Update chat examples
    if (config.chat_examples && config.chat_examples.length) {
      const ul = document.getElementById("chat-examples");
      if (ul) {
        ul.innerHTML = config.chat_examples
          .map((ex) => `<li>"${ex}"</li>`)
          .join("");
      }
    }
    return config;
  } catch {
    // Config endpoint may not exist in static mode — that's fine
    return null;
  }
}

async function loadDashboard() {
  try {
    // Load config first for dynamic titles
    await loadConfig();

    const [graph, coherence, contradictions] = await Promise.all([
      fetchJSON("/graph"),
      fetchJSON("/coherence-history"),
      fetchJSON("/contradictions"),
    ]);

    dashboardData.coherence = coherence;

    renderKnowledgeGraph("#knowledge-graph", graph);
    renderContradictions("#contradiction-panel", contradictions);

    // Timeline loads on topic change
    const topicSelect = document.getElementById("timeline-topic");
    topicSelect.addEventListener("change", loadTimeline);

    // Init chat
    initChat();
    console.log("Dashboard loaded:", Object.keys(dashboardData));
  } catch (err) {
    console.error("Failed to load dashboard:", err);
  }
}

loadDashboard();
