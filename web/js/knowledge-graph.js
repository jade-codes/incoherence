/**
 * Force-directed knowledge graph visualization.
 *
 * Features: zoom/pan, topic filter, click-to-inspect, colour by topic,
 * severity-weighted contradiction edges.
 */

const TOPIC_COLORS = {
  housing: "#f59e0b",
  health: "#ef4444",
  poverty: "#dc2626",
  climate: "#10b981",
  transport: "#6366f1",
  education: "#8b5cf6",
  regeneration: "#0ea5e9",
  flooding: "#06b6d4",
  democracy: "#ec4899",
  economy: "#f97316",
  community: "#14b8a6",
  transparency: "#a855f7",
};

export function renderKnowledgeGraph(selector, data) {
  const container = document.querySelector(selector);
  container.innerHTML = "";

  if (!data.nodes || !data.links || !data.nodes.length) {
    container.innerHTML =
      '<p style="color:#888;text-align:center;padding:2rem;">No graph data available.</p>';
    return;
  }

  // ── Controls bar ──
  const controls = document.createElement("div");
  controls.className = "graph-controls";
  controls.innerHTML = `
    <select id="graph-topic-filter">
      <option value="all">All topics</option>
      ${[...new Set(data.nodes.map((n) => n.topic).filter(Boolean))]
        .sort()
        .map((t) => `<option value="${t}">${t}</option>`)
        .join("")}
    </select>
    <label class="graph-toggle">
      <input type="checkbox" id="graph-labels" checked> Labels
    </label>
    <label class="graph-toggle">
      <input type="checkbox" id="graph-only-contradictions"> Only contradictions
    </label>
    <button id="graph-zoom-in" class="graph-btn">+</button>
    <button id="graph-zoom-out" class="graph-btn">-</button>
    <button id="graph-reset-zoom" class="graph-btn">Reset view</button>
  `;
  container.appendChild(controls);

  // ── Detail panel ──
  const detail = document.createElement("div");
  detail.id = "graph-detail";
  detail.className = "graph-detail hidden";
  container.appendChild(detail);

  // ── SVG ──
  const width = container.clientWidth;
  const height = Math.max(550, container.clientHeight - 50);

  const svg = d3
    .select(selector)
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  // Zoom group
  const g = svg.append("g");

  const zoom = d3
    .zoom()
    .scaleExtent([0.2, 5])
    .filter((event) => {
      // Allow wheel events always, allow drag only on SVG background (not nodes)
      if (event.type === "wheel") return true;
      if (event.type === "dblclick") return true;
      // For pointer/mouse events, only zoom-pan if not on a node
      return !event.target.closest(".node");
    })
    .on("zoom", (event) => g.attr("transform", event.transform));

  svg.call(zoom);
  // Ensure SVG captures wheel events
  svg.on("wheel.zoom", null);
  svg.call(zoom);

  // ── Legend ──
  const legend = g.append("g").attr("transform", "translate(16, 16)");
  const legendItems = [
    { label: "Entity", color: "#4a9eff", r: 10 },
    { label: "Claim", color: "#22c55e", r: 6 },
    { label: "Outcome", color: "#ef4444", r: 6 },
  ];
  legendItems.forEach((item, i) => {
    legend
      .append("circle")
      .attr("cx", 6)
      .attr("cy", i * 22)
      .attr("r", item.r)
      .attr("fill", item.color);
    legend
      .append("text")
      .attr("x", 22)
      .attr("y", i * 22 + 5)
      .attr("fill", "#888")
      .attr("font-size", "11px")
      .text(item.label);
  });

  // ── Build simulation ──
  let currentNodes = [...data.nodes];
  let currentLinks = [...data.links];

  const simulation = d3
    .forceSimulation(currentNodes)
    .force(
      "link",
      d3
        .forceLink(currentLinks)
        .id((d) => d.id)
        .distance((d) => (d.relationship === "contradicted" ? 120 : 60))
    )
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide(20));

  // ── Draw elements ──
  let linkG = g.append("g").attr("class", "links");
  let nodeG = g.append("g").attr("class", "nodes");

  let linkEls, nodeEls, labelEls;

  function nodeColor(d) {
    if (d.type === "entity") return "#4a9eff";
    if (d.topic && TOPIC_COLORS[d.topic]) return TOPIC_COLORS[d.topic];
    return d.type === "claim" ? "#22c55e" : "#ef4444";
  }

  function nodeRadius(d) {
    if (d.type === "entity") return 14;
    return 7;
  }

  function render() {
    // Links
    linkEls = linkG
      .selectAll("line")
      .data(currentLinks, (d) => `${d.source.id || d.source}-${d.target.id || d.target}`)
      .join("line")
      .attr("stroke", (d) =>
        d.relationship === "contradicted"
          ? "#ef4444"
          : d.relationship === "claims"
            ? "#4a9eff33"
            : "#333"
      )
      .attr("stroke-width", (d) =>
        d.severity ? 1 + d.severity * 3 : 1
      )
      .attr("stroke-dasharray", (d) =>
        d.relationship === "contradicted" ? "6 3" : null
      )
      .attr("stroke-opacity", (d) =>
        d.relationship === "contradicted" ? 0.8 : 0.3
      );

    // Nodes
    nodeEls = nodeG
      .selectAll("g.node")
      .data(currentNodes, (d) => d.id)
      .join(
        (enter) => {
          const g = enter.append("g").attr("class", "node").call(drag(simulation));

          g.append("circle")
            .attr("r", nodeRadius)
            .attr("fill", nodeColor)
            .attr("stroke", "#000")
            .attr("stroke-width", 1);

          g.append("text")
            .attr("class", "node-label")
            .attr("dx", (d) => nodeRadius(d) + 4)
            .attr("dy", 4)
            .attr("fill", "#ccc")
            .attr("font-size", (d) => (d.type === "entity" ? "12px" : "10px"))
            .attr("font-weight", (d) => (d.type === "entity" ? "600" : "400"))
            .text((d) => {
              if (d.type === "entity") return d.label;
              return (d.label || "").substring(0, 40) + (d.label && d.label.length > 40 ? "..." : "");
            });

          g.on("click", (event, d) => showDetail(d));

          return g;
        },
        (update) => update,
        (exit) => exit.remove()
      );

    // Toggle labels
    const showLabels = document.getElementById("graph-labels")?.checked ?? true;
    nodeG.selectAll(".node-label").attr("display", showLabels ? null : "none");
  }

  render();

  simulation.on("tick", () => {
    linkEls
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    nodeEls.attr("transform", (d) => `translate(${d.x},${d.y})`);
  });

  // ── Detail panel ──
  function showDetail(d) {
    detail.classList.remove("hidden");

    const topicBadge = d.topic
      ? `<span class="detail-topic" style="background:${TOPIC_COLORS[d.topic] || '#555'}">${d.topic}</span>`
      : "";

    let extra = "";
    if (d.type === "claim") {
      extra = `<div class="detail-label">Claim</div>`;
    } else if (d.type === "outcome") {
      const dir = d.direction
        ? `<span class="detail-dir detail-dir-${d.direction}">${d.direction}</span>`
        : "";
      extra = `<div class="detail-label">Outcome ${dir}</div>`;
    } else {
      extra = `<div class="detail-label">Entity</div>`;
    }

    // Find connected contradictions
    const contradictions = data.links.filter(
      (l) =>
        l.relationship === "contradicted" &&
        ((l.source.id || l.source) === d.id || (l.target.id || l.target) === d.id)
    );

    let contraHTML = "";
    if (contradictions.length) {
      contraHTML = `<div class="detail-section"><strong>${contradictions.length} contradiction(s):</strong>`;
      for (const c of contradictions.slice(0, 5)) {
        const otherId =
          (c.source.id || c.source) === d.id
            ? c.target.id || c.target
            : c.source.id || c.source;
        const other = data.nodes.find((n) => n.id === otherId);
        const sev = c.severity ? `${(c.severity * 100).toFixed(0)}%` : "?";
        const otherLink = other && other.source_url
          ? `<a href="${other.source_url}" target="_blank" rel="noopener" class="source-link">source</a>`
          : "";
        contraHTML += `<div class="detail-contra">
          <span class="severity severity-${c.severity > 0.6 ? "high" : c.severity > 0.3 ? "mid" : "low"}">${sev}</span>
          ${other ? other.label : otherId}
          ${otherLink}
        </div>`;
      }
      contraHTML += "</div>";
    }

    const sourceHTML = d.source_url
      ? `<a href="${d.source_url}" target="_blank" rel="noopener" class="source-link">View source</a>`
      : "";

    detail.innerHTML = `
      <button class="detail-close" onclick="this.parentElement.classList.add('hidden')">x</button>
      ${extra}
      ${topicBadge}
      ${d.date ? `<div class="detail-date">${d.date}</div>` : ""}
      <div class="detail-text">${d.label || d.id}</div>
      ${sourceHTML}
      ${contraHTML}
    `;
  }

  // ── Filter handlers ──
  function applyFilters() {
    const topic = document.getElementById("graph-topic-filter").value;
    const onlyContra = document.getElementById("graph-only-contradictions").checked;

    let filteredNodes = data.nodes;
    let filteredLinks = data.links;

    if (topic !== "all") {
      const topicNodeIds = new Set(
        data.nodes.filter((n) => n.topic === topic || n.type === "entity").map((n) => n.id)
      );
      filteredLinks = data.links.filter(
        (l) =>
          topicNodeIds.has(l.source.id || l.source) ||
          topicNodeIds.has(l.target.id || l.target)
      );
      // Include all nodes connected by remaining links
      const connectedIds = new Set();
      filteredLinks.forEach((l) => {
        connectedIds.add(l.source.id || l.source);
        connectedIds.add(l.target.id || l.target);
      });
      filteredNodes = data.nodes.filter((n) => connectedIds.has(n.id));
    }

    if (onlyContra) {
      const contraLinks = filteredLinks.filter((l) => l.relationship === "contradicted");
      const contraIds = new Set();
      contraLinks.forEach((l) => {
        contraIds.add(l.source.id || l.source);
        contraIds.add(l.target.id || l.target);
      });
      // Also include entities connected to those nodes
      const entityLinks = filteredLinks.filter(
        (l) =>
          l.relationship !== "contradicted" &&
          (contraIds.has(l.source.id || l.source) || contraIds.has(l.target.id || l.target))
      );
      entityLinks.forEach((l) => {
        contraIds.add(l.source.id || l.source);
        contraIds.add(l.target.id || l.target);
      });
      filteredNodes = filteredNodes.filter((n) => contraIds.has(n.id));
      filteredLinks = filteredLinks.filter(
        (l) =>
          contraIds.has(l.source.id || l.source) && contraIds.has(l.target.id || l.target)
      );
    }

    currentNodes = filteredNodes;
    currentLinks = filteredLinks;

    simulation.nodes(currentNodes);
    simulation.force("link").links(currentLinks);
    simulation.alpha(0.8).restart();
    render();
  }

  document
    .getElementById("graph-topic-filter")
    .addEventListener("change", applyFilters);
  document
    .getElementById("graph-only-contradictions")
    .addEventListener("change", applyFilters);
  document
    .getElementById("graph-labels")
    .addEventListener("change", () => {
      const show = document.getElementById("graph-labels").checked;
      nodeG.selectAll(".node-label").attr("display", show ? null : "none");
    });
  document
    .getElementById("graph-zoom-in")
    .addEventListener("click", () => {
      svg.transition().duration(300).call(zoom.scaleBy, 1.5);
    });
  document
    .getElementById("graph-zoom-out")
    .addEventListener("click", () => {
      svg.transition().duration(300).call(zoom.scaleBy, 0.67);
    });
  document
    .getElementById("graph-reset-zoom")
    .addEventListener("click", () => {
      svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity);
    });

  function drag(simulation) {
    return d3
      .drag()
      .on("start", (event, d) => {
        // Stop this drag from triggering zoom pan
        event.sourceEvent.stopPropagation();
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on("drag", (event, d) => {
        d.fx = event.x;
        d.fy = event.y;
      })
      .on("end", (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      });
  }
}
