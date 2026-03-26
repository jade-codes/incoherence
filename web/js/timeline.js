/**
 * Claims vs Outcomes timeline visualization.
 *
 * Claims appear above the axis, outcomes below.
 * Curved arcs connect contradicting claim/outcome pairs.
 */

export function renderTimeline(selector, data) {
  const container = document.querySelector(selector);
  container.innerHTML = "";

  const width = container.clientWidth;
  const height = Math.max(550, container.clientHeight);
  const margin = { top: 50, right: 40, bottom: 50, left: 60 };

  const svg = d3
    .select(selector)
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  if (!data.claims || !data.outcomes || (!data.claims.length && !data.outcomes.length)) {
    svg
      .append("text")
      .attr("x", width / 2)
      .attr("y", height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .text("No timeline data available.");
    return;
  }

  // Filter out items with no valid date
  const claims = data.claims.filter((d) => d.date && !isNaN(new Date(d.date)));
  const outcomes = data.outcomes.filter((d) => d.date && !isNaN(new Date(d.date)));

  const allDates = [
    ...claims.map((d) => new Date(d.date)),
    ...outcomes.map((d) => new Date(d.date)),
  ];

  if (!allDates.length) {
    svg
      .append("text")
      .attr("x", width / 2)
      .attr("y", height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .text("No dated items to display.");
    return;
  }

  const x = d3
    .scaleTime()
    .domain(d3.extent(allDates))
    .range([margin.left, width - margin.right])
    .nice();

  const midY = height / 2;
  const claimZone = { top: margin.top, bottom: midY - 15 };
  const outcomeZone = { top: midY + 15, bottom: height - margin.bottom };

  // ── Axis ──
  svg
    .append("line")
    .attr("x1", margin.left)
    .attr("y1", midY)
    .attr("x2", width - margin.right)
    .attr("y2", midY)
    .attr("stroke", "#2a2a2a")
    .attr("stroke-width", 1);

  svg
    .append("g")
    .attr("transform", `translate(0,${midY})`)
    .call(d3.axisBottom(x).ticks(8))
    .selectAll("text")
    .attr("fill", "#888");

  svg.selectAll(".domain").attr("stroke", "#2a2a2a");
  svg.selectAll(".tick line").attr("stroke", "#2a2a2a");

  // ── Labels ──
  svg
    .append("text")
    .attr("x", margin.left)
    .attr("y", margin.top - 20)
    .attr("fill", "#4a9eff")
    .attr("font-size", "12px")
    .attr("font-weight", "600")
    .text("Claims (what they said)");

  svg
    .append("text")
    .attr("x", margin.left)
    .attr("y", height - margin.bottom + 35)
    .attr("fill", "#ef4444")
    .attr("font-size", "12px")
    .attr("font-weight", "600")
    .text("Outcomes (what happened)");

  // ── Position claims (above axis) with deterministic y spread ──
  // Sort by date, then spread vertically using index
  claims.sort((a, b) => new Date(a.date) - new Date(b.date));
  const claimHeight = claimZone.bottom - claimZone.top;
  const claimPositions = new Map();

  claims.forEach((d, i) => {
    const cx = x(new Date(d.date));
    // Spread vertically: alternate between bands to reduce overlap
    const band = (i % 4) / 4;
    const cy = claimZone.top + claimHeight * (0.15 + band * 0.7);
    claimPositions.set(d.id, { cx, cy });
  });

  // ── Position outcomes (below axis) ──
  outcomes.sort((a, b) => new Date(a.date) - new Date(b.date));
  const outcomeHeight = outcomeZone.bottom - outcomeZone.top;
  const outcomePositions = new Map();

  outcomes.forEach((d, i) => {
    const cx = x(new Date(d.date));
    const band = (i % 4) / 4;
    const cy = outcomeZone.top + outcomeHeight * (0.15 + band * 0.7);
    outcomePositions.set(d.id, { cx, cy });
  });

  // ── Draw contradiction arcs FIRST (behind dots) ──
  const arcGroup = svg.append("g").attr("class", "arcs");

  if (data.links) {
    const contradictions = data.links.filter((l) => l.relationship === "contradicted");

    contradictions.forEach((link) => {
      const cp = claimPositions.get(link.claim_id);
      const op = outcomePositions.get(link.outcome_id);
      if (!cp || !op) return;

      const severity = link.severity || 0.5;

      // Draw a curved arc between claim and outcome
      const midX = (cp.cx + op.cx) / 2;
      const curveOffset = (cp.cx - op.cx) * 0.3;

      arcGroup
        .append("path")
        .attr(
          "d",
          `M ${cp.cx} ${cp.cy} C ${midX + curveOffset} ${midY - 5}, ${midX - curveOffset} ${midY + 5}, ${op.cx} ${op.cy}`
        )
        .attr("fill", "none")
        .attr("stroke", "#ef4444")
        .attr("stroke-width", 1 + severity * 2)
        .attr("stroke-opacity", 0.15 + severity * 0.3)
        .attr("stroke-dasharray", "6 3")
        .append("title")
        .text(
          `Severity: ${(severity * 100).toFixed(0)}%`
        );
    });
  }

  // ── Draw claim dots ──
  const claimDots = svg
    .append("g")
    .selectAll(".claim-dot")
    .data(claims)
    .join("circle")
    .attr("class", "claim-dot")
    .attr("cx", (d) => claimPositions.get(d.id).cx)
    .attr("cy", (d) => claimPositions.get(d.id).cy)
    .attr("r", 6)
    .attr("fill", "#4a9eff")
    .attr("stroke", "#000")
    .attr("stroke-width", 0.5)
    .attr("opacity", 0.85)
    .attr("cursor", "pointer");

  claimDots.append("title").text((d) => `${d.date}\n${d.text}\n${d.source_url || ""}`);

  // Hover highlight
  claimDots
    .on("mouseover", function (event, d) {
      d3.select(this).attr("r", 9).attr("opacity", 1);
      // Highlight connected arcs
      arcGroup
        .selectAll("path")
        .attr("stroke-opacity", function () {
          return this.__data__?.claim_id === d.id ? 0.8 : 0.05;
        });
    })
    .on("mouseout", function () {
      d3.select(this).attr("r", 6).attr("opacity", 0.85);
      arcGroup.selectAll("path").attr("stroke-opacity", (l) => {
        const sev = l?.severity || 0.5;
        return 0.15 + sev * 0.3;
      });
    });

  // ── Draw outcome dots ──
  const outcomeDots = svg
    .append("g")
    .selectAll(".outcome-dot")
    .data(outcomes)
    .join("circle")
    .attr("class", "outcome-dot")
    .attr("cx", (d) => outcomePositions.get(d.id).cx)
    .attr("cy", (d) => outcomePositions.get(d.id).cy)
    .attr("r", 5)
    .attr("fill", (d) =>
      d.direction === "worsened"
        ? "#ef4444"
        : d.direction === "improved"
          ? "#22c55e"
          : "#666"
    )
    .attr("stroke", "#000")
    .attr("stroke-width", 0.5)
    .attr("opacity", 0.8)
    .attr("cursor", "pointer");

  outcomeDots.append("title").text((d) => {
    const dir = d.direction ? ` [${d.direction}]` : "";
    return `${d.date}${dir}\n${d.text}\n${d.source_url || ""}`;
  });

  // ── Click tooltip ──
  const tooltip = d3
    .select(selector)
    .append("div")
    .attr("class", "timeline-tooltip hidden");

  function showTooltip(event, d, kind) {
    const dir = d.direction ? `<span class="detail-dir detail-dir-${d.direction}">${d.direction}</span> ` : "";
    const link = d.source_url
      ? `<a href="${d.source_url}" target="_blank" rel="noopener" class="source-link">View source</a>`
      : "";
    tooltip
      .classed("hidden", false)
      .html(`
        <div class="tt-kind">${kind}</div>
        <div class="tt-date">${d.date || ""} ${dir}</div>
        <div class="tt-text">${(d.text || "").substring(0, 200)}${d.text && d.text.length > 200 ? "..." : ""}</div>
        ${link}
      `)
      .style("left", `${event.offsetX + 15}px`)
      .style("top", `${event.offsetY + 15}px`);
  }

  claimDots
    .on("click", (event, d) => showTooltip(event, d, "Claim"));
  outcomeDots
    .on("click", (event, d) => showTooltip(event, d, "Outcome"));

  // Close tooltip on background click
  svg.on("click", (event) => {
    if (event.target.tagName === "svg" || event.target.tagName === "SVG") {
      tooltip.classed("hidden", true);
    }
  });

  outcomeDots
    .on("mouseover", function (event, d) {
      d3.select(this).attr("r", 8).attr("opacity", 1);
      arcGroup
        .selectAll("path")
        .attr("stroke-opacity", function () {
          return this.__data__?.outcome_id === d.id ? 0.8 : 0.05;
        });
    })
    .on("mouseout", function () {
      d3.select(this).attr("r", 5).attr("opacity", 0.8);
      arcGroup.selectAll("path").attr("stroke-opacity", (l) => {
        const sev = l?.severity || 0.5;
        return 0.15 + sev * 0.3;
      });
    });

  // ── Outcome direction legend ──
  const oLegend = svg
    .append("g")
    .attr("transform", `translate(${width - margin.right - 130}, ${margin.top - 10})`);

  [
    { label: "Worsened", color: "#ef4444" },
    { label: "Improved", color: "#22c55e" },
    { label: "Unknown", color: "#666" },
  ].forEach((item, i) => {
    oLegend
      .append("circle")
      .attr("cx", 5)
      .attr("cy", i * 18)
      .attr("r", 5)
      .attr("fill", item.color);
    oLegend
      .append("text")
      .attr("x", 16)
      .attr("y", i * 18 + 4)
      .attr("fill", "#888")
      .attr("font-size", "10px")
      .text(item.label);
  });
}
