/**
 * Coherence scores by topic — horizontal bar chart.
 *
 * Shows how coherent each topic is, with danger zone highlighting.
 */

export function renderCoherenceChart(selector, data) {
  console.log("renderCoherenceChart called", selector, data?.length);
  const container = document.querySelector(selector);
  if (!container) { console.error("No container for", selector); return; }
  container.innerHTML = "";
  console.log("Container width:", container.clientWidth);

  const width = container.clientWidth || container.parentElement?.clientWidth || 900;
  const margin = { top: 30, right: 120, bottom: 40, left: 200 };

  if (!data || !data.length) {
    container.innerHTML = '<p style="color:#888;text-align:center;padding:2rem;">No coherence data available.</p>';
    return;
  }

  // Deduplicate: keep one entry per entity+topic, prefer lowest score
  const key = (d) => `${d.entity_id}:${d.topic}`;
  const best = new Map();
  data.forEach((d) => {
    const k = key(d);
    if (!best.has(k) || d.score < best.get(k).score) {
      best.set(k, d);
    }
  });
  const items = [...best.values()].sort((a, b) => a.score - b.score);

  if (!items.length) {
    container.innerHTML = '<p style="color:#888;text-align:center;padding:2rem;">No coherence data available.</p>';
    return;
  }

  const height = Math.max(400, items.length * 34 + 80);

  const svg = d3
    .select(selector)
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  const labels = items.map(
    (d) =>
      `${d.topic} (${d.entity_id === "hull-cc" ? "Hull" : d.entity_id === "east-riding" ? "East Riding" : d.entity_id})`
  );

  const chartHeight = height - margin.top - margin.bottom;
  const barHeight = Math.min(30, chartHeight / items.length - 4);

  const innerWidth = width - margin.left - margin.right;

  const x = d3
    .scaleLinear()
    .domain([0, 1])
    .range([0, innerWidth]);

  const y = d3
    .scaleBand()
    .domain(labels)
    .range([margin.top, margin.top + items.length * (barHeight + 4)])
    .padding(0.1);

  const chartBottom = margin.top + items.length * (barHeight + 4);

  // Chart group offset by margin
  const g = svg.append("g").attr("transform", `translate(${margin.left}, 0)`);

  // Danger zone background (below 0.5)
  g.append("rect")
    .attr("x", 0)
    .attr("y", margin.top)
    .attr("width", x(0.5))
    .attr("height", chartBottom - margin.top)
    .attr("fill", "#ef4444")
    .attr("opacity", 0.05);

  // Warning zone (0.5-0.7)
  g.append("rect")
    .attr("x", x(0.5))
    .attr("y", margin.top)
    .attr("width", x(0.7) - x(0.5))
    .attr("height", chartBottom - margin.top)
    .attr("fill", "#eab308")
    .attr("opacity", 0.03);

  // Threshold lines
  [0.5, 0.7].forEach((v) => {
    g.append("line")
      .attr("x1", x(v))
      .attr("y1", margin.top)
      .attr("x2", x(v))
      .attr("y2", chartBottom)
      .attr("stroke", "#333")
      .attr("stroke-dasharray", "4 2");

    g.append("text")
      .attr("x", x(v))
      .attr("y", margin.top - 8)
      .attr("text-anchor", "middle")
      .attr("fill", "#555")
      .attr("font-size", "10px")
      .text(v === 0.5 ? "Incoherent" : "Coherent");
  });

  // Bars
  g.selectAll(".bar")
    .data(items)
    .join("rect")
    .attr("class", "bar")
    .attr("x", 0)
    .attr("y", (d, i) => y(labels[i]))
    .attr("width", (d) => Math.max(0, x(d.score)))
    .attr("height", barHeight)
    .attr("rx", 3)
    .attr("fill", (d) =>
      d.score < 0.4
        ? "#ef4444"
        : d.score < 0.7
          ? "#eab308"
          : "#22c55e"
    )
    .attr("opacity", 0.8);

  // Labels (topic names) — to the left of the chart
  svg
    .selectAll(".label")
    .data(items)
    .join("text")
    .attr("x", margin.left - 8)
    .attr("y", (d, i) => y(labels[i]) + barHeight / 2 + 4)
    .attr("text-anchor", "end")
    .attr("fill", "#ccc")
    .attr("font-size", "12px")
    .text((d, i) => labels[i]);

  // Score labels + contradiction count
  g.selectAll(".score")
    .data(items)
    .join("text")
    .attr("x", (d) => x(d.score) + 6)
    .attr("y", (d, i) => y(labels[i]) + barHeight / 2 + 4)
    .attr("fill", "#888")
    .attr("font-size", "11px")
    .text(
      (d) =>
        `${(d.score * 100).toFixed(0)}% (${d.n_contradictions} contradictions)`
    );

  // X axis
  g.append("g")
    .attr("transform", `translate(0,${chartBottom})`)
    .call(
      d3
        .axisBottom(x)
        .ticks(5)
        .tickFormat((d) => `${(d * 100).toFixed(0)}%`)
    )
    .selectAll("text")
    .attr("fill", "#888");

  g.selectAll(".domain, .tick line").attr("stroke", "#2a2a2a");
}
