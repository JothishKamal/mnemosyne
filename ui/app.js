// The UI renders what the server computed. It never aggregates, buckets, or
// infers anything itself: bucketing lives in replay/, entirely server-side, so
// that this page and the future `mnemo replay` CLI read the same model and
// cannot silently diverge.
//
// A note on wire shapes: the collector encodes the Timeline Model
// (replay.TimelineModel and friends) with explicit camelCase JSON tags, and
// any embedded protobuf message (*v1.Event, *v1.EventID) is marshaled with
// protojson before being embedded in the response, so it speaks the same
// convention: camelCase field names (`operationType`, `traceId`), and the
// `operation` oneof flattened directly onto the event (`httpServer: {...}` /
// `httpClient: {...}`) rather than wrapped in a Go-cased type name. protojson
// also renders enums as their name string (e.g. `"HTTP_SERVER"`), not their
// number. This file reads exactly those shapes, nothing is reformatted or
// reinterpreted, only displayed.

const $ = (id) => document.getElementById(id);

async function json(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status} ${await r.text()}`);
  return r.json();
}

const OPERATION_TYPE_NAMES = {
  OPERATION_TYPE_UNSPECIFIED: "unspecified",
  HTTP_SERVER: "http.server",
  HTTP_CLIENT: "http.client",
  LOCAL: "local",
  DB_QUERY: "db.query",
  CACHE_OP: "cache",
  MSG_PRODUCE: "msg.produce",
  MSG_CONSUME: "msg.consume",
};

const state = {
  model: null,
  traces: [],
};

// --- bootstrap ---------------------------------------------------------

$("load").addEventListener("click", () => loadAll().catch(reportError));

function reportError(e) {
  $("status").textContent = e.message;
  $("status").classList.add("status--error");
}

async function loadAll() {
  $("status").classList.remove("status--error");
  $("status").textContent = "loading…";

  const from = $("from").value || 0;
  const to = $("to").value || Date.now();
  const resolution = Math.max(60, Math.floor($("timeline").clientWidth / 4) || 60);

  const [model, traces] = await Promise.all([
    json(`/api/timeline?from=${from}&to=${to}&resolution=${resolution}`),
    json(`/api/traces?from=${from}&to=${to}&limit=50`).catch(() => []),
  ]);

  state.model = model;
  state.traces = traces || [];

  renderSkew(model);
  renderLanes(model);
  renderGaps(model);
  renderInFlight(model);
  populateTraces(state.traces);

  const parts = [`${(model.lanes ?? []).length} lanes`];
  if ((model.inFlight ?? []).length) parts.push(`${model.inFlight.length} never ended`);
  if ((model.gaps ?? []).length) parts.push(`${model.gaps.length} gaps`);
  if ((model.skewWarnings ?? []).length) parts.push(`${model.skewWarnings.length} skew warnings`);
  $("status").textContent = parts.join(" · ");
}

// --- lanes ---------------------------------------------------------------

function renderLanes(m) {
  const host = $("lanes");
  host.replaceChildren();

  const skewedServices = new Set((m.skewWarnings ?? []).map((s) => s.service));

  for (const lane of m.lanes ?? []) {
    const row = document.createElement("div");
    row.className = "lane";
    if (skewedServices.has(lane.service)) row.classList.add("lane--skewed");

    const label = document.createElement("span");
    label.className = "lane-label";
    label.textContent = `${lane.service} · ${lane.resource}`;
    row.append(label);

    const track = document.createElement("div");
    track.className = "lane-track";
    for (const b of lane.buckets) {
      if (b.count === 0) continue;
      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = b.errorCount > 0 ? "bucket bucket--error" : "bucket";
      cell.style.left = `${(b.index / m.resolution) * 100}%`;
      cell.style.width = `${Math.max(100 / m.resolution, 0.4)}%`;
      const durMs = (b.maxDurationNanos / 1e6).toFixed(1);
      cell.title = `${b.count} operation${b.count === 1 ? "" : "s"}, ${b.errorCount} error${b.errorCount === 1 ? "" : "s"}, max ${durMs}ms`;
      cell.setAttribute("aria-label", cell.title);
      track.append(cell);
    }
    row.append(track);
    host.append(row);
  }

  if ((m.lanes ?? []).length === 0) {
    const empty = document.createElement("p");
    empty.className = "hint";
    empty.textContent = "No events in this window.";
    host.append(empty);
  }
}

// Gap markers render inline, at the exact point on the timeline where events
// were lost, never as a footnote and never silently. A blank stretch of
// timeline is indistinguishable from "nothing happened", which is the worst
// thing this tool could show an operator staring down an incident.
function renderGaps(m) {
  const host = $("gaps");
  host.replaceChildren();

  const span = m.toWallMs - m.fromWallMs || 1;
  for (const g of m.gaps ?? []) {
    const el = document.createElement("div");
    el.className = "gap";
    el.setAttribute("role", "alert");
    el.style.left = `${Math.max(0, ((g.fromWallMs - m.fromWallMs) / span) * 100)}%`;
    el.style.width = `${Math.max(0.6, ((g.toWallMs - g.fromWallMs) / span) * 100)}%`;

    const label = document.createElement("span");
    label.className = "gap-label";
    label.textContent = `${g.droppedCount} event${g.droppedCount === 1 ? "" : "s"} dropped (${g.reason})`;
    el.append(label);
    host.append(el);
  }
}

// A skewed clock is reported, never silently rendered as an impossible
// timeline. This banner sits above the lanes; the affected lane rows are also
// outlined (see .lane--skewed in style.css) so the warning is anchored to
// where the geometry can't be trusted.
function renderSkew(m) {
  const host = $("skew");
  host.replaceChildren();
  for (const s of m.skewWarnings ?? []) {
    const el = document.createElement("div");
    el.className = "skew";
    el.setAttribute("role", "alert");
    el.textContent = `clock skew: ${s.service} is ${s.observedSkewMs}ms behind its caller. Ordering for this service is not trustworthy at wall-clock resolution`;
    host.append(el);
  }
}

// In-flight operations, a BEGIN with no matching END, render with an
// explicit "never ended" marker, visually distinct from anything that
// completed. This is how a hang or a deadlock looks in a recording: the
// timeline must not go quiet exactly where the answer is.
function renderInFlight(m) {
  const host = $("inflight-panel");
  host.replaceChildren();

  if ((m.inFlight ?? []).length === 0) return;

  const heading = document.createElement("h2");
  heading.className = "inflight-heading";
  heading.textContent = "Never ended";
  host.append(heading);

  const list = document.createElement("ul");
  list.className = "inflight-list";
  for (const op of m.inFlight ?? []) {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "inflight";
    btn.title = "Load the recorded causal chain for this operation";

    const tag = document.createElement("span");
    tag.className = "tag tag--inflight";
    tag.textContent = "never ended";
    btn.append(tag);

    const desc = document.createElement("span");
    desc.className = "inflight-desc";
    desc.textContent = `${op.service} · ${op.description}`;
    btn.append(desc);

    btn.addEventListener("click", () => loadChain(op.eventId).catch(reportError));
    li.append(btn);
    list.append(li);
  }
  host.append(list);
}

// --- traces ----------------------------------------------------------------

// /api/chain needs a trace ID; the Timeline Model does not attach one to each
// in-flight or bucketed operation (buckets are aggregates with no event
// identity at all). We resolve it through /api/traces and let the operator's
// selection, defaulted to the first trace in the window, scope chain
// lookups, rather than guessing or fabricating an identity client-side.
function populateTraces(traces) {
  const sel = $("trace");
  const previous = sel.value;
  sel.replaceChildren();

  if ((traces ?? []).length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "no traces in window";
    sel.append(opt);
    sel.disabled = true;
    return;
  }

  sel.disabled = false;
  for (const t of traces ?? []) {
    const hex = toHex(t.TraceID);
    const opt = document.createElement("option");
    opt.value = hex;
    const errorMark = t.HasError ? " ⚠" : "";
    opt.textContent = `${t.RootService} ${t.RootRoute} (${t.EventCount} events)${errorMark}`;
    sel.append(opt);
  }
  if (traces.some((t) => toHex(t.TraceID) === previous)) {
    sel.value = previous;
  } else {
    sel.selectedIndex = 0;
  }
}

// --- causal chain ------------------------------------------------------

async function loadChain(eventId) {
  const traceHex = $("trace").value;
  if (!traceHex) {
    throw new Error("select a trace before loading its causal chain");
  }
  const emitter = toHex(eventId.emitterId);
  $("status").textContent = "loading chain…";
  const chain = await json(
    `/api/chain?trace=${traceHex}&emitter=${emitter}&seq=${eventId.sequence}&max=200`
  );
  renderChain(chain);
  const chainNodes = chain.nodes ?? [];
  $("status").textContent = `chain: ${chainNodes.length} node${chainNodes.length === 1 ? "" : "s"}${chain.truncated ? " (truncated)" : ""}`;
}

function renderChain(chain) {
  $("chain-hint").hidden = true;
  const host = $("chain");
  host.replaceChildren();

  // Nodes arrive root-first-ish from the engine's BFS; reverse so the panel
  // reads top-to-bottom as "what happened first" -> "what it caused".
  const nodes = [...chain.nodes].reverse();

  for (const node of nodes ?? []) {
    const li = document.createElement("li");
    li.className = "chain-node";
    // Every edge shown here was observed, v1 renders only recorded edges.
    // The attribute (and its dedicated CSS treatment) exist now so that
    // adding inferred edges later cannot quietly blur a fact with a guess.
    li.dataset.provenance = "recorded";
    li.dataset.edge = node.edgeFromParent || "root";

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chain-node-btn";

    const edge = document.createElement("span");
    edge.className = "chain-edge";
    edge.textContent = node.edgeFromParent || "selected";
    btn.append(edge);

    const title = document.createElement("span");
    title.className = "chain-title";
    title.textContent = describe(node.event);
    btn.append(title);

    // HLC never appears here, only in the raw inspector below, opened by
    // clicking a node. Without that escape hatch an engineer who disagrees
    // with an ordering decision has no way to find out why.
    if (!node.event.durationNanos) {
      const tag = document.createElement("span");
      tag.className = "tag tag--inflight";
      tag.textContent = "never ended";
      btn.append(tag);
    }

    btn.addEventListener("click", () => showRaw(node.event));
    li.append(btn);
    host.append(li);
  }

  if (chain.truncated) {
    const li = document.createElement("li");
    li.className = "chain-truncated";
    li.textContent = "chain truncated. More causally related events exist";
    host.append(li);
  }

  if ((nodes ?? []).length === 0) {
    const li = document.createElement("li");
    li.className = "hint";
    li.textContent = "No recorded chain for this event.";
    host.append(li);
  }
}

// describe() reads the real wire shape of a *v1.Event as protojson renders
// it: camelCase fields, and the operation oneof flattened directly onto the
// event as httpServer / httpClient.
function describe(ev) {
  const service = ev.service?.name ?? "unknown-service";

  if (ev.httpServer) {
    return `${service}: ${ev.httpServer.method} ${ev.httpServer.route || ev.httpServer.path}`;
  }
  if (ev.httpClient) {
    return `${service} → ${ev.httpClient.host}${ev.httpClient.path}`;
  }

  const localName = ev.attributes?.["operation.name"]?.stringValue;
  if (localName) return `${service}: ${localName}`;

  const opType = ev.operationType ?? "OPERATION_TYPE_UNSPECIFIED";
  const kind = OPERATION_TYPE_NAMES[opType] ?? `operation type ${opType}`;
  return `${service}: ${kind}`;
}

// HLC belongs in the inspector, never the narrative. Without it, an engineer
// who disagrees with an ordering has no way to find out why.
function showRaw(ev) {
  $("inspector").hidden = false;
  $("raw").textContent = JSON.stringify(ev, null, 2);
  $("inspector").scrollIntoView({ block: "nearest", behavior: "smooth" });
}

function toHex(b64) {
  if (!b64) return "";
  const bin = atob(b64);
  let hex = "";
  for (let i = 0; i < bin.length; i++) {
    hex += bin.charCodeAt(i).toString(16).padStart(2, "0");
  }
  return hex;
}
