"use strict";

// ── constants ─────────────────────────────────────────────

const TERM_COLORS = ["#00f0ff", "#fcee0a", "#ff2a6d", "#39ff14", "#b967ff", "#ff8a00", "#4da3ff", "#ff00c8"];
const termColor = t => TERM_COLORS[(t - 1 + TERM_COLORS.length) % TERM_COLORS.length];

const KINDS = {
  heartbeat: { color: "#ff003c", legend: "♥ heartbeat" },
  append: { color: "#00f0ff", legend: "#n append entries" },
  prevote: { color: "#b967ff", legend: "pre-vote?" },
  vote: { color: "#fcee0a", legend: "vote?" },
  snapshot: { color: "#39ff14", legend: "snap install snapshot" },
};

const CATS = {
  election: "ELECTION", leader: "LEADER", commit: "COMMIT", client: "CLIENT", read: "READ",
  network: "NETWORK", chaos: "CHAOS", snapshot: "SNAPSHOT", membership: "MEMBERS",
  lesson: "LESSON", system: "SYSTEM", error: "ERROR",
};

const ROLE = {
  leader: { label: "★ LEADER", color: "#fcee0a" },
  candidate: { label: "⚑ CANDIDATE", color: "#ff2a6d" },
  follower: { label: "● FOLLOWER", color: "#00f0ff" },
  offline: { label: "✖ OFFLINE", color: "#5c5966" },
};

const W = 800, H = 500, CARD_W = 152, CARD_H = 100;

// ── state ─────────────────────────────────────────────────

const S = {
  state: null,
  recvAt: 0,
  selected: null,
  packets: [],
  pos: {},
  feedKeys: new Set(),
  filters: new Set(Object.keys(CATS)),
  sideB: new Set(),
  cliNode: "n1",
  history: [],
  histIdx: 0,
  busy: false,
  dragging: false,
  done: loadDone(),
  mem: {},
};

// ── helpers ───────────────────────────────────────────────

const $ = sel => document.querySelector(sel);
const esc = s => String(s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const svg = (tag, attrs = {}) => {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
};
const pad = (n, w = 2) => String(n).padStart(w, "0");
const clock = t => { const d = new Date(t); return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`; };
const secs = ms => (ms / 1000).toFixed(ms < 1000 ? 2 : 1).replace(/\.?0+$/, "") + "s";

const nodes = () => (S.state ? S.state.nodes : []);
const byId = id => nodes().find(n => n.id === id);
const leader = () => {
  let best = null;
  for (const n of nodes()) if (n.up && n.role === "leader" && (!best || n.term > best.term)) best = n;
  return best;
};
const members = () => nodes().filter(n => n.member);
const majority = () => Math.floor(Math.max(members().length, 1) / 2) + 1;

function loadDone() {
  try { return new Set(JSON.parse(localStorage.getItem("raftlab.done") || "[]")); } catch { return new Set(); }
}
function saveDone() {
  try { localStorage.setItem("raftlab.done", JSON.stringify([...S.done])); } catch { /* storage unavailable */ }
}

function toast(text) {
  const t = $("#toast");
  t.textContent = text;
  t.classList.add("show");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => t.classList.remove("show"), 3200);
}

async function post(url, body) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  return r.json();
}

async function act(type, extra = {}) {
  const res = await post("/api/action", { type, ...extra });
  if (res.error) toast(res.error);
  return res;
}

// ── live connection ───────────────────────────────────────

function connect() {
  const es = new EventSource("/api/events");
  es.addEventListener("state", e => {
    const first = !S.state;
    S.state = JSON.parse(e.data);
    S.recvAt = performance.now();
    if (first) onFirstState();
    render();
  });
  es.addEventListener("msg", e => addPacket(JSON.parse(e.data)));
  es.addEventListener("feed", e => addFeed(JSON.parse(e.data)));
  es.addEventListener("reset", () => {
    for (const p of S.packets) p.g.remove();
    S.packets = [];
    S.sideB.clear();
  });
  es.onerror = () => { $("#hint").innerHTML = '<span class="warn">Lost connection to the lab. Retrying…</span>'; };
}

function onFirstState() {
  const n = nodes()[0];
  if (n && n.redis) {
    const port = n.redis.split(":").pop();
    $("#foot-cli").textContent = `redis-cli -c -p ${port}`;
  }
  setCliNode(S.cliNode);
  // On narrow screens the map scrolls sideways; start in the middle.
  requestAnimationFrame(() => { const w = $(".map-wrap"); w.scrollLeft = (w.scrollWidth - w.clientWidth) / 2; });
}

function render() {
  renderVitals();
  renderHint();
  renderMap();
  renderLogGrid();
  renderInspector();
  renderControls();
}

// ── header ────────────────────────────────────────────────

function renderVitals() {
  const st = S.state, L = leader();
  const up = nodes().filter(n => n.up).length;
  const term = Math.max(0, ...nodes().filter(n => n.up).map(n => n.term));
  const items = [
    ["TERM", pad(term), ""],
    ["LEADER", L ? L.id : "NONE", L ? "" : "bad"],
    ["COMMIT", L ? "#" + L.commit : "—", "cyan"],
    ["NODES UP", `${up}/${nodes().length}`, up < majority() ? "bad" : ""],
    ["MAJORITY", `${majority()} of ${members().length}`, ""],
    ["HEARTBEAT", secs(st.heartbeatMs), "cyan"],
    ["ELECTION T/O", `${secs(st.electionMs)}–${secs(2 * st.electionMs)}`, "cyan"],
  ];
  $("#vitals").innerHTML = items.map(([k, v, c]) => `<div><dt>${k}</dt><dd class="${c}">${esc(v)}</dd></div>`).join("");
}

// reachableFrom counts the running members on n's side of the network.
const reachableFrom = n => members().filter(m => m.up && m.group === n.group).length;

function renderHint() {
  const L = leader();
  const leaders = nodes().filter(n => n.up && n.role === "leader");
  const candidates = nodes().filter(n => n.up && n.role === "candidate");
  const reachable = L ? reachableFrom(L) : 0;
  let h;
  if (leaders.length > 1) {
    const stale = leaders.filter(n => n !== L).map(n => `${n.id} (term ${n.term})`).join(", ");
    h = `<span class="warn">⚠ TWO LEADERS?</span> ${stale} still thinks it leads, but <b>${L.id}</b> leads the newer term <b>${L.term}</b>. Only a side with ${majority()} nodes can commit, so the old leader's writes just wait. Heal the network and watch it step down.`;
  } else if (L && reachable < majority()) {
    h = `<span class="warn">⚠ ${L.id} still thinks it leads term ${L.term}, but only ${reachable} of ${members().length} members can hear it.</span> Nothing it accepts can commit until a majority is back.`;
  } else if (L) {
    h = `<b>${L.id}</b> leads term <b>${L.term}</b>. It sends a heartbeat every ${secs(S.state.heartbeatMs)}; a write commits once <b>${majority()} of ${members().length}</b> nodes store it. Try <code>SET user:1 Neo</code> in the console.`;
  } else if (candidates.length) {
    h = `<b>ELECTION:</b> ${candidates.map(c => c.id).join(", ")} ${candidates.length > 1 ? "are" : "is"} asking for votes in term <b>${Math.max(...candidates.map(c => c.term))}</b>. ${majority()} votes are needed.`;
  } else if (nodes().filter(n => n.up).length < majority()) {
    h = `<span class="warn">⚠ Only ${nodes().filter(n => n.up).length} nodes are up.</span> Without ${majority()} nodes, nobody can be elected and nothing can be written. Restart some nodes.`;
  } else {
    h = `<b>NO LEADER.</b> Every follower is waiting for a heartbeat. Watch the draining bars: when one runs out, that node asks for (pre-)votes.`;
  }
  $("#hint").innerHTML = h;
}

// ── cluster map ───────────────────────────────────────────

function layout() {
  const list = nodes(), pos = {};
  list.forEach((n, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / list.length;
    pos[n.id] = { x: W / 2 + 290 * Math.cos(a), y: H / 2 + 172 * Math.sin(a) };
  });
  S.pos = pos;
}

function renderMap() {
  layout();
  const list = nodes(), L = leader();

  const links = $("#links");
  links.replaceChildren();
  for (let i = 0; i < list.length; i++) {
    for (let j = i + 1; j < list.length; j++) {
      const a = list[i], b = list[j], pa = S.pos[a.id], pb = S.pos[b.id];
      let cls = "link";
      if (!a.up || !b.up) cls += " dead";
      else if (a.group !== b.group) cls += " cut";
      links.append(svg("line", { class: cls, x1: pa.x, y1: pa.y, x2: pb.x, y2: pb.y }));
      if (cls.endsWith("cut")) {
        const t = svg("text", { class: "cut-mark", x: (pa.x + pb.x) / 2, y: (pa.y + pb.y) / 2 + 5, "text-anchor": "middle" });
        t.textContent = "✂";
        links.append(t);
      }
    }
  }

  const core = $("#core");
  core.innerHTML = `
    <circle class="core-ring" cx="${W / 2}" cy="${H / 2}" r="78"/>
    <text class="core-text" x="${W / 2}" y="${H / 2 - 30}">${L ? "LEADER" : "NO LEADER"}</text>
    <text class="core-big ${L ? "" : "bad"}" x="${W / 2}" y="${H / 2 + 2}">${L ? L.id : "· · ·"}</text>
    <text class="core-text" x="${W / 2}" y="${H / 2 + 26}">TERM ${L ? L.term : "—"}</text>
    <text class="core-text" x="${W / 2}" y="${H / 2 + 44}">COMMIT ${L ? "#" + L.commit : "—"}</text>`;

  const g = $("#nodes");
  g.replaceChildren();
  for (const n of list) g.append(nodeCard(n));
}

function nodeCard(n) {
  const p = S.pos[n.id], x = p.x - CARD_W / 2, y = p.y - CARD_H / 2;
  const role = ROLE[n.role] || ROLE.offline;
  const cls = ["node", n.role, n.member ? "" : "nonmember", S.selected === n.id ? "selected" : ""].join(" ");
  const g = svg("g", { class: cls, tabindex: 0, role: "button", "aria-label": `${n.id}, ${n.role}` });
  g.addEventListener("click", () => select(n.id));
  g.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") select(n.id); });

  let flag = "";
  if (!n.member && n.up) flag = `<text class="flag" x="${p.x}" y="${y - 7}" fill="#8a8794">NOT A MEMBER</text>`;
  else if (n.group >= 100) flag = `<text class="flag" x="${p.x}" y="${y - 7}" fill="#ff003c">⚡ ISOLATED</text>`;
  else if (n.group > 0) flag = `<text class="flag" x="${p.x}" y="${y - 7}" fill="#ff003c">✂ SIDE B</text>`;

  let bottom;
  if (!n.up) {
    bottom = `<text class="line" x="${x + 10}" y="${y + 86}">disk intact · restart me</text>`;
  } else if (n.role === "leader") {
    bottom = `<text class="hb" x="${x + 10}" y="${y + 86}"><tspan class="beat">♥</tspan> heartbeat · ${secs(S.state.heartbeatMs)}</text>`;
  } else if (!n.member) {
    bottom = `<text class="line" x="${x + 10}" y="${y + 86}">waits to be added</text>`;
  } else {
    bottom = `<text class="line" x="${x + 10}" y="${y + 74}" font-size="9" letter-spacing=".08em">ELECTION TIMER</text>
      <rect class="bar-bg" x="${x + 10}" y="${y + 80}" width="${CARD_W - 20}" height="8"/>
      <rect class="bar" data-timer="${n.id}" x="${x + 10}" y="${y + 80}" width="0" height="8"/>`;
  }

  const v = s => `<tspan class="v">${esc(s)}</tspan>`;
  g.innerHTML = `
    ${S.selected === n.id ? `<rect class="sel" x="${x - 5}" y="${y - 5}" width="${CARD_W + 10}" height="${CARD_H + 10}"/>` : ""}
    <rect class="card" x="${x}" y="${y}" width="${CARD_W}" height="${CARD_H}"/>
    <text class="id" x="${x + 10}" y="${y + 20}">${n.id}</text>
    <text class="role" x="${x + CARD_W - 9}" y="${y + 19}" fill="${role.color}">${role.label}</text>
    ${n.up ? `<text class="line" x="${x + 10}" y="${y + 39}">term ${v(n.term)}  vote ${v(n.votedFor || "—")}</text>
    <text class="line" x="${x + 10}" y="${y + 55}">last ${v("#" + n.last)}  commit ${v("#" + n.commit)}</text>` :
    `<text class="line" x="${x + 10}" y="${y + 47}">crashed</text>`}
    ${bottom}${flag}`;
  return g;
}

function addPacket(m) {
  if (!S.state || S.packets.length > 180) return;
  const a = S.pos[m.from], b = S.pos[m.to];
  if (!a || !b) return;
  if (m.kind === "heartbeat" && m.reply && S.packets.length > 60) return;

  const color = KINDS[m.kind]?.color || "#fff";
  let label = m.label;
  if (m.reply) label = m.kind === "heartbeat" ? (m.ok ? "✓" : "✗") : (m.ok ? "✓ " : "✗ ") + m.label;
  const w = Math.max(18, label.length * 6.6 + 10);

  const g = svg("g", { class: "pkt" });
  const rect = svg("rect", {
    x: -w / 2, y: -8, width: w, height: 16, rx: 3,
    fill: m.reply ? "#07070b" : color, stroke: color, "fill-opacity": m.reply ? 0.9 : 0.92,
  });
  const text = svg("text", { fill: m.reply ? color : "#07070b" });
  text.textContent = label;
  g.append(rect, text);
  $("#packets").append(g);

  // Requests and replies on the same link travel side by side.
  const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1;
  const ux = dx / len, uy = dy / len, off = m.reply ? 8 : -8;
  const inset = 58;
  S.packets.push({
    g, text, rect, fate: m.fate, start: performance.now(), dur: Math.max(m.ms, 60),
    x0: a.x + ux * inset - uy * off, y0: a.y + uy * inset + ux * off,
    x1: b.x - ux * inset - uy * off, y1: b.y - uy * inset + ux * off,
  });
}

function animate(now) {
  const keep = [];
  for (const p of S.packets) {
    const t = (now - p.start) / p.dur;
    if (p.fate === "ok") {
      if (t >= 1) { p.g.remove(); continue; }
      place(p, t, 1);
    } else {
      // Lost and blocked packets die halfway: the server waits half the
      // travel time before reporting them lost.
      if (t < 0.5) place(p, t, 1);
      else if (t < 1.1) {
        if (!p.dead) {
          p.dead = true;
          p.text.textContent = p.fate === "drop" ? "✗ lost" : "✂ blocked";
          p.rect.setAttribute("width", 64); p.rect.setAttribute("x", -32);
          p.rect.setAttribute("fill", "#2a0008"); p.rect.setAttribute("stroke", "#ff003c");
          p.text.setAttribute("fill", "#ff003c");
        }
        place(p, 0.5, 1 - (t - 0.5) / 0.6);
      } else { p.g.remove(); continue; }
    }
    keep.push(p);
  }
  S.packets = keep;

  // Election timers drain between state updates.
  if (S.state) {
    const elapsed = now - S.recvAt;
    for (const n of nodes()) {
      const bar = document.querySelector(`[data-timer="${n.id}"]`);
      if (!bar || !n.timerTotalMs) continue;
      const f = Math.max(0, Math.min(1, (n.timerLeftMs - elapsed) / n.timerTotalMs));
      bar.setAttribute("width", (CARD_W - 20) * f);
      bar.classList.toggle("low", f < 0.25);
    }
  }
  requestAnimationFrame(animate);
}

function place(p, t, opacity) {
  const e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2; // ease in-out
  const x = p.x0 + (p.x1 - p.x0) * e, y = p.y0 + (p.y1 - p.y0) * e;
  p.g.setAttribute("transform", `translate(${x.toFixed(1)},${y.toFixed(1)})`);
  p.g.setAttribute("opacity", opacity.toFixed(2));
}

function renderLegend() {
  const items = Object.values(KINDS).map(k => `<span><i style="color:${k.color}">■</i> ${k.legend}</span>`);
  items.push('<span><i style="color:#e6e3df">✓ ✗</i> replies</span>', '<span><i style="color:#ff003c">✂</i> partition</span>');
  $("#legend").innerHTML = items.join("");
}

// ── replicated log grid ───────────────────────────────────

function renderLogGrid() {
  const list = nodes(), L = leader();
  const last = Math.max(0, ...list.map(n => n.last));
  const cols = 24, from = Math.max(1, last - cols + 1);
  const commit = L ? L.commit : Math.max(0, ...list.map(n => n.commit));

  let html = "<table><thead><tr><th></th>";
  for (let i = from; i < from + cols; i++) html += `<th class="${i <= commit ? "committed" : ""}">${i}</th>`;
  html += "</tr></thead><tbody>";
  for (const n of list) {
    const cls = n.up ? (n.role === "leader" ? "leader" : "") : "offline";
    html += `<tr><th class="row ${cls}">${n.id}${n.role === "leader" ? " ★" : ""}</th>`;
    const byIndex = new Map((n.log || []).map(e => [e.i, e]));
    for (let i = from; i < from + cols; i++) {
      if (!n.up) { html += '<td class="empty"></td>'; continue; }
      const e = byIndex.get(i);
      if (i <= n.snapshot) {
        html += `<td class="snap" title="#${i}: compacted into ${n.id}'s snapshot">▓</td>`;
      } else if (e) {
        const c = termColor(e.t), done = i <= n.commit;
        const tip = `#${i} · term ${e.t} · ${e.text} · ${done ? "committed" : "NOT committed yet"}`;
        html += `<td class="${done ? "committed" : ""} ${e.type}" style="color:${c};border-color:${c};background:${c}${done ? "26" : "0a"}" title="${esc(tip)}">${e.t}</td>`;
      } else {
        html += '<td class="empty"></td>';
      }
    }
    html += "</tr>";
  }
  html += "</tbody></table>";
  html += `<div class="note">Solid cell = committed (on a majority), dashed = not yet. ▓ = compacted into a snapshot (every ${S.state.snapshotThreshold} entries). Hover a cell to see its command.</div>`;
  $("#loggrid").innerHTML = html;
}

// ── inspector ─────────────────────────────────────────────

function select(id) {
  S.selected = id;
  render();
}

function explainNode(n) {
  const L = leader();
  if (!n.up) return `<b>${n.id} is OFFLINE</b> (crashed). Its memory is gone, but its term, vote and log are safe on disk. Restart it to watch it recover and catch up on what it missed.`;
  if (!n.member) return `<b>${n.id} is not a member</b> of the cluster. It never starts elections and nobody counts it for a majority. A leader can add it with a membership change.`;
  let s;
  if (n.role === "leader") {
    s = `<b>${n.id} is the LEADER</b> of term ${n.term}. Every write goes through it: it appends the write to its log and sends it to the followers. Every ${secs(S.state.heartbeatMs)} it sends a heartbeat (♥) so nobody starts an election. An entry is <em>committed</em> once ${majority()} of ${members().length} nodes store it.`;
    if (reachableFrom(n) < majority()) s += ` <b style="color:var(--red)">But it can only reach ${reachableFrom(n)} of ${members().length} members, so nothing it accepts can commit.</b>`;
  } else if (n.role === "candidate") {
    s = `<b>${n.id} is a CANDIDATE</b> in term ${n.term}: it voted for itself and is asking the others for votes. With ${majority()} votes it becomes leader; if the vote splits, the timers run again.`;
  } else {
    s = `<b>${n.id} is a FOLLOWER</b>. It stores what the leader${L ? " (" + L.id + ")" : ""} sends and answers it. The bar under it is its <em>election timer</em>: every heartbeat refills it. If it ever runs out, ${n.id} assumes the leader is dead and starts an election.`;
  }
  if (n.group >= 100) s += ` <b style="color:var(--red)">It is isolated:</b> no message gets through, so it cannot tell whether it or everyone else is cut off.`;
  else if (n.group > 0) s += ` <b style="color:var(--red)">It is on side B</b> of a network split.`;
  return s;
}

function renderInspector() {
  let n = byId(S.selected);
  if (!n) {
    n = leader() || nodes()[0];
    if (!n) return;
    S.selected = n.id;
  }
  const role = ROLE[n.role] || ROLE.offline;
  $("#insp-sub").textContent = n.id + (n.redis ? " · " + n.redis : "");

  const stat = (k, v) => `<div><dt>${k}</dt><dd title="${esc(v)}">${esc(v)}</dd></div>`;
  const side = n.group >= 100 ? "ISOLATED" : n.group > 0 ? "SIDE B" : "CONNECTED";
  let html = `<span class="role-badge" style="color:${role.color};border-color:${role.color}">${role.label}</span>
    <p class="explain">${explainNode(n)}</p>`;

  if (n.up) {
    html += `<dl class="stats">
      ${stat("TERM", n.term)}${stat("VOTED FOR", n.votedFor || "—")}${stat("LEADER", n.leader || "—")}
      ${stat("LAST INDEX", "#" + n.last)}${stat("COMMIT", "#" + n.commit)}${stat("APPLIED", "#" + n.applied)}
      ${stat("SNAPSHOT", n.snapshot ? "≤ #" + n.snapshot : "none")}${stat("NETWORK", side)}${stat("TIMER", n.role === "leader" ? "leader" : secs(n.timerTotalMs))}
    </dl>`;
  }

  if (n.progress && n.progress.length) {
    html += `<div class="ins-h">REPLICATION PROGRESS <small>the leader's view of each follower</small></div>`;
    for (const p of n.progress) {
      const f = n.last ? Math.min(1, p.match / n.last) : 0;
      html += `<div class="progress-row"><span>${p.id}</span><div class="meter"><span style="width:${(f * 100).toFixed(1)}%"></span></div><span>match #${p.match} · next #${p.next}</span></div>`;
    }
  }

  if (n.up) {
    const log = [...(n.log || [])].reverse().slice(0, 30);
    html += `<div class="ins-h">LOG <small>newest first · ${n.snapshot ? "#1–#" + n.snapshot + " are in the snapshot" : "nothing compacted yet"}</small></div>
      <div class="table-wrap"><table class="tbl"><thead><tr><th>#</th><th>term</th><th>entry</th><th>state</th></tr></thead><tbody>`;
    html += log.length ? log.map(e => {
      const st = e.i <= n.applied ? '<span class="ok">applied</span>' : e.i <= n.commit ? '<span class="ok">committed</span>' : '<span class="wait">waiting</span>';
      return `<tr><td>${e.i}</td><td style="color:${termColor(e.t)}">${e.t}</td><td title="${esc(e.text)}">${esc(e.text)}</td><td>${st}</td></tr>`;
    }).join("") : '<tr><td colspan="4" class="empty">empty</td></tr>';
    html += "</tbody></table></div>";

    const data = n.data || [];
    html += `<div class="ins-h">DATA <small>${n.id}'s own copy · ${data.length} key${data.length === 1 ? "" : "s"}</small></div>
      <div class="table-wrap"><table class="tbl"><thead><tr><th>key</th><th>value</th><th>ttl</th></tr></thead><tbody>`;
    html += data.length ? data.map(d => `<tr><td>${esc(d.k)}</td><td title="${esc(d.v)}">${esc(d.v)}</td><td>${d.ttl < 0 ? "—" : secs(d.ttl)}</td></tr>`).join("") :
      '<tr><td colspan="3" class="empty">no keys yet: try SET in the console</td></tr>';
    html += "</tbody></table></div>";
  }

  html += `<div class="ins-actions">
    ${n.up ? `<button class="btn danger small" data-node-act="crash">✖ CRASH</button>` : `<button class="btn go small" data-node-act="restart">↻ RESTART</button>`}
    <button class="btn danger small" data-node-act="isolate">${n.group ? "RECONNECT" : "⚡ ISOLATE"}</button>
    ${n.up ? `<button class="btn small" data-node-act="snapshot">▓ SNAPSHOT NOW</button>` : ""}
    ${n.member ? `<button class="btn danger small" data-node-act="remove">− REMOVE FROM CLUSTER</button>` : ""}
    <button class="btn small" data-node-act="console">› USE IN CONSOLE</button>
  </div>`;

  const box = $("#inspector");
  const scrolls = [...box.querySelectorAll(".table-wrap")].map(t => t.scrollTop);
  box.innerHTML = html;
  box.querySelectorAll(".table-wrap").forEach((t, i) => { t.scrollTop = scrolls[i] || 0; });
}

$("#inspector").addEventListener("click", e => {
  const b = e.target.closest("[data-node-act]");
  if (!b) return;
  const id = S.selected, a = b.dataset.nodeAct;
  if (a === "console") { setCliNode(id); $("#cli-input").focus(); return; }
  act(a, { node: id });
});

// ── console ───────────────────────────────────────────────

const CHIPS = ["SET user:1 Neo", "GET user:1", "INCR visits", "SET otp 4242 EX 20", "TTL otp", "KEYS *", "DBSIZE", "DEL user:1", "RAFT INFO"];

function setCliNode(id) {
  S.cliNode = id;
  const n = byId(id);
  $("#cli-prompt").textContent = id + ">";
  $("#cli-sub").textContent = n && n.redis ? `${id} · ${n.redis}` : id;
  renderCliSelect();
}

function renderCliSelect() {
  const sel = $("#cli-node");
  const want = nodes().map(n => n.id).join(",");
  if (sel.dataset.ids !== want) {
    sel.dataset.ids = want;
    sel.innerHTML = nodes().map(n => `<option value="${n.id}">${n.id}</option>`).join("");
  }
  sel.value = S.cliNode;
}

function cliPrint(cls, html) {
  const out = $("#cli-out");
  const div = document.createElement("div");
  div.className = cls;
  div.innerHTML = html;
  out.append(div);
  while (out.childElementCount > 400) out.firstElementChild.remove();
  out.scrollTop = out.scrollHeight;
  return div;
}

async function runCommand(line, node = S.cliNode) {
  line = line.trim();
  if (!line) return;
  S.history.push(line);
  S.histIdx = S.history.length;
  cliPrint("in", `<span class="p">${esc(node)}&gt;</span> ${esc(line)}`);
  const wait = cliPrint("sys", "… waiting for the cluster");
  const res = await post("/api/command", { node, line, follow: $("#cli-follow").checked });
  wait.remove();
  let at = node;
  for (const l of res.lines || []) {
    if (l.startsWith("-> Redirected to ")) { at = l.slice(17); cliPrint("redir", esc(l)); }
    else if (l.startsWith("(error)") || l.startsWith("Could not")) cliPrint("err", esc(l));
    else cliPrint("out", esc(l));
  }
  if (at !== node) setCliNode(at);
  return res.lines || [];
}

$("#cli-form").addEventListener("submit", e => {
  e.preventDefault();
  const input = $("#cli-input");
  const line = input.value;
  input.value = "";
  runCommand(line);
});

$("#cli-input").addEventListener("keydown", e => {
  if (e.key === "ArrowUp" && S.histIdx > 0) { S.histIdx--; e.target.value = S.history[S.histIdx]; e.preventDefault(); }
  if (e.key === "ArrowDown") { S.histIdx = Math.min(S.history.length, S.histIdx + 1); e.target.value = S.history[S.histIdx] || ""; e.preventDefault(); }
});

$("#cli-node").addEventListener("change", e => setCliNode(e.target.value));

function renderChips() {
  $("#chips").innerHTML = CHIPS.map(c => `<button type="button">${esc(c)}</button>`).join("");
  $("#chips").addEventListener("click", e => {
    const b = e.target.closest("button");
    if (b) runCommand(b.textContent);
  });
}

// ── network & chaos ───────────────────────────────────────

function renderControls() {
  const st = S.state;
  document.querySelectorAll("[data-speed]").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.speed === st.speed)));
  const pv = $("#prevote");
  pv.setAttribute("aria-pressed", String(st.preVote));
  pv.innerHTML = `PRE-VOTE <b>${st.preVote ? "ON" : "OFF"}</b>`;
  if (!S.dragging) {
    $("#latency").value = st.latencyMs;
    $("#drop").value = st.dropPct;
  }
  $("#latency-v").textContent = $("#latency").value + " ms";
  $("#drop-v").textContent = $("#drop").value + " %";
  $("#sides").innerHTML = nodes().map(n => `<button type="button" data-side="${n.id}" class="${S.sideB.has(n.id) ? "b" : ""}">${n.id}<small>side ${S.sideB.has(n.id) ? "B" : "A"}</small></button>`).join("");
  renderCliSelect();
}

for (const id of ["latency", "drop"]) {
  const input = $("#" + id);
  input.addEventListener("input", () => {
    S.dragging = true;
    $("#latency-v").textContent = $("#latency").value + " ms";
    $("#drop-v").textContent = $("#drop").value + " %";
  });
  input.addEventListener("change", async () => {
    await act("network", { latency: +$("#latency").value, drop: +$("#drop").value });
    S.dragging = false;
  });
}

$("#sides").addEventListener("click", e => {
  const b = e.target.closest("[data-side]");
  if (!b) return;
  const id = b.dataset.side;
  S.sideB.has(id) ? S.sideB.delete(id) : S.sideB.add(id);
  renderControls();
});

$("#apply-split").addEventListener("click", () => act("partition", { groups: Object.fromEntries([...S.sideB].map(id => [id, 1])) }));

const ACTIONS = {
  "crash-leader": () => { const L = leader(); return L ? act("crash", { node: L.id }) : toast("no leader to crash right now"); },
  "isolate-leader": () => { const L = leader(); return L ? act("isolate", { node: L.id }) : toast("no leader right now"); },
  split: () => splitLeaderMinority(),
  heal: () => { S.sideB.clear(); return act("heal"); },
  "restart-all": () => Promise.all(nodes().filter(n => !n.up).map(n => act("restart", { node: n.id }))),
  burst: () => act("burst", { count: 40 }),
  add: () => act("add"),
};

function splitLeaderMinority() {
  const L = leader();
  const ids = nodes().map(n => n.id);
  const b = L ? [L.id, ids.find(id => id !== L.id)] : ids.slice(0, 2);
  S.sideB = new Set(b);
  return act("partition", { groups: Object.fromEntries(b.map(id => [id, 1])) });
}

document.querySelectorAll("[data-act]").forEach(b => b.addEventListener("click", () => ACTIONS[b.dataset.act]()));
document.querySelectorAll("[data-speed]").forEach(b => b.addEventListener("click", () => act("reset", { speed: b.dataset.speed, preVote: S.state.preVote })));
$("#prevote").addEventListener("click", () => act("reset", { speed: S.state.speed, preVote: !S.state.preVote }));
$("#reset").addEventListener("click", () => act("reset", { speed: S.state.speed, preVote: S.state.preVote }));

// ── event feed ────────────────────────────────────────────

function renderFilters() {
  $("#filters").innerHTML = Object.entries(CATS).map(([k, v]) =>
    `<button type="button" class="cat-${k}" data-cat="${k}" aria-pressed="${S.filters.has(k)}">${v}</button>`).join("");
}

$("#filters").addEventListener("click", e => {
  const b = e.target.closest("[data-cat]");
  if (!b) return;
  const c = b.dataset.cat;
  S.filters.has(c) ? S.filters.delete(c) : S.filters.add(c);
  b.setAttribute("aria-pressed", String(S.filters.has(c)));
  document.querySelectorAll(`#feed li[data-cat="${c}"]`).forEach(li => { li.hidden = !S.filters.has(c); });
});

function addFeed(ev) {
  const key = ev.t + "|" + ev.text;
  if (S.feedKeys.has(key)) return;
  S.feedKeys.add(key);
  if (S.feedKeys.size > 1000) S.feedKeys = new Set([...S.feedKeys].slice(-500));

  const li = document.createElement("li");
  li.dataset.cat = ev.cat;
  li.hidden = !S.filters.has(ev.cat);
  li.innerHTML = `<span class="time">${clock(ev.t)}</span><span class="tag cat-${ev.cat}">${CATS[ev.cat] || ev.cat}</span> ${esc(ev.text)}`;
  const list = $("#feed");
  list.prepend(li);
  while (list.childElementCount > 400) list.lastElementChild.remove();
}

const say = text => addFeed({ t: Date.now(), cat: "lesson", text });

// ── lessons ───────────────────────────────────────────────

async function waitFor(pred, ms = 25000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if (S.state && pred()) return true;
    await sleep(150);
  }
  return false;
}

async function needLeader() {
  if (!leader()) say("Waiting for the cluster to elect a leader…");
  if (!(await waitFor(() => leader()))) throw new Error("no leader was elected; restart some nodes or heal the network");
  return leader();
}

const followers = () => nodes().filter(n => n.up && n.member && n.role !== "leader");

async function restartAll() {
  await act("heal");
  S.sideB.clear();
  await Promise.all(nodes().filter(n => !n.up).map(n => act("restart", { node: n.id })));
}

const LESSONS = [
  {
    id: "start", title: "Start here: what am I looking at?",
    body: `<p>This page runs a real <b>5-node database cluster</b> inside one program. The nodes run the same Raft and Redis code as raftkv; only the network between them is simulated, and slowed down so you can see every message.</p>
      <ul><li><b>CLUSTER MAP</b>: the nodes and the messages flying between them.</li>
      <li><b>REPLICATED LOGS</b>: each node's list of commands, lined up by position.</li>
      <li><b>INSPECTOR</b>: click any node to look inside it.</li>
      <li><b>REDIS-CLI</b>: talk to the database, like a real app would.</li>
      <li><b>NETWORK &amp; CHAOS</b>: crash nodes, cut cables, lose packets.</li>
      <li><b>EVENT FEED</b>: everything that happens, explained.</li></ul>
      <p>The lessons go from zero to Raft. Each one has <b>▶ TRY IT</b> buttons that run the experiment for you.</p>`,
    watch: "The first election: every node starts as a follower, and the first one whose timer runs out becomes the leader.",
    tries: [],
  },
  {
    id: "redis", title: "What is Redis?",
    body: `<p><b>Redis</b> is a <em>key-value database</em>: a giant dictionary kept in memory. You store a value under a key and get it back by its name. Apps use it for logins, caches, counters and leaderboards because it is very fast.</p>
      <p><code>SET user:1 Neo</code> stores "Neo" under <code>user:1</code>. <code>GET user:1</code> reads it back. <code>INCR visits</code> adds 1 to a counter. <code>SET otp 4242 EX 20</code> stores a key that deletes itself after 20 seconds.</p>
      <p>raftkv speaks the same protocol as Redis, so any Redis client works with it. The difference is underneath: your data lives on 5 machines at once.</p>`,
    watch: "The command travels to the leader, appears in every node's log, then in every node's DATA table in the inspector.",
    tries: [
      ["SET user:1 Neo", async () => runCommand("SET user:1 Neo", (await needLeader()).id)],
      ["GET user:1", async () => runCommand("GET user:1", (await needLeader()).id)],
      ["INCR visits ×3", async () => { const L = await needLeader(); for (let i = 0; i < 3; i++) await runCommand("INCR visits", L.id); }],
      ["Key that expires", async () => { const L = await needLeader(); await runCommand("SET otp 4242 EX 20", L.id); await runCommand("TTL otp", L.id); }],
    ],
  },
  {
    id: "why", title: "Why a cluster? Enter Raft",
    body: `<p>One server is a single point of failure: if it dies, so does your data. So we keep <b>copies on several servers</b>. The hard part is keeping the copies identical while servers crash and messages get lost. If two servers accept different writes, which one is right?</p>
      <p><b>Raft</b> solves this with three ideas:</p>
      <ol><li>One <b>leader</b> at a time takes all the writes.</li>
      <li>The leader copies each write to the others, in the same order: the <b>log</b>.</li>
      <li>A write counts only once a <b>majority</b> stores it. Any two majorities share at least one node, so a committed write survives any minority failing.</li></ol>
      <p>With 5 nodes the majority is 3, so the cluster survives any 2 failures.</p>`,
    watch: "Writes keep working with 2 nodes down, and stop (without losing anything) with 3 down.",
    tries: [
      ["Crash 2 followers", async () => { await needLeader(); for (const f of followers().slice(0, 2)) await act("crash", { node: f.id }); say("2 of 5 nodes are down. 3 are left: still a majority."); }],
      ["Write anyway", async () => runCommand("SET survived yes", (await needLeader()).id)],
      ["Crash a 3rd node", async () => {
        const f = followers()[0];
        if (f) await act("crash", { node: f.id });
        say("Only 2 nodes are up now: no majority. The next write waits and then gives up, but nothing is lost or corrupted.");
        const L = leader();
        if (L) await runCommand("SET doomed write", L.id);
      }],
      ["Restart everyone", restartAll],
    ],
  },
  {
    id: "election", title: "Terms and leader election",
    body: `<p>Time is split into <b>terms</b> (1, 2, 3…). Each term has at most one leader. The leader sends a <b>heartbeat ♥</b> regularly. Every follower has a random <b>election timer</b> (the draining bar): each heartbeat refills it.</p>
      <p>If a timer runs out, that follower assumes the leader is dead and runs for leader:</p>
      <ol><li><b>Pre-vote</b>: "would you vote for me?" (nothing changes yet).</li>
      <li>If a majority says yes, it becomes a <b>candidate</b>: term + 1, votes for itself, asks for real <b>votes</b>.</li>
      <li>With a majority of votes it becomes the new <b>leader</b>.</li></ol>
      <p>Each node gives one vote per term, so two leaders in the same term are impossible. The timers are random, so usually only one node runs at a time.</p>`,
    watch: "Purple pre-vote? and yellow vote? packets, the term going up by one, and the old leader coming back as a follower.",
    tries: [
      ["Crash the leader", async () => { const L = await needLeader(); S.mem.crashed = L.id; await act("crash", { node: L.id }); say(`${L.id} crashed. Watch the followers' timers run out.`); }],
      ["Restart it", async () => { const id = S.mem.crashed || nodes().find(n => !n.up)?.id; if (id) { await act("restart", { node: id }); select(id); } }],
    ],
  },
  {
    id: "replication", title: "Log replication and commit",
    body: `<p>Every write becomes an <b>entry</b> in the leader's log: its position (<em>index</em>), its <em>term</em> and the command. The leader sends new entries inside <b>AppendEntries</b> messages (cyan <code>#n</code> packets).</p>
      <p>Each follower first checks that its log matches the leader's just before the new entries, so logs can never fork. Then it stores them and says ✓. Once a majority stores an entry, it is <b>committed</b>: the leader applies it, answers the client, and tells the followers so they apply it too.</p>
      <p>Followers don't take writes: they answer <code>MOVED</code> and the client goes to the leader.</p>`,
    watch: "The MOVED redirect in the console, cyan packets, and dashed log cells turning solid once committed.",
    tries: [
      ["Write via a follower", async () => { await needLeader(); const f = followers()[0]; if (f) await runCommand("SET color cyan", f.id); }],
      ["Burst 40 writes", async () => { await needLeader(); await act("burst", { count: 40 }); }],
    ],
  },
  {
    id: "persistence", title: "Persistence: crash and recover",
    body: `<p>A node writes its term, its vote and every log entry to disk (with <code>fsync</code>) <b>before</b> answering anyone. A crash wipes memory, but the disk keeps those promises.</p>
      <p>A restarted node reloads its log from disk, learns from the leader which entries are committed, and re-applies them to rebuild its data. It also catches up on everything it missed.</p>`,
    watch: "The restarted node's inspector: the term and log come back from disk, then the data refills and the missed writes arrive.",
    tries: [
      ["Crash a follower", async () => { await needLeader(); const f = followers()[0]; S.mem.f = f.id; await act("crash", { node: f.id }); select(f.id); }],
      ["Write 3 keys", async () => { const L = await needLeader(); for (const kv of ["a 1", "b 2", "c 3"]) await runCommand("SET " + kv, L.id); }],
      ["Restart it", async () => { const id = S.mem.f || nodes().find(n => !n.up)?.id; if (id) { await act("restart", { node: id }); select(id); } }],
    ],
  },
  {
    id: "partition", title: "Network partitions",
    body: `<p>Networks fail too: the nodes keep running but can't reach each other. Split the cluster 2 | 3 with the leader on the small side:</p>
      <ul><li>The old leader still thinks it leads, but it can't reach a majority, so <b>nothing it accepts can commit</b>.</li>
      <li>The 3 nodes on the other side elect a new leader in a higher term and carry on.</li>
      <li>When the network heals, the old leader sees the higher term and steps down. Its uncommitted entries are replaced. The client never got a success for them, so nothing acknowledged is lost.</li></ul>`,
    watch: "Two leaders in different terms during the split, the minority's dashed cells, and their replacement after HEAL.",
    tries: [
      ["Split: leader + 1 | 3", async () => { const L = await needLeader(); S.mem.old = L.id; await splitLeaderMinority(); }],
      ["Write to the old leader", async () => { const id = S.mem.old || leader()?.id; if (id) { say(`Writing to ${id}. It cannot reach a majority, so this times out after 5 s.`); await runCommand("SET split old-side", id); } }],
      ["Write to the new leader", async () => {
        const ok = await waitFor(() => nodes().some(n => n.up && n.role === "leader" && n.group === 0));
        const L = nodes().find(n => n.up && n.role === "leader" && n.group === 0);
        if (ok && L) await runCommand("SET split majority-side", L.id);
      }],
      ["Heal", async () => { S.sideB.clear(); await act("heal"); }],
    ],
  },
  {
    id: "prevote", title: "Pre-Vote: no pointless elections",
    body: `<p>Without Pre-Vote, an isolated node times out again and again, raising its term each time. When it reconnects, its much higher term forces the healthy leader to step down, and the cluster holds an election for nothing.</p>
      <p>With <b>Pre-Vote</b>, the isolated node first asks "would you elect me?". Nobody can answer, so its term never moves and rejoining is silent. Compare both: the toggle in the header resets the cluster with Pre-Vote on or off.</p>`,
    watch: "The isolated node's term. With Pre-Vote OFF it climbs, and when it reconnects the leader changes.",
    tries: [
      ["Isolate a follower for 12 s", async () => {
        await needLeader();
        const f = followers()[0];
        await act("isolate", { node: f.id });
        select(f.id);
        say(`${f.id} is isolated for 12 s. Watch its term.`);
        await sleep(12000);
        await act("isolate", { node: f.id });
        say(`${f.id} is back. Did the leader change?`);
      }],
      ["Reset with Pre-Vote OFF", () => act("reset", { speed: S.state.speed, preVote: false })],
      ["Reset with Pre-Vote ON", () => act("reset", { speed: S.state.speed, preVote: true })],
    ],
  },
  {
    id: "snapshots", title: "Snapshots and log compaction",
    body: `<p>The log would grow forever. So after every ${30} applied entries, a node saves a <b>snapshot</b> of its data and deletes the entries it covers (the ▓ cells in the log grid).</p>
      <p>If a follower has been away so long that the leader already deleted the entries it needs, the leader sends its whole snapshot instead: the green <code>snap</code> packet.</p>`,
    watch: "▓ cells appearing, then a green snap packet to the reconnected follower, and its data filling in at once.",
    tries: [
      ["Isolate a follower", async () => { await needLeader(); const f = followers()[0]; S.mem.lag = f.id; await act("isolate", { node: f.id }); select(f.id); }],
      ["Burst 40 writes", async () => { await needLeader(); await act("burst", { count: 40 }); }],
      ["Reconnect it", async () => { const id = S.mem.lag || nodes().find(n => n.group)?.id; if (id) await act("isolate", { node: id }); }],
      ["Snapshot the leader now", async () => act("snapshot", { node: (await needLeader()).id })],
    ],
  },
  {
    id: "membership", title: "Adding and removing nodes",
    body: `<p>Clusters grow and shrink while running. The list of members is itself a <b>log entry</b> (<code>config [...]</code>), replicated like any write.</p>
      <p>Raft changes membership <b>one server at a time</b>: then any majority of the old list overlaps any majority of the new one, so two leaders can never exist during the change. A leader that removes itself keeps leading until the change commits, then steps down.</p>`,
    watch: "The config entry in the logs, the new node catching up from nothing, and the removed leader stepping down.",
    tries: [
      ["Add a node", async () => { await needLeader(); await act("add"); }],
      ["Remove the leader", async () => { const L = await needLeader(); await act("remove", { node: L.id }); }],
    ],
  },
  {
    id: "readindex", title: "Fast reads with ReadIndex",
    body: `<p>Reads don't change data, so writing them into the log would waste a disk write and a round of replication. But a leader can't just answer from memory: a newer leader might have replaced it without it knowing, and it would return stale data.</p>
      <p><b>ReadIndex</b>: the leader notes its commit index, sends one heartbeat round, and if a majority answers it is still the leader, it reads from memory. Nothing is written to the log.</p>`,
    watch: "After a GET, the log index does not move. An isolated leader cannot confirm, so its read times out instead of returning stale data.",
    tries: [
      ["GET user:1 (no log entry)", async () => { const L = await needLeader(); say(`${L.id}'s last index before the read: #${L.last}.`); await runCommand("GET user:1", L.id); say(`…and after: #${byId(L.id).last}.`); }],
      ["Isolate the leader, read from it", async () => {
        const L = await needLeader();
        await act("isolate", { node: L.id });
        say(`${L.id} is isolated but still thinks it leads. A GET there must not answer with possibly stale data.`);
        await runCommand("GET user:1", L.id);
      }],
      ["Heal", async () => { S.sideB.clear(); await act("heal"); }],
    ],
  },
  {
    id: "linearizable", title: "Linearizability: the promise",
    body: `<p>All of this gives raftkv the strongest consistency guarantee there is: <b>linearizability</b>. The cluster behaves as if there were a single copy of the data, and every operation happened at one instant between its request and its reply.</p>
      <p>In practice: once a write is acknowledged, every later read sees it, whichever nodes fail. raftkv's test suite proves it: concurrent clients hammer a 5-node cluster while nodes crash and the network splits, and the <b>Porcupine</b> checker verifies every single result.</p>
      <p>Try to break it yourself: crank packet loss, split the network, crash leaders, and keep using the console. You should never read old data after a successful write.</p>`,
    watch: "No matter what you break, an acknowledged write is never lost and never read stale.",
    tries: [
      ["Chaos: 20% loss + split", async () => { await act("network", { latency: S.state.latencyMs, drop: 20 }); await splitLeaderMinority(); }],
      ["Calm down", async () => { await act("network", { latency: S.state.latencyMs, drop: 0 }); await restartAll(); }],
    ],
  },
  {
    id: "glossary", title: "Glossary",
    body: `<table class="gloss">
      <tr><td>node</td><td>one server running raftkv</td></tr>
      <tr><td>leader</td><td>the one node that accepts writes and replicates them</td></tr>
      <tr><td>follower</td><td>stores what the leader sends; redirects clients</td></tr>
      <tr><td>candidate</td><td>a follower asking for votes to become leader</td></tr>
      <tr><td>term</td><td>an election period; at most one leader each</td></tr>
      <tr><td>heartbeat</td><td>empty message proving the leader is alive</td></tr>
      <tr><td>election timer</td><td>random countdown; when it ends, start an election</td></tr>
      <tr><td>log / entry</td><td>ordered list of commands / one command in it</td></tr>
      <tr><td>index</td><td>an entry's position in the log</td></tr>
      <tr><td>majority</td><td>more than half the members (3 of 5)</td></tr>
      <tr><td>commit</td><td>an entry stored on a majority: permanent</td></tr>
      <tr><td>apply</td><td>run a committed command on the data</td></tr>
      <tr><td>snapshot</td><td>saved copy of the data replacing old log entries</td></tr>
      <tr><td>partition</td><td>network split: nodes run but can't talk</td></tr>
      <tr><td>pre-vote</td><td>"would you elect me?" before a real election</td></tr>
      <tr><td>ReadIndex</td><td>reading safely without writing to the log</td></tr>
      <tr><td>MOVED</td><td>"I'm not the leader, ask that node instead"</td></tr>
      <tr><td>linearizable</td><td>behaves like one copy, one operation at a time</td></tr></table>`,
    watch: "",
    tries: [],
  },
];

function renderLessons() {
  $("#lessons").innerHTML = LESSONS.map((l, i) => `
    <li class="lesson" data-lesson="${i}" ${i === 0 ? "open" : ""}>
      <button type="button" aria-expanded="${i === 0}">${esc(l.title)}<span class="done">${S.done.has(l.id) ? "✓ tried" : ""}</span></button>
      <div class="body">${l.body}
        ${l.tries.length ? `<div class="try">${l.tries.map((t, j) => `<button type="button" class="btn go small" data-try="${i}:${j}">▶ ${esc(t[0])}</button>`).join("")}</div>` : ""}
        ${l.watch ? `<div class="watch">${esc(l.watch)}</div>` : ""}
      </div>
    </li>`).join("");
}

$("#lessons").addEventListener("click", async e => {
  const head = e.target.closest(".lesson > button");
  if (head) {
    const li = head.parentElement, open = !li.hasAttribute("open");
    li.toggleAttribute("open", open);
    head.setAttribute("aria-expanded", String(open));
    return;
  }
  const b = e.target.closest("[data-try]");
  if (!b || S.busy) return;
  const [i, j] = b.dataset.try.split(":").map(Number);
  const lesson = LESSONS[i], [label, run] = lesson.tries[j];
  S.busy = true;
  document.querySelectorAll("[data-try]").forEach(x => { x.disabled = true; });
  say(`▶ ${lesson.title}: ${label}`);
  try {
    await run();
    S.done.add(lesson.id);
    saveDone();
    b.closest(".lesson").querySelector(".done").textContent = "✓ tried";
  } catch (err) {
    toast(err.message);
  } finally {
    S.busy = false;
    document.querySelectorAll("[data-try]").forEach(x => { x.disabled = false; });
  }
});

// ── start ─────────────────────────────────────────────────

renderLegend();
renderChips();
renderFilters();
renderLessons();
connect();
requestAnimationFrame(animate);
