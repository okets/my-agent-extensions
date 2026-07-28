#!/usr/bin/env node

// src/cli.ts
import { mkdtempSync as mkdtempSync2, rmSync, existsSync as existsSync3, writeFileSync as writeFileSync3 } from "node:fs";
import { tmpdir as tmpdir2 } from "node:os";
import { join as join6, resolve, basename } from "node:path";

// ../core/src/transports/connector-setup.ts
import { spawn } from "node:child_process";
import { existsSync, readFileSync as readFileSync2 } from "node:fs";
import { join } from "node:path";

// ../core/src/metadata/frontmatter.ts
import { readFileSync, writeFileSync } from "node:fs";
import { parse, stringify } from "yaml";
function parseFrontmatterContent(content) {
  if (!content.startsWith("---\n") && !content.startsWith("---\r\n")) {
    return { data: {}, body: content };
  }
  const closingIndex = content.indexOf("\n---", 4);
  if (closingIndex === -1) {
    return { data: {}, body: content };
  }
  const yamlStr = content.slice(4, closingIndex);
  let body = content.slice(closingIndex + 4);
  if (body.startsWith("\n")) body = body.slice(1);
  const data = parse(yamlStr) ?? {};
  return { data, body };
}
function readFrontmatter(filePath) {
  const content = readFileSync(filePath, "utf-8");
  return parseFrontmatterContent(content);
}

// ../core/src/transports/connector-setup.ts
function deriveConnectorLaunch(folderAbs) {
  const capPath = join(folderAbs, "CAPABILITY.md");
  const mcpPath = join(folderAbs, ".mcp.json");
  if (!existsSync(capPath) || !existsSync(mcpPath)) {
    throw new Error(`connector folder ${folderAbs} missing CAPABILITY.md or .mcp.json`);
  }
  const fm = readFrontmatter(capPath).data;
  const requiredEnv = fm.requires?.env ?? [];
  const mcp = JSON.parse(readFileSync2(mcpPath, "utf8"));
  return { mcp, requiredEnv };
}
function connectorDepsResolve(folderAbs) {
  return existsSync(join(folderAbs, "node_modules", "@modelcontextprotocol", "sdk"));
}
async function runConnectorSetup(folderAbs) {
  if (connectorDepsResolve(folderAbs)) return { ready: true, ranInstall: false };
  const setup = join(folderAbs, "scripts", "setup.sh");
  const cmd = existsSync(setup) ? "bash" : "npm";
  const args = existsSync(setup) ? [setup] : ["install"];
  const code = await new Promise((resolve2) => {
    const p = spawn(cmd, args, { cwd: folderAbs, stdio: "ignore" });
    p.on("exit", (c) => resolve2(c ?? 1));
    p.on("error", () => resolve2(1));
  });
  if (code !== 0) return { ready: false, ranInstall: true, error: `setup exited ${code}` };
  return { ready: connectorDepsResolve(folderAbs), ranInstall: true };
}

// ../core/src/transports/conformance/harness.ts
import { join as join5 } from "node:path";

// ../core/src/capabilities/transport-manifest.ts
var ROLES = /* @__PURE__ */ new Set(["dedicated", "personal"]);
var UPSTREAM_KINDS = /* @__PURE__ */ new Set(["socket", "poll", "push", "webhook", "none"]);
function validateTransportManifest(data) {
  const errors = [];
  if (typeof data.name !== "string" || !data.name) errors.push("Missing name");
  if (typeof data.backend !== "string" || !data.backend) errors.push("Missing/empty backend");
  const roles = data.roles;
  if (!Array.isArray(roles) || roles.length === 0) {
    errors.push("roles must be a non-empty list");
  } else {
    for (const r of roles) {
      if (!ROLES.has(r)) errors.push(`Unknown role "${String(r)}" (allowed: dedicated, personal)`);
    }
  }
  if ("role" in data) {
    errors.push("`role` must NOT be set in frontmatter \u2014 the effective role is binding-derived (\xA73.2). Use `roles` for the capability ceiling.");
  }
  if ("upstream" in data && !UPSTREAM_KINDS.has(data.upstream)) {
    errors.push(`Unknown upstream "${String(data.upstream)}" (allowed: socket, poll, push, webhook, none)`);
  }
  if ("ingress" in data && data.ingress !== "webhook") {
    errors.push(`Unknown ingress "${String(data.ingress)}" (allowed: webhook)`);
  }
  if ("upstream_self_window_ms" in data) {
    const w = data.upstream_self_window_ms;
    if (typeof w !== "number" || !Number.isFinite(w) || w <= 0) {
      errors.push(`upstream_self_window_ms must be a positive number (got ${String(w)})`);
    }
  }
  if (errors.length > 0) return { valid: false, errors };
  return {
    valid: true,
    manifest: {
      name: data.name,
      backend: data.backend,
      roles,
      session_held: typeof data.session_held === "boolean" ? data.session_held : true,
      // fail-safe
      upstream: typeof data.upstream === "string" && UPSTREAM_KINDS.has(data.upstream) ? data.upstream : "none",
      upstream_self_window_ms: typeof data.upstream_self_window_ms === "number" ? data.upstream_self_window_ms : void 0,
      style: data.style ?? void 0,
      lifecycle: Array.isArray(data.lifecycle) ? data.lifecycle : void 0,
      onboarding: data.onboarding ?? void 0,
      streams: typeof data.streams === "string" ? data.streams : void 0,
      template_version: typeof data.template_version === "number" ? data.template_version : void 0,
      icon: typeof data.icon === "string" ? data.icon : void 0,
      ingress: data.ingress === "webhook" ? "webhook" : void 0
    }
  };
}

// ../core/src/transports/mcp-wire-adapter.ts
import { EventEmitter as EventEmitter2 } from "node:events";
import { join as join2 } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ReadBuffer, serializeMessage } from "@modelcontextprotocol/sdk/shared/stdio.js";

// ../core/src/transports/transport-supervisor.ts
import { spawn as spawn2 } from "node:child_process";
import { EventEmitter } from "node:events";
var SIGKILL_GRACE_MS = 5e3;
var RECYCLE_RATE_WINDOW_MS = 10 * 6e4;
var RECYCLE_RATE_ALERT_THRESHOLD = 3;
var TransportSupervisor = class _TransportSupervisor extends EventEmitter {
  constructor(o) {
    super();
    this.o = o;
  }
  child = null;
  starting = false;
  // single-flight lock
  stopped = false;
  restarts = 0;
  spawnCount = 0;
  crashTimes = [];
  /** A6: monotonic recycle count + sliding-window timestamps for the recycle-rate alert. */
  recycleCount = 0;
  recycleTimes = [];
  needsAttention = false;
  /** Ring buffer for spawn/exit evidence (cap 20). Assembled into supervisorLog for the
   *  escalate event so Pass B's CFR can feed it to MODE:FIX without child-stderr capture. */
  logRing = [];
  static LOG_RING_CAP = 20;
  /**
   * D-A8: when set, recycle() no-ops (manual investigation in progress).
   * Task 4b placeholder — the setter `holdRestart()` lands in Task 4b; until then this stays
   * false, so recycle()'s guard is intentionally always-false here, NOT dead code.
   */
  heldForAttention = false;
  /** Tier-0 S4b (2026-07-02): whether the CURRENT child completed an MCP attach. The MCP stdio
   *  handshake needs no network, so an exit BEFORE attach is broken connector code, never an
   *  environmental blip — such exits must stay breaker-eligible even when a sticky pre-recycle
   *  `network_down` classification would otherwise skip the breaker (silent infinite respawn). */
  attachedThisSpawn = false;
  /** Called by the adapter when the MCP attach to the current child succeeds (Tier-0 S4b). */
  noteAttached() {
    this.attachedThisSpawn = true;
  }
  async start() {
    if (this.starting || this.child) return;
    this.starting = true;
    this.stopped = false;
    this.heldForAttention = false;
    this.needsAttention = false;
    this.crashTimes = [];
    try {
      if (this.o.preSpawn) {
        const verdict = await this.o.preSpawn();
        if (verdict === "needs-setup") {
          this.emit("needs-setup");
          return;
        }
      }
      this.spawnOnce();
    } finally {
      this.starting = false;
    }
  }
  /** Push an entry into the capped ring buffer (oldest entries are evicted at the cap). */
  pushLog(entry) {
    const ts = (this.o.now ?? Date.now)();
    this.logRing.push(`[${ts}] ${entry}`);
    if (this.logRing.length > _TransportSupervisor.LOG_RING_CAP) this.logRing.shift();
  }
  /** Returns a snapshot copy of the ring buffer (Pass B feeds this into MODE:FIX). */
  recentLog() {
    return [...this.logRing];
  }
  /** The class the breaker would assign to an exit right now, via the injected `classifyExit`
   *  cause-source (S5 Pass B DEV-2). 'transient' when none is wired. Exposed so the adapter's
   *  live cause-source wiring is observable without forcing a real process exit. */
  classifyExitNow() {
    return this.o.classifyExit?.() ?? "transient";
  }
  /** D-A8: stop keep-alive restarts until a human re-pairs (cleared by a fresh start()). */
  holdRestart() {
    this.heldForAttention = true;
  }
  spawnOnce() {
    const scopedEnv = typeof this.o.scopedEnv === "function" ? this.o.scopedEnv() : this.o.scopedEnv;
    const child = spawn2(this.o.command, this.o.args, {
      cwd: this.o.cwd,
      env: scopedEnv,
      // SCOPED — no inherited process.env
      stdio: ["pipe", "pipe", "pipe"],
      // M10-S8 GAP-2: own process group (pgid = child.pid), so teardown can signal the
      // WHOLE tree. Connectors launched via a wrapper (node_modules/.bin/tsx → real node)
      // otherwise leak the grandchild on kill — SIGKILL can't be forwarded by the wrapper.
      detached: true
    });
    this.child = child;
    this.attachedThisSpawn = false;
    this.spawnCount++;
    this.pushLog(`spawn #${this.spawnCount}`);
    console.warn(`[supervisor:${this.o.id}] ${this.spawnCount === 1 ? "spawn" : "respawn"} #${this.spawnCount} (pid=${child.pid ?? "?"})`);
    child.stderr?.on("data", (chunk) => {
      for (const line of chunk.toString().split("\n")) {
        if (line.length > 0) process.stderr.write(`[connector:${this.o.id}] ${line}
`);
      }
    });
    child.on("exit", () => {
      this.child = null;
      if (this.stopped) return;
      if (this.heldForAttention) return;
      const rawCls = this.o.classifyExit?.() ?? "transient";
      const cls = this.attachedThisSpawn ? rawCls : "transient";
      if (cls !== rawCls) this.pushLog(`exit pre-attach \u2014 class '${rawCls}' overridden to 'transient' (breaker-eligible)`);
      this.pushLog(`exit (${cls})`);
      if (cls !== "network_down") {
        const now = (this.o.now ?? Date.now)();
        this.crashTimes = this.crashTimes.filter((t) => now - t < (this.o.windowMs ?? 6e4));
        this.crashTimes.push(now);
        if (this.crashTimes.length >= (this.o.maxRestarts ?? 5)) {
          this.needsAttention = true;
          this.emit("broken");
          this.emit("escalate", { reason: "reconnect-exhausted", supervisorLog: this.recentLog() });
          return;
        }
      }
      this.restarts++;
      setTimeout(() => {
        if (!this.stopped && !this.child) this.spawnOnce();
      }, this.o.backoffMs ?? 1e3);
    });
    this.emit("spawn", child);
  }
  /**
   * D-A9: replace a wedged/deaf child without stopping the supervisor.
   * Kill it WITHOUT setting `stopped`, so the exit handler's keep-alive restart respawns
   * a fresh connector. This is the probe's half-open heal path — a deaf process can't be
   * re-attached (the handshake would hang); only a fresh spawn recovers it.
   * No-op when held for attention (D-A8) or intentionally stopped.
   */
  /** Signal the child's whole process GROUP (M10-S8 GAP-2); falls back to the direct
   *  child if the group is already gone (ESRCH) or unavailable. */
  killTree(child, signal) {
    try {
      if (child.pid) process.kill(-child.pid, signal);
      else child.kill(signal);
    } catch {
      try {
        child.kill(signal);
      } catch {
      }
    }
  }
  recycle() {
    if (this.heldForAttention || this.stopped) return;
    const child = this.child;
    if (!child) return;
    this.recycleCount++;
    const now = (this.o.now ?? Date.now)();
    this.recycleTimes = this.recycleTimes.filter((t) => now - t < RECYCLE_RATE_WINDOW_MS);
    this.recycleTimes.push(now);
    this.pushLog(`recycle #${this.recycleCount}`);
    console.warn(
      `[supervisor:${this.o.id}] recycle #${this.recycleCount} \u2014 replacing connector child (pid=${child.pid ?? "?"}); ${this.recycleTimes.length} recycle(s) within ${RECYCLE_RATE_WINDOW_MS / 6e4}min`
    );
    let exited = false;
    child.once("exit", () => {
      exited = true;
    });
    this.killTree(child, "SIGTERM");
    setTimeout(() => {
      if (!exited) this.killTree(child, "SIGKILL");
    }, SIGKILL_GRACE_MS);
  }
  childProcess() {
    return this.child;
  }
  // Task 4 attaches the SDK client to this
  async stop() {
    this.stopped = true;
    const child = this.child;
    if (!child) return;
    this.killTree(child, "SIGTERM");
    await new Promise((resolve2) => {
      const t = setTimeout(() => {
        this.killTree(child, "SIGKILL");
        resolve2();
      }, SIGKILL_GRACE_MS);
      child.on("exit", () => {
        clearTimeout(t);
        resolve2();
      });
    });
    this.killTree(child, "SIGKILL");
    this.child = null;
  }
  status() {
    const now = (this.o.now ?? Date.now)();
    const recentRecycles = this.recycleTimes.filter((t) => now - t < RECYCLE_RATE_WINDOW_MS).length;
    return {
      running: !!this.child,
      pid: this.child?.pid ?? null,
      restarts: this.restarts,
      spawnCount: this.spawnCount,
      needsAttention: this.needsAttention,
      recycleCount: this.recycleCount,
      recycleRateAlert: recentRecycles >= RECYCLE_RATE_ALERT_THRESHOLD
    };
  }
  /** Test helper: start, read the first stdout chunk, stop. Proves the env-scoping
   *  contract (the child sees ONLY scopedEnv, never the host process.env). */
  async captureStdoutOnce() {
    await this.start();
    const child = this.child;
    if (!child || !child.stdout) throw new Error("no child stdout to capture");
    const chunk = await new Promise((resolve2, reject) => {
      child.stdout.once("data", (d) => resolve2(d.toString()));
      child.once("error", reject);
      child.once("exit", () => resolve2(""));
    });
    await this.stop();
    return chunk;
  }
};

// ../core/src/capabilities/failure-symptoms.ts
import os from "node:os";
import fs from "node:fs";
function isHostResourceExhausted(stats) {
  const cpus = stats.cpuCount > 0 ? stats.cpuCount : 1;
  const loadPerCpu = stats.loadAvg1 / cpus;
  return loadPerCpu > 4 || stats.swapUsedRatio >= 0.95;
}
function readHostResourceStats() {
  const loadAvg1 = os.loadavg()[0] ?? 0;
  const cpuCount = os.cpus()?.length ?? 1;
  let swapUsedRatio = 0;
  try {
    const meminfo = fs.readFileSync("/proc/meminfo", "utf-8");
    const total = Number(/SwapTotal:\s+(\d+)/.exec(meminfo)?.[1] ?? 0);
    const free = Number(/SwapFree:\s+(\d+)/.exec(meminfo)?.[1] ?? 0);
    if (total > 0) swapUsedRatio = (total - free) / total;
  } catch {
  }
  return { loadAvg1, cpuCount, swapUsedRatio };
}
function isEnvironmentalTimeout(error, stats) {
  if (!/timeout|timed out|etimedout/i.test(error)) return false;
  return isHostResourceExhausted(stats ?? readHostResourceStats());
}

// ../core/src/transports/disconnect-classify.ts
function classifyDisconnect(cause, hostStats) {
  if (cause.loggedOut) return "logged_out";
  if (cause.error && isEnvironmentalTimeout(cause.error, hostStats)) return "network_down";
  return "transient";
}
function initialRecoveryState() {
  return "healthy";
}

// ../core/src/transports/cursor-store.ts
import { readFileSync as readFileSync3, writeFileSync as writeFileSync2, renameSync, mkdirSync } from "fs";
import { dirname } from "path";
var CursorStore = class {
  constructor(filePath) {
    this.filePath = filePath;
  }
  load() {
    try {
      return readFileSync3(this.filePath, "utf8").trim() || null;
    } catch {
      return null;
    }
  }
  save(cursor) {
    const cur = this.load();
    if (cur !== null && Number(cursor) <= Number(cur)) return;
    mkdirSync(dirname(this.filePath), { recursive: true });
    const tmp = `${this.filePath}.tmp`;
    writeFileSync2(tmp, cursor);
    renameSync(tmp, this.filePath);
  }
};

// ../core/src/transports/wire-protocol.ts
import { z } from "zod";
var SEND_TOOL = "transport/send";
var DRAIN_TOOL = "transport/drain";
var TYPING_TOOL = "transport/typing";
var HEALTH_TOOL = "transport/health";
var HealthToolOutputSchema = z.object({
  upstreamConnected: z.boolean(),
  lastUpstreamActivityMs: z.number().optional(),
  reason: z.enum(["transient", "logged_out", "credential", "network"]).optional(),
  inboundReceived: z.number().optional(),
  inboundEnqueued: z.number().optional()
});
var MESSAGE_AVAILABLE = "notifications/transport/message_available";
var NEEDS_ATTENTION = "notifications/transport/needs_attention";
var REACTION_TOOL = "transport/reaction";
var EDIT_TOOL = "transport/edit";
var MARK_READ_TOOL = "transport/mark_read";
var ONBOARDING_POLL_TOOL = "transport/onboarding_poll";
var ONBOARDING_ARTIFACT_NOTIF = "notifications/transport/onboarding_artifact";

// ../core/src/transports/media-handle.ts
var VALID_KINDS = /* @__PURE__ */ new Set(["image", "audio", "file"]);
function isMediaHandle(x) {
  if (x === null || typeof x !== "object") return false;
  if (Buffer.isBuffer(x)) return false;
  if (x instanceof Uint8Array) return false;
  const h = x;
  if (typeof h["id"] !== "string") return false;
  if (typeof h["mimeType"] !== "string") return false;
  if (typeof h["size"] !== "number") return false;
  if (typeof h["kind"] !== "string" || !VALID_KINDS.has(h["kind"])) return false;
  if ("caption" in h && typeof h["caption"] !== "string") return false;
  if ("duration" in h && typeof h["duration"] !== "number") return false;
  if ("ptt" in h && typeof h["ptt"] !== "boolean") return false;
  return true;
}

// ../core/src/transports/onboarding-artifact.ts
var STATUS_STATES = /* @__PURE__ */ new Set(["waiting", "linked", "expired", "error", "preflight_ok", "preflight_rejected"]);
function parseOnboardingArtifacts(res) {
  const arr = res?.artifacts;
  if (!Array.isArray(arr)) return [];
  const out = [];
  for (const a of arr) {
    if (!a || typeof a !== "object") continue;
    const o = a;
    if (o.kind === "code" && typeof o.code === "string") {
      out.push({ kind: "code", code: o.code, ...typeof o.label === "string" ? { label: o.label } : {}, ...typeof o.instructions === "string" ? { instructions: o.instructions } : {} });
    } else if (o.kind === "status" && typeof o.state === "string" && STATUS_STATES.has(o.state)) {
      out.push({ kind: "status", state: o.state, ...typeof o.detail === "string" ? { detail: o.detail } : {}, ...typeof o.registered === "boolean" ? { registered: o.registered } : {} });
    } else if (o.kind === "image" && isMediaHandle(o.handle)) {
      out.push({ kind: "image", handle: o.handle, ...typeof o.label === "string" ? { label: o.label } : {} });
    } else if (o.kind === "prompt" && typeof o.text === "string") {
      out.push({
        kind: "prompt",
        text: o.text,
        ...typeof o.secretKey === "string" ? { secretKey: o.secretKey } : {},
        ...typeof o.label === "string" ? { label: o.label } : {},
        ...o.inputType === "text" || o.inputType === "action" ? { inputType: o.inputType } : {}
      });
    }
  }
  return out;
}

// ../core/src/transports/inbound-divergence.ts
function computeInboundDivergence(input) {
  const { inboundEnqueued, baselineInboundEnqueued, inboundDeliveredNow, baselineInboundDelivered, priorStreak, sustainedThreshold } = input;
  if (inboundEnqueued === void 0 || baselineInboundEnqueued === void 0) {
    return { streak: 0, sustained: false };
  }
  const enqueuedGrowth = inboundEnqueued - baselineInboundEnqueued;
  const deliveredGrowth = inboundDeliveredNow - baselineInboundDelivered;
  const divergentThisCheck = enqueuedGrowth > deliveredGrowth;
  const streak = divergentThisCheck ? priorStreak + 1 : 0;
  return { streak, sustained: streak >= sustainedThreshold };
}

// ../core/src/transports/types.ts
function initialStatus() {
  return {
    running: false,
    connected: false,
    reconnectAttempts: 0,
    lastConnectedAt: null,
    lastDisconnect: null,
    lastMessageAt: null,
    lastEventAt: null,
    lastError: null,
    recoveryState: initialRecoveryState(),
    recycleCount: 0,
    recycleRateAlert: false
  };
}

// ../core/src/transports/outbound-pacer.ts
var DEFAULT_RATE_PER_MIN = 20;
var OutboundPacer = class {
  ratePerMin;
  /** Minimum spacing between grants, in ms. */
  intervalMs;
  /**
   * The timestamp (ms) at/after which the NEXT grant may fire. Advances by `intervalMs`
   * on every grant so a queue of concurrent acquires spaces out deterministically without
   * each one having to observe the previous one's completion.
   */
  nextAt = 0;
  constructor(opts) {
    const rate = opts.ratePerMin != null && opts.ratePerMin > 0 ? opts.ratePerMin : DEFAULT_RATE_PER_MIN;
    this.ratePerMin = rate;
    this.intervalMs = 6e4 / rate;
  }
  /**
   * Resolve when this sender may transmit. FIFO across concurrent callers: each call reserves
   * the next slot synchronously (by advancing `nextAt` before awaiting), so ordering is fixed
   * at call time and a later caller can never overtake an earlier one.
   */
  acquire() {
    const now = Date.now();
    const grantAt = Math.max(now, this.nextAt);
    this.nextAt = grantAt + this.intervalMs;
    const wait = grantAt - now;
    if (wait <= 0) return Promise.resolve();
    return new Promise((resolve2) => setTimeout(resolve2, wait));
  }
};

// ../core/src/transports/render/render.ts
import { lexer } from "marked";

// ../core/src/transports/render/dialects.ts
var DIALECTS = {
  // Nothing renders — pure text. Everything degrades.
  plaintext: {
    tables: false,
    bold: false,
    italic: false,
    strikethrough: false,
    inlineCode: false,
    codeBlocks: false,
    blockquote: false,
    anchorLinks: false,
    lists: true,
    headings: false,
    defaultMaxLength: 4096
  },
  // WhatsApp: *bold* _italic_ ~strike~ `code`, raw URLs (no anchor text), no tables/headings.
  whatsapp: {
    tables: false,
    bold: true,
    italic: true,
    strikethrough: true,
    inlineCode: true,
    codeBlocks: true,
    blockquote: false,
    anchorLinks: false,
    lists: true,
    headings: false,
    defaultMaxLength: 4096
  },
  // Telegram MarkdownV2: bold/italic/code + anchor links, no tables.
  "telegram-markdownv2": {
    tables: false,
    bold: true,
    italic: true,
    strikethrough: true,
    inlineCode: true,
    codeBlocks: true,
    blockquote: true,
    anchorLinks: true,
    lists: true,
    headings: false,
    defaultMaxLength: 4096
  },
  // CommonMark: everything, including tables (GFM) and anchor links.
  commonmark: {
    tables: true,
    bold: true,
    italic: true,
    strikethrough: true,
    inlineCode: true,
    codeBlocks: true,
    blockquote: true,
    anchorLinks: true,
    lists: true,
    headings: true,
    defaultMaxLength: 1e5
  },
  // Discord: CommonMark-style **bold** *italic* ~~strike~~ `code` ```fences``` > quotes,
  // lists + `# ` headings. NO anchor-text links ([text](url) renders literally → degrade to
  // `text (url)`), NO tables. 2000-char per-message limit. Emphasis uses DOUBLE-char markers
  // (the renderer's non-singleChar path) — Discord renders single `*_~` literally and never
  // 400s, so a stray literal asterisk is left as-is (escapeText is telegram-only).
  "discord-md": {
    tables: false,
    bold: true,
    italic: true,
    strikethrough: true,
    inlineCode: true,
    codeBlocks: true,
    blockquote: true,
    anchorLinks: false,
    lists: true,
    headings: true,
    defaultMaxLength: 2e3
  }
};
var DIALECT_ALIASES = {
  plain: "plaintext",
  text: "plaintext",
  md: "commonmark",
  markdown: "commonmark"
};
function resolveDialect(d) {
  if (d in DIALECTS) return d;
  return DIALECT_ALIASES[d];
}
function dialectSupports(dialect, feature) {
  return DIALECTS[dialect][feature] === true;
}

// ../core/src/transports/render/split.ts
function defaultFormatMarker(index, total) {
  return ` (${index}/${total})`;
}
function markerWidth(total, formatMarker) {
  if (total <= 1) return 0;
  return formatMarker(total, total).length;
}
function packChunks(text, budget) {
  if (budget <= 0) return [text];
  const chunks = [];
  let cur = "";
  const flush = () => {
    if (cur.length > 0) {
      chunks.push(cur);
      cur = "";
    }
  };
  const appendWord = (word) => {
    let w = word;
    while (w.length > 0) {
      const sep = cur.length > 0 ? " " : "";
      if (cur.length + sep.length + w.length <= budget) {
        cur += sep + w;
        return;
      }
      if (cur.length === 0) {
        chunks.push(w.slice(0, budget));
        w = w.slice(budget);
        continue;
      }
      flush();
    }
  };
  const lines = text.split("\n");
  for (let li = 0; li < lines.length; li++) {
    const line = lines[li];
    const sep = cur.length > 0 ? "\n" : "";
    if (cur.length + sep.length + line.length <= budget) {
      cur += sep + line;
    } else {
      if (cur.length > 0) flush();
      const words = line.split(" ");
      for (const word of words) appendWord(word);
    }
  }
  flush();
  return chunks.length > 0 ? chunks : [""];
}
function splitToParts(text, maxLength, opts = {}) {
  const formatMarker = opts.formatMarker ?? defaultFormatMarker;
  if (text.length <= maxLength) return [text];
  let prevTotal = -1;
  let chunks = [];
  let total = Math.ceil(text.length / Math.max(1, maxLength - markerWidth(99, formatMarker))) + 1;
  for (let i = 0; i < 4 && total !== prevTotal; i++) {
    prevTotal = total;
    const budget = Math.max(1, maxLength - markerWidth(total, formatMarker));
    chunks = packChunks(text, budget);
    total = chunks.length;
  }
  if (total <= 1) return chunks;
  const finalBudget = Math.max(1, maxLength - markerWidth(total, formatMarker));
  chunks = packChunks(text, finalBudget);
  total = chunks.length;
  return chunks.map((c, idx) => total > 1 ? `${c}${formatMarker(idx + 1, total)}` : c);
}

// ../core/src/transports/render/render.ts
var TG_V2_TEXT_SPECIAL = /[\\_*[\]()~`>#+\-=|{}.!]/g;
var TG_V2_CODE_SPECIAL = /[\\`]/g;
function escapeText(text, dialect) {
  if (dialect !== "telegram-markdownv2") return text;
  return text.replace(TG_V2_TEXT_SPECIAL, (c) => "\\" + c);
}
function escapeCode(text, dialect) {
  if (dialect !== "telegram-markdownv2") return text;
  return text.replace(TG_V2_CODE_SPECIAL, (c) => "\\" + c);
}
function escapeLinkUrl(url, dialect) {
  if (dialect !== "telegram-markdownv2") return url;
  return url.replace(/[\\)]/g, (c) => "\\" + c);
}
function renderRaw(raw, dialect) {
  const stripped = raw.replace(/<!--[\s\S]*?-->/g, "");
  if (stripped.trim() === "") return "";
  return escapeText(stripped, dialect);
}
function singleCharEmphasis(dialect) {
  return dialect === "whatsapp" || dialect === "telegram-markdownv2";
}
function renderInline(tokens, ctx) {
  if (!tokens || tokens.length === 0) return "";
  let out = "";
  for (const t of tokens) out += renderInlineToken(t, ctx);
  return out;
}
function renderInlineToken(token, ctx) {
  const t = token;
  switch (t.type) {
    case "text":
    case "escape":
      return escapeText(t.text ?? "", ctx.dialect);
    case "strong": {
      const inner = renderInline(t.tokens, ctx);
      if (!dialectSupports(ctx.dialect, "bold")) {
        ctx.degraded.push({ feature: "bold", from: "**", to: "plaintext" });
        return inner;
      }
      if (ctx.suppressStrong) return inner;
      return singleCharEmphasis(ctx.dialect) ? `*${inner}*` : `**${inner}**`;
    }
    case "em": {
      const inner = renderInline(t.tokens, ctx);
      if (!dialectSupports(ctx.dialect, "italic")) {
        ctx.degraded.push({ feature: "italic", from: "*", to: "plaintext" });
        return inner;
      }
      return singleCharEmphasis(ctx.dialect) ? `_${inner}_` : `*${inner}*`;
    }
    case "del": {
      const inner = renderInline(t.tokens, ctx);
      if (!dialectSupports(ctx.dialect, "strikethrough")) {
        ctx.degraded.push({ feature: "strikethrough", from: "~~", to: "plaintext" });
        return inner;
      }
      return singleCharEmphasis(ctx.dialect) ? `~${inner}~` : `~~${inner}~~`;
    }
    case "codespan": {
      const text = t.text ?? "";
      if (!dialectSupports(ctx.dialect, "inlineCode")) {
        ctx.degraded.push({ feature: "inlineCode", from: "`", to: "plaintext" });
        return escapeText(text, ctx.dialect);
      }
      return `\`${escapeCode(text, ctx.dialect)}\``;
    }
    case "link": {
      const link = t;
      const text = renderInline(link.tokens, ctx) || escapeText(link.text || link.href, ctx.dialect);
      if (dialectSupports(ctx.dialect, "anchorLinks")) {
        return `[${text}](${escapeLinkUrl(link.href, ctx.dialect)})`;
      }
      ctx.degraded.push({ feature: "anchorLinks", from: "[text](url)", to: "text (url)" });
      return `${text} (${link.href})`;
    }
    case "image": {
      const img = t;
      ctx.degraded.push({ feature: "images", from: "![alt](url)", to: "alt (url)" });
      return escapeText(`${img.text || "image"} (${img.href})`, ctx.dialect);
    }
    case "br":
      return "\n";
    default:
      return renderRaw(t.text ?? t.raw ?? "", ctx.dialect);
  }
}
function renderTextTable(table, ctx) {
  const header = table.header.map((c) => renderInline(c.tokens, ctx).trim());
  const rows = table.rows.map((r) => r.map((c) => renderInline(c.tokens, ctx).trim()));
  const cols = header.length;
  const widths = [];
  for (let i = 0; i < cols; i++) {
    let w = header[i]?.length ?? 0;
    for (const row of rows) w = Math.max(w, row[i]?.length ?? 0);
    widths[i] = w;
  }
  const fmtRow = (cells) => cells.map((c, i) => (c ?? "").padEnd(widths[i] ?? 0)).join("  ").trimEnd();
  const lines = [fmtRow(header), ...rows.map(fmtRow)];
  return lines.join("\n");
}
function renderBlockToken(token, ctx) {
  const t = token;
  switch (t.type) {
    case "space":
      return "";
    case "paragraph":
      return renderInline(t.tokens, ctx);
    case "text": {
      const tt = t;
      return tt.tokens ? renderInline(tt.tokens, ctx) : escapeText(tt.text ?? "", ctx.dialect);
    }
    case "heading": {
      const h = t;
      if (dialectSupports(ctx.dialect, "headings")) {
        return `${"#".repeat(Math.max(1, Math.min(6, h.depth)))} ${renderInline(h.tokens, ctx)}`;
      }
      const prevSuppress = ctx.suppressStrong;
      ctx.suppressStrong = true;
      const inner = renderInline(h.tokens, ctx);
      ctx.suppressStrong = prevSuppress;
      if (dialectSupports(ctx.dialect, "bold") && inner.trim() !== "") {
        ctx.degraded.push({ feature: "headings", from: `h${h.depth}`, to: "bold line" });
        return singleCharEmphasis(ctx.dialect) ? `*${inner}*` : `**${inner}**`;
      }
      ctx.degraded.push({ feature: "headings", from: `h${h.depth}`, to: "plain line" });
      return inner;
    }
    case "code": {
      const c = t;
      if (dialectSupports(ctx.dialect, "codeBlocks")) {
        return "```" + (c.lang ?? "") + "\n" + escapeCode(c.text, ctx.dialect) + "\n```";
      }
      ctx.degraded.push({ feature: "codeBlocks", from: "```", to: "plaintext" });
      return escapeText(c.text, ctx.dialect);
    }
    case "blockquote": {
      const b = t;
      const inner = b.tokens.map((tok) => renderBlockToken(tok, ctx)).filter((s) => s !== "").join("\n");
      if (dialectSupports(ctx.dialect, "blockquote")) {
        return inner.split("\n").map((l) => `> ${l}`).join("\n");
      }
      ctx.degraded.push({ feature: "blockquote", from: ">", to: "plaintext" });
      return inner;
    }
    case "list": {
      const list = t;
      const lines = list.items.map((item, idx) => {
        const rawMarker = list.ordered ? `${(typeof list.start === "number" ? list.start : 1) + idx}.` : "-";
        const marker = escapeText(rawMarker, ctx.dialect);
        const itemTokens = item.tokens;
        const nestedLists = itemTokens.filter((tok) => tok.type === "list");
        const checkboxTok = itemTokens.find((tok) => tok.type === "checkbox");
        const cbPrefix = checkboxTok ? renderInlineToken(checkboxTok, ctx) : "";
        const body = itemTokens.filter((tok) => {
          const ty = tok.type;
          return ty !== "list" && ty !== "checkbox";
        }).map((tok) => renderBlockToken(tok, ctx)).filter((s) => s !== "").join("\n");
        const inner = (cbPrefix + body).trim();
        const head = `${marker} ${inner}`;
        if (nestedLists.length === 0) return head;
        const sub = nestedLists.map((tok) => renderBlockToken(tok, ctx)).join("\n").split("\n").map((l) => `  ${l}`).join("\n");
        return `${head}
${sub}`;
      });
      return lines.join("\n");
    }
    case "table": {
      const table = t;
      if (dialectSupports(ctx.dialect, "tables")) {
        const head = `| ${table.header.map((c) => renderInline(c.tokens, ctx).trim()).join(" | ")} |`;
        const sep = `| ${table.header.map(() => "---").join(" | ")} |`;
        const body = table.rows.map((r) => `| ${r.map((c) => renderInline(c.tokens, ctx).trim()).join(" | ")} |`).join("\n");
        return [head, sep, body].join("\n");
      }
      ctx.degraded.push({ feature: "tables", from: "markdown table", to: "text-table" });
      return renderTextTable(table, ctx);
    }
    case "hr":
      return escapeText("---", ctx.dialect);
    case "html":
      return renderRaw(t.text ?? t.raw ?? "", ctx.dialect);
    default:
      return renderRaw(t.text ?? t.raw ?? "", ctx.dialect);
  }
}
function renderForDialect(markdown, dialect, opts = {}) {
  const resolved = resolveDialect(dialect);
  const ctx = { dialect: resolved ?? "plaintext", degraded: [] };
  const tokens = lexer(markdown);
  const blocks = [];
  for (const tok of tokens) {
    const rendered = renderBlockToken(tok, ctx);
    if (rendered !== "") blocks.push(rendered);
  }
  const text = blocks.join("\n\n");
  const maxLength = opts.maxLength ?? DIALECTS[ctx.dialect].defaultMaxLength;
  const parts = splitToParts(text, maxLength, {
    formatMarker: (index, total) => escapeText(` (${index}/${total})`, ctx.dialect)
  });
  return { parts, degraded: ctx.degraded };
}

// ../core/src/transports/send-pipeline.ts
var FALLBACK_MAX_LENGTH = 4096;
async function renderAndPace(opts) {
  const { content, dialect, maxLength, pacer, send } = opts;
  const render = opts.render ?? renderForDialect;
  let parts;
  let fellBackToPlaintext = false;
  try {
    parts = render(content, dialect, { maxLength }).parts;
  } catch {
    fellBackToPlaintext = true;
    parts = splitToParts(content, maxLength ?? FALLBACK_MAX_LENGTH);
  }
  let partsSent = 0;
  for (const part of parts) {
    await pacer.acquire();
    await send(part);
    partsSent++;
  }
  return { partsSent, ackAt: partsSent > 0 ? Date.now() : null, fellBackToPlaintext };
}

// ../core/src/transports/mcp-wire-adapter.ts
var DEFAULT_PROBE_TIMEOUT_MS = 5e3;
var HEALTH_PROBE_TIMEOUT_MS = 5e3;
var HANDSHAKE_TIMEOUT_MS = 3e4;
var UPSTREAM_MAX_ATTEMPTS = 5;
var UPSTREAM_HEALTHY_PROBES_TO_RESET = 2;
var INBOUND_DIVERGENCE_SUSTAINED_THRESHOLD = 3;
var INBOUND_MAX_ATTEMPTS = 3;
var LAST_MESSAGE_ID_LRU_CAP = 200;
var SAFE_ENV_KEYS = ["PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL", "LC_CTYPE", "SHELL", "USER", "LOGNAME", "NODE_PATH", "SELFHEAL_WATCHDOG_OFF", "SELFHEAL_DROP_INBOUND"];
function safeBaseline() {
  const out = {};
  for (const k of SAFE_ENV_KEYS) {
    const v = process.env[k];
    if (v != null) out[k] = v;
  }
  return out;
}
var ChildStreamTransport = class {
  constructor(child) {
    this.child = child;
  }
  onclose;
  onerror;
  onmessage;
  readBuffer = new ReadBuffer();
  closed = false;
  protocolVersion;
  setProtocolVersion(version) {
    this.protocolVersion = version;
  }
  onData = (chunk) => {
    this.readBuffer.append(chunk);
    try {
      let msg;
      while ((msg = this.readBuffer.readMessage()) !== null) this.onmessage?.(msg);
    } catch (e) {
      this.onerror?.(e);
    }
  };
  onErr = (e) => this.onerror?.(e);
  onEnd = () => {
    if (!this.closed) {
      this.closed = true;
      this.onclose?.();
    }
  };
  async start() {
    const out = this.child.stdout;
    if (!out) throw new Error("child has no stdout");
    out.on("data", this.onData);
    out.on("error", this.onErr);
    out.on("end", this.onEnd);
    this.child.on("exit", this.onEnd);
  }
  async send(message) {
    const stdin = this.child.stdin;
    if (!stdin || !stdin.writable) throw new Error("child stdin not writable");
    await new Promise((resolve2, reject) => {
      stdin.write(serializeMessage(message), (err) => err ? reject(err) : resolve2());
    });
  }
  async close() {
    this.closed = true;
    this.child.stdout?.off("data", this.onData);
    this.child.stdout?.off("error", this.onErr);
    this.child.stdout?.off("end", this.onEnd);
    this.child.off("exit", this.onEnd);
    this.onclose?.();
  }
};
var McpWireAdapter = class extends EventEmitter2 {
  type = "transport";
  id;
  supervisor;
  cursor;
  client = null;
  transport = null;
  status = initialStatus();
  attaching = false;
  connectCount = 0;
  draining = false;
  drainQueued = false;
  firstConnect = null;
  /** Startup handshake deadline (ms) — see HANDSHAKE_TIMEOUT_MS. */
  handshakeTimeoutMs;
  /** Framework-render mode config (D-EXEC-2): undefined dialect → legacy raw send. */
  dialect;
  maxLength;
  pacer;
  /** Host-resource stats provider for liveness-probe disconnect classification (D-A2). */
  hostStats;
  /**
   * D-A1: whether this transport participates in the heartbeat liveness sweep.
   * Public so the heartbeat can read it off the plugin instance without a cast.
   * Default false — only the generic factory sets this true; live named factories
   * (Telegram, Baileys) stay off until the S6 cutover.
   */
  livenessProbe;
  /**
   * A3: the connector's keepalive/poll cadence in ms (manifest-`upstream`-derived). Public so the
   * heartbeat sweep can read it off the plugin instance and derive the per-connector staleThreshold.
   * Undefined when the connector declares no persistent cadence (push/none).
   */
  keepaliveMs;
  /**
   * 2a.2: the connector's exported upstream self-window in ms. Public so the heartbeat sweep can
   * read it off the plugin instance and derive `staleThreshold = max(floor, this)` (structural
   * coupling). Undefined when the connector exports no self-window (→ kind-cadence fallback).
   */
  upstreamSelfWindowMs;
  /**
   * D-B1: whether this transport opts into the Tier-2 self-heal bridge. Public so the bridge
   * can read it without a cast. Default false — only the generic factory sets it true.
   */
  selfHeal;
  /**
   * D-B8 (DEV-2): the class of the most recent live disconnect, recorded by the liveness probe.
   * Feeds the supervisor's `classifyExit` cause-source so a real process EXIT is env-classified
   * (a network blip never trips the breaker). `undefined` until the first classified disconnect.
   */
  lastDisconnectClass;
  /**
   * A2: the adapter's OWN consecutive upstream-dead detection count (bounded recovery budget,
   * distinct from the supervisor crash breaker). Bumped each time `transport/health` reports the
   * upstream socket dead/stale; reset to 0 on the first healthy report. On exhaustion the adapter
   * surfaces `needs_attention` + holds restart.
   */
  upstreamAttempts = 0;
  /**
   * 2a.3: count of CONSECUTIVE healthy upstream probes since the last dead/stale report. Reset to 0
   * the moment a probe reports dead/stale (a flap down-stroke breaks the streak). Only when this
   * reaches {@link UPSTREAM_HEALTHY_PROBES_TO_RESET} is `upstreamAttempts` cleared — so an isolated
   * heal cannot reset a flapping ladder. The decrement-not-reset that makes a flap reach the floor.
   */
  consecutiveHealthyProbes = 0;
  // 2b (M10-S9.10): successful end-to-end drains since the last attach (rebaselined on attach — see
  // attach()). Cross-checked against the connector's self-reported inboundEnqueued (NOT
  // inboundReceived — keying on the raw pre-filter count false-fires on ordinary filtered traffic
  // like fromMe echoes; fixed live, architect gate `wf_2be46c65`, commit 5b5cf170) to detect a
  // silent in-connector drop that never reaches transport/drain at all (health/ping stay honest).
  // The baseline pair anchors at the FIRST health check after (re)attach — a deficit model, not
  // tick-to-tick (see inbound-divergence.ts for why: a single dropped message must stay flagged
  // across checks, not just the one tick it arrived on).
  inboundDelivered = 0;
  inboundBaselineEnqueued = void 0;
  inboundBaselineDelivered = 0;
  inboundDivergenceStreak = 0;
  inboundAttempts = 0;
  /** Tier-0 T0.8 (2026-07-02): set at a divergence recycle; evaluated at the first post-attach
   *  health check. If the recycle did not recover the un-drained inbound, a real message was
   *  probably lost — emit ONE plain-language 'inbound-loss-suspected' so the user can be told to
   *  resend (never a technical page; per the CTO's paging principle 2026-07-02). */
  pendingInboundLoss = void 0;
  // 2b gate fix (D11): consecutive NON-divergent checks, mirroring consecutiveHealthyProbes'
  // decrement-not-reset shape — a flapping divergence (divergent/healthy/divergent…) must not evade
  // the ladder via a single healthy tick between drops, so `inboundAttempts` only clears after
  // INBOUND_DIVERGENCE_SUSTAINED_THRESHOLD consecutive non-divergent checks, never on upstream
  // health alone (an inbound-only failure keeps the upstream healthy by definition — see D11).
  consecutiveInboundHealthyProbes = 0;
  /**
   * M10-S9 discoverability: the set of MCP tool names declared by the connector, captured once
   * after each successful MCP handshake (`listTools()` best-effort — failure → empty set, never
   * blocks connect). Re-captured on every respawn/re-attach since the connector may change.
   * Exposed via `supportsTool(name)` — the honest reaction-gating primitive.
   */
  connectorTools = /* @__PURE__ */ new Set();
  /**
   * M10-S9 B1: bounded insertion-ordered map of (recipient → most-recent provider message id).
   * Keyed by the `to` recipient string. Capped at LAST_MESSAGE_ID_LRU_CAP entries total; when
   * the cap is exceeded, the oldest insertion is evicted. In-memory only — a restart loses
   * pre-restart ids; reactions/edits target recent messages.
   */
  lastMessageIds = /* @__PURE__ */ new Map();
  constructor(o) {
    super();
    this.id = o.id;
    this.livenessProbe = o.livenessProbe ?? false;
    this.keepaliveMs = o.keepaliveMs;
    this.upstreamSelfWindowMs = o.upstreamSelfWindowMs;
    this.selfHeal = o.selfHeal ?? false;
    this.handshakeTimeoutMs = o.handshakeTimeoutMs ?? HANDSHAKE_TIMEOUT_MS;
    this.cursor = new CursorStore(join2(o.cursorDir, o.id));
    this.hostStats = o.hostStats;
    if (o.dialect) {
      this.dialect = o.dialect;
      this.maxLength = o.maxLength;
      this.pacer = new OutboundPacer({ ratePerMin: o.sendRatePerMin });
    }
    this.supervisor = new TransportSupervisor({
      id: o.id,
      command: o.command,
      args: o.args,
      cwd: o.cwd,
      // Passed DOWN as a resolver: the supervisor re-invokes it per spawn, so both the safe
      // baseline (PATH etc.) and the caller's scoped env are re-read on every respawn — a secret
      // corrected while the connector flaps reaches the next respawn (env freshness).
      scopedEnv: () => ({
        ...safeBaseline(),
        ...typeof o.scopedEnv === "function" ? o.scopedEnv() : o.scopedEnv
      }),
      backoffMs: o.backoffMs,
      maxRestarts: o.maxRestarts,
      windowMs: o.windowMs,
      preSpawn: o.preSpawn,
      // D-B8 (DEV-2): feed the live disconnect cause-source so a real process EXIT is
      // env-classified (network_down → breaker-skipped), not just the probe path. Pass A
      // proved this seam by injection; here it is wired to the adapter's classified state.
      classifyExit: () => this.lastDisconnectClass ?? "transient"
    });
    this.supervisor.on("spawn", (child) => {
      void this.attach(child).catch((e) => this.onAttachError(e));
    });
    this.supervisor.on("broken", () => {
      this.status.connected = false;
      this.status.running = false;
      this.status.lastError = "crash-loop breaker tripped \u2014 connector needs attention";
      this.transitionRecoveryState("needs_attention", "crash-loop-breaker");
      this.firstConnect?.reject(new Error(this.status.lastError));
      this.firstConnect = null;
    });
  }
  // ── lifecycle ──────────────────────────────────────────────────
  async connect() {
    const existing = this.supervisor.childProcess();
    if (existing && this.status.connected) return;
    const p = new Promise((resolve2, reject) => {
      this.firstConnect = { resolve: resolve2, reject };
    });
    if (existing) {
      void this.attach(existing).catch((e) => this.onAttachError(e));
    }
    await this.supervisor.start();
    await p;
  }
  async disconnect() {
    if (this.client) {
      try {
        await this.client.close();
      } catch {
      }
    }
    this.client = null;
    this.transport = null;
    await this.supervisor.stop();
    this.status.connected = false;
    this.status.running = false;
  }
  /** Re-establish the MCP client to the SAME live connector (provider keeps its backlog),
   *  then re-drain from the persisted cursor. Does NOT kill the process. See DECISIONS.md D5. */
  async forceReconnect() {
    const child = this.supervisor.childProcess();
    if (!child) throw new Error("forceReconnect: no live connector child");
    this.status.connected = false;
    this.status.reconnectAttempts++;
    await this.attach(child);
  }
  /**
   * D-A8: the connector reports a terminal, human-actionable condition (logged-out / pairing-
   * expired). Surface `needs_attention` honestly and HOLD restart (no retry storm) until a human
   * re-pairs. This is NOT a Tier-2 escalation — logged-out is a re-pair, not code drift.
   */
  onNeedsAttention(params) {
    this.status.connected = false;
    this.status.lastDisconnect = { at: /* @__PURE__ */ new Date(), status: "logged_out", error: params?.detail ?? params?.reason ?? "needs_attention", loggedOut: true };
    this.supervisor.holdRestart();
    this.transitionRecoveryState("needs_attention", params?.reason ?? "logged_out", { forceEmit: true });
  }
  /**
   * Liveness oracle (S5 Pass A, spec §8): a half-open socket — the child is alive but the MCP
   * client is wedged/deaf, or the child died without an `exit` event — leaves `connected=true` lying.
   * Issue a real MCP ping with a short timeout; on timeout/reject mark the socket honest-false,
   * record the disconnect (classified), surface `reconnecting`, and RECYCLE the process (D-A9 —
   * a deaf process can't be re-attached, the handshake hangs; only a fresh spawn recovers it).
   * Benign + idempotent: a healthy connector answers and nothing changes (no recycle).
   *
   * Driven by the heartbeat tick (no new timer). No-op when not connected. MUST NOT await a
   * reconnect (recycle is fire-and-forget; the healthy transition lands when the fresh child attaches).
   */
  async probeLiveness(opts) {
    if (!this.client || !this.status.connected) return;
    const timeout = opts?.timeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS;
    try {
      await this.client.ping({ timeout });
    } catch (err) {
      const cause = { loggedOut: false, error: err instanceof Error ? err.message : String(err) };
      const cls = classifyDisconnect(cause, this.hostStats?.());
      this.lastDisconnectClass = cls;
      this.status.connected = false;
      this.status.lastDisconnect = { at: /* @__PURE__ */ new Date(), status: "disconnected", error: cause.error };
      if (cls === "logged_out") this.transitionRecoveryState("needs_attention", "logged_out");
      else this.transitionRecoveryState("reconnecting");
      this.emit("liveness-lost", cls);
      if (cls !== "logged_out") this.recycleOrAttention();
      return;
    }
    await this.checkUpstreamHealth(opts);
  }
  /**
   * A2: after a successful stdio ping, consult the connector's self-reported upstream liveness.
   * An EXPLICIT post-ping branch (NOT a synthetic throw into the ping catch): a healthy upstream
   * heals the dot + resets the ladder; a dead/stale upstream routes into `handleUpstreamHealth`.
   * Any throw/absence of the tool degrades to ping-only (never crashes the probe).
   */
  async checkUpstreamHealth(opts) {
    if (this.supportsTool(HEALTH_TOOL)) {
      let health;
      try {
        const res = await this.client.callTool(
          { name: HEALTH_TOOL, arguments: {} },
          void 0,
          { timeout: HEALTH_PROBE_TIMEOUT_MS }
        );
        health = parseHealth(res);
      } catch {
        health = void 0;
      }
      if (health && await this.handleUpstreamHealth(health, opts)) return;
      if (health) {
        this.status.upstreamConnected = true;
        if (health.lastUpstreamActivityMs !== void 0) this.status.lastUpstreamActivityMs = health.lastUpstreamActivityMs;
      }
      if (health?.inboundReceived !== void 0 && health.inboundEnqueued !== void 0 && health.inboundReceived > health.inboundEnqueued) {
        console.warn(`[wire:${this.id}] connector filtered/dropped pre-enqueue (informational, not a framework alarm): received=${health.inboundReceived} enqueued=${health.inboundEnqueued}`);
      }
      if (health?.inboundEnqueued !== void 0 && this.inboundBaselineEnqueued === void 0) {
        this.inboundBaselineEnqueued = health.inboundEnqueued;
        this.inboundBaselineDelivered = this.inboundDelivered;
        if (this.pendingInboundLoss) {
          const recovered = this.inboundDelivered > this.pendingInboundLoss.deliveredAtRecycle;
          if (!recovered) {
            console.warn(`[wire:${this.id}] inbound-loss SUSPECTED: divergence recycle did not recover the un-drained inbound (delivered still ${this.inboundDelivered}); surfacing a plain-language user notice`);
            this.emit("inbound-loss-suspected");
          } else {
            console.warn(`[wire:${this.id}] inbound recovered by the recycle (delivered ${this.pendingInboundLoss.deliveredAtRecycle} \u2192 ${this.inboundDelivered}) \u2014 no user notice`);
          }
          this.pendingInboundLoss = void 0;
        }
      }
      const deliveredAtLastCheck = this.inboundDelivered;
      const divergence = computeInboundDivergence({
        inboundEnqueued: health?.inboundEnqueued,
        baselineInboundEnqueued: this.inboundBaselineEnqueued,
        inboundDeliveredNow: this.inboundDelivered,
        baselineInboundDelivered: this.inboundBaselineDelivered,
        priorStreak: this.inboundDivergenceStreak,
        sustainedThreshold: INBOUND_DIVERGENCE_SUSTAINED_THRESHOLD
      });
      this.inboundDivergenceStreak = divergence.streak;
      if (divergence.streak === 0) {
        this.consecutiveInboundHealthyProbes++;
        if (this.consecutiveInboundHealthyProbes >= INBOUND_DIVERGENCE_SUSTAINED_THRESHOLD && this.inboundAttempts > 0) {
          this.inboundAttempts = 0;
        }
      } else {
        this.consecutiveInboundHealthyProbes = 0;
      }
      if (divergence.sustained) {
        console.warn(`[wire:${this.id}] SUSTAINED inbound divergence: connector reports traffic arriving but nothing has drained in ${INBOUND_DIVERGENCE_SUSTAINED_THRESHOLD} consecutive checks \u2014 enqueued=${health?.inboundEnqueued} (baseline ${this.inboundBaselineEnqueued}) delivered=${this.inboundDelivered} (baseline ${this.inboundBaselineDelivered}) received=${health?.inboundReceived} cursor=${this.cursor.load()}`);
        this.inboundDivergenceStreak = 0;
        await this.reDrainBeforeRecycle();
        console.warn(`[wire:${this.id}] inbound divergence re-drain outcome: delivered=${this.inboundDelivered} (was ${deliveredAtLastCheck} at alarm) cursor=${this.cursor.load()} \u2014 ${this.inboundDelivered === deliveredAtLastCheck ? "nothing drained \u2192 escalating" : "drain recovered the backlog"}`);
        if (this.inboundDelivered === deliveredAtLastCheck) {
          this.inboundAttempts++;
          if (this.inboundAttempts > INBOUND_MAX_ATTEMPTS) {
            this.status.connected = false;
            this.supervisor.holdRestart();
            this.transitionRecoveryState("needs_attention", "inbound-silent-failure-exhausted");
          } else {
            this.status.connected = false;
            this.pendingInboundLoss = { deliveredAtRecycle: this.inboundDelivered };
            this.transitionRecoveryState("reconnecting");
            this.recycleOrAttention();
          }
        }
      }
    }
    this.consecutiveHealthyProbes++;
    if (this.consecutiveHealthyProbes >= UPSTREAM_HEALTHY_PROBES_TO_RESET && this.upstreamAttempts > 0) {
      this.upstreamAttempts = 0;
      this.emit("recovered");
    }
    this.lastDisconnectClass = void 0;
    if (this.status.recoveryState === "reconnecting" && this.status.connected) {
      this.transitionRecoveryState("healthy");
    }
    void this.triggerDrain().catch(() => {
    });
  }
  /**
   * A2: route a dead/stale upstream report into the bounded recovery ladder. Returns `true` when it
   * has handled the report (caller returns), `false` when the upstream is healthy (caller heals).
   *
   *  - reason:'logged_out'  → terminal human-action condition → needs_attention + hold restart, no loop.
   *  - exhausted own budget → needs_attention + hold restart (the bound; Invariant 5).
   *  - reason:'transient'   → DEFER the heavy recycle (prefer the connector's own in-process reconnect):
   *                           keep MCP `connected` true so the heartbeat keeps re-probing the ladder.
   *  - otherwise (stale half-open / network / unspecified) → escalate to a fresh process (recycle).
   *
   * The supervisor crash breaker is deliberately fed `network_down` (not `upstream_dead`) so a
   * recycle-induced exit does NOT also trip it — the adapter's OWN counter is the single bound here.
   */
  async handleUpstreamHealth(health, opts) {
    const now = Date.now();
    const stale = health.lastUpstreamActivityMs !== void 0 && opts?.staleThresholdMs !== void 0 && now - health.lastUpstreamActivityMs > opts.staleThresholdMs;
    const dead = health.upstreamConnected === false;
    if (!dead && !stale) return false;
    this.consecutiveHealthyProbes = 0;
    this.status.upstreamConnected = false;
    if (health.lastUpstreamActivityMs !== void 0) this.status.lastUpstreamActivityMs = health.lastUpstreamActivityMs;
    console.warn(
      `[wire:${this.id}] upstream health DECISION: ${dead ? "dead" : "stale"} reason=${health.reason ?? "unspecified"} lastUpstreamActivityMs=${health.lastUpstreamActivityMs ?? "n/a"}`
    );
    if (health.reason === "logged_out" || health.reason === "credential") {
      this.lastDisconnectClass = "logged_out";
      this.emit("liveness-lost", "logged_out");
      this.onNeedsAttention({
        reason: health.reason === "credential" ? "credential" : "logged_out",
        ...health.reason === "credential" ? { detail: "the stored credential is invalid or incomplete \u2014 re-enter the secret" } : {}
      });
      this.upstreamAttempts = 0;
      return true;
    }
    this.upstreamAttempts++;
    this.lastDisconnectClass = "network_down";
    this.status.lastDisconnect = {
      at: /* @__PURE__ */ new Date(),
      status: "disconnected",
      error: dead ? "upstream socket down" : "upstream socket stale (no recent activity)"
    };
    this.emit("liveness-lost", "upstream_dead");
    if (this.upstreamAttempts > UPSTREAM_MAX_ATTEMPTS) {
      this.status.connected = false;
      this.supervisor.holdRestart();
      this.transitionRecoveryState("needs_attention", "upstream-recovery-exhausted");
      return true;
    }
    this.transitionRecoveryState("reconnecting");
    if (health.reason === "transient") return true;
    await this.reDrainBeforeRecycle();
    this.status.connected = false;
    this.recycleOrAttention();
    return true;
  }
  /**
   * S1.2 (D1): re-drain to exhaustion over the live pipe before a recycle. Bounded — a wedged pipe
   * that cannot complete the drain within the health timeout PROVES the pipe is wedged, and we
   * recycle anyway (the caller does). Best-effort: any error is swallowed because the durable cursor
   * re-drains again as soon as the fresh child attaches, so nothing is lost either way.
   */
  async reDrainBeforeRecycle() {
    const drain = this.triggerDrain().catch(() => {
    });
    await Promise.race([drain, new Promise((resolve2) => setTimeout(resolve2, HEALTH_PROBE_TIMEOUT_MS))]);
  }
  /**
   * RECYCLE the connector, falling through to an honest `needs_attention` when a recycle would be a
   * no-op (the recoveryState wedge fix). `supervisor.recycle()` silently no-ops when there is no
   * live child (held / stopped / between restarts) or after the breaker has tripped — which would
   * otherwise leave the §6 dot stuck on 'reconnecting' while nothing is actually recovering.
   */
  recycleOrAttention() {
    const st = this.supervisor.status();
    if (st.needsAttention) {
      this.transitionRecoveryState("needs_attention", "crash-loop-breaker");
      return;
    }
    if (this.supervisor.childProcess() === null) {
      this.transitionRecoveryState("needs_attention", "no-live-child");
      return;
    }
    this.supervisor.recycle();
  }
  // ── send ───────────────────────────────────────────────────────
  async send(to, message) {
    if (!this.client) throw new Error("not connected");
    const client = this.client;
    const media = message.mediaHandles && message.mediaHandles.length > 0 ? message.mediaHandles : void 0;
    let mediaAttached = false;
    const transmit = async (content) => {
      const args = { to, content };
      if (media && !mediaAttached) {
        args.media = media;
        mediaAttached = true;
      }
      const result = await client.callTool({ name: SEND_TOOL, arguments: args });
      return parseSendResult(result);
    };
    if (this.dialect && this.pacer) {
      let lastId;
      await renderAndPace({
        content: message.content,
        dialect: this.dialect,
        maxLength: this.maxLength,
        pacer: this.pacer,
        send: async (content) => {
          const id2 = await transmit(content);
          if (id2 !== void 0) lastId = id2;
        }
      });
      this.recordMessageId(to, lastId);
      return;
    }
    const id = await transmit(message.content);
    this.recordMessageId(to, id);
  }
  /**
   * M10-S9 B1: record the most recent provider message id for a recipient.
   * Updates the LRU map — evicts the oldest entry when the cap is exceeded.
   * No-ops when `id` is undefined (old connectors that return nothing).
   */
  recordMessageId(to, id) {
    if (id === void 0) return;
    if (this.lastMessageIds.has(to)) this.lastMessageIds.delete(to);
    else if (this.lastMessageIds.size >= LAST_MESSAGE_ID_LRU_CAP) {
      const oldest = this.lastMessageIds.keys().next().value;
      if (oldest !== void 0) this.lastMessageIds.delete(oldest);
    }
    this.lastMessageIds.set(to, id);
  }
  /**
   * M10-S9 B1: the provider id of the most recent message sent to `to`, if the connector
   * reported one. In-memory — a restart loses pre-restart ids; reactions/edits target
   * recent messages only.
   */
  lastSentMessageId(to) {
    return this.lastMessageIds.get(to);
  }
  /**
   * Invoke a control-plane tool by name (e.g. transport/quiesce, Pass C). Thin passthrough to the
   * MCP client; throws if not connected. The framework parses the result. Bytes never cross here —
   * control-plane JSON only.
   */
  async callControlTool(name, args) {
    if (!this.client) throw new Error("not connected");
    return this.client.callTool({ name, arguments: args });
  }
  /**
   * Pull the connector's current onboarding artifacts (QR/code/status) during an interactive
   * handshake (M10.5). Empty when the connector doesn't implement the tool (back-compat:
   * secret-only onboarding) or returns nothing. NEVER throws — a method-not-found / transient
   * error must not wedge onboarding. Uses the existing `callControlTool` wrapper (like typing/
   * reaction/edit), so it inherits the not-connected guard. The connector may return artifacts
   * under `structuredContent` or a JSON text block — we read whichever the SDK provides.
   * The bytes never cross here (an image artifact carries a `MediaHandle`, never bytes).
   */
  async onboardingPoll(refresh = false) {
    try {
      const res = await this.callControlTool(ONBOARDING_POLL_TOOL, { refresh });
      const payload = res?.structuredContent ?? (() => {
        try {
          return JSON.parse(res?.content?.[0]?.text ?? "{}");
        } catch {
          return {};
        }
      })();
      return parseOnboardingArtifacts(payload);
    } catch {
      return [];
    }
  }
  // ── typing lifecycle (M10-S6 Pass B addendum) ──────────────────────
  // Drive a "typing…" indicator over the control plane. The framework fires these best-effort
  // via `fireHook` (connection-gated + catch-drop), so any throw here never breaks a turn. The
  // connector owns the "how" (Telegram's refresh/auto-expire); we only relay start/stop.
  async onTypingStart(to) {
    await this.callControlTool(TYPING_TOOL, { to, action: "start" });
  }
  async onTypingStop(to) {
    await this.callControlTool(TYPING_TOOL, { to, action: "stop" });
  }
  // ── reaction + edit (M10-S9 B2) ────────────────────────────────
  // Control-plane signals, best-effort semantics identical to typing: the framework
  // connection-gates + catch-drops via `fireHook`; the connector swallows its own errors.
  /**
   * React to a message with an emoji. Transmits a `transport/reaction` control-plane signal.
   * Best-effort — a failure must never break a turn.
   */
  async sendReaction(to, messageId, emoji) {
    await this.callControlTool(REACTION_TOOL, { to, messageId, emoji });
  }
  /**
   * Edit a previously-sent message. Transmits a `transport/edit` control-plane signal.
   *
   * When a render dialect is configured, the content is rendered to the dialect's format
   * before transmission. Unlike `send`, an edit can NEVER multi-part — if the rendered
   * content exceeds the dialect's length cap (or the `maxLength` override), it is truncated
   * to one part with an "…" suffix rather than split into multiple messages. An edit replaces
   * a single message, not a thread of parts.
   *
   * Best-effort — a failure must never break a turn.
   */
  async editMessage(to, messageId, content) {
    let wire = content;
    if (this.dialect) {
      try {
        const result = renderForDialect(content, this.dialect, { maxLength: this.maxLength });
        wire = result.parts[0] ?? content;
        if (result.parts.length > 1) {
          const d = resolveDialect(this.dialect);
          const cap = this.maxLength ?? (d ? DIALECTS[d].defaultMaxLength : 4096);
          wire = wire.replace(/ \\?\(1\/\d+\\?\)\s*$/, "");
          wire = wire.slice(0, cap - 1) + "\u2026";
        }
      } catch {
      }
    }
    await this.callControlTool(EDIT_TOOL, { to, messageId, content: wire });
  }
  /**
   * Mark an inbound message read (read receipt). Transmits a `transport/mark_read` control-plane
   * signal. Best-effort — a failure must never break a turn.
   */
  async onMarkRead(messageId, from) {
    await this.callControlTool(MARK_READ_TOOL, { messageId, from });
  }
  // ── status ─────────────────────────────────────────────────────
  transportStatus() {
    const s = this.supervisor.status();
    return {
      ...this.status,
      running: s.running,
      reconnectAttempts: Math.max(this.status.reconnectAttempts, s.restarts),
      // A6: surface the supervisor's recycle telemetry on the transport status.
      recycleCount: s.recycleCount,
      recycleRateAlert: s.recycleRateAlert
    };
  }
  /** The MCP protocol version negotiated during the last successful handshake.
   *  The SDK calls `transport.setProtocolVersion(v)` (if present) after initialize; we
   *  capture it on ChildStreamTransport and surface it here for the conformance harness. */
  negotiatedProtocolVersion() {
    return this.transport?.protocolVersion;
  }
  /** The render dialect this adapter is in (render mode), or undefined for legacy raw send.
   *  M10-S6 Pass B: feeds the manager's getDialect → the never-lie generation hint. */
  effectiveDialect() {
    return this.dialect;
  }
  // ── notification routing ───────────────────────────────────────
  /**
   * The connector's notifications are WAKEUPS — the cursor / poll tool is the source of truth.
   * Bound as a field arrow so it can be handed to `client.fallbackNotificationHandler` (which
   * invokes it unbound) and keeps `this`.
   *   - MESSAGE_AVAILABLE          → drain the backlog from the durable cursor.
   *   - NEEDS_ATTENTION            → terminal human-action condition (logged-out / re-pair).
   *   - ONBOARDING_ARTIFACT_NOTIF  → (G1) re-emit a framework-side wakeup so the dashboard
   *                                  PairingSession driver re-polls + re-presents (a rotating
   *                                  QR can't expire before the user scans).
   */
  handleNotification = async (n) => {
    try {
      if (n.method === MESSAGE_AVAILABLE) await this.triggerDrain();
      else if (n.method === NEEDS_ATTENTION) this.onNeedsAttention(n.params);
      else if (n.method === ONBOARDING_ARTIFACT_NOTIF) this.emit("onboarding_artifact");
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (!/Connection closed|-32000/.test(msg)) console.warn(`[wire:${this.id}] notification handler error: ${msg}`);
    }
  };
  /** Test seam: returns the notification handler so a unit test can exercise the wakeup branches
   *  without spawning a connector process. */
  buildNotificationHandlerForTest() {
    return this.handleNotification;
  }
  // ── internals ──────────────────────────────────────────────────
  async attach(child) {
    if (this.attaching) return;
    this.attaching = true;
    try {
      if (this.client) {
        try {
          await this.client.close();
        } catch {
        }
      }
      this.client = null;
      this.transport = null;
      const transport = new ChildStreamTransport(child);
      const client = new Client({ name: `transport-${this.id}`, version: "1.0.0" });
      client.fallbackNotificationHandler = this.handleNotification;
      const handshakeAbort = new AbortController();
      const handshakeTimer = setTimeout(() => handshakeAbort.abort(), this.handshakeTimeoutMs);
      try {
        await client.connect(transport, { signal: handshakeAbort.signal });
      } catch (e) {
        if (handshakeAbort.signal.aborted) {
          this.onHandshakeTimeout();
          return;
        }
        throw e;
      } finally {
        clearTimeout(handshakeTimer);
      }
      this.connectCount++;
      this.client = client;
      this.transport = transport;
      this.status.running = true;
      this.status.connected = true;
      this.status.lastConnectedAt = /* @__PURE__ */ new Date();
      this.lastDisconnectClass = void 0;
      this.supervisor.noteAttached();
      this.transitionRecoveryState("healthy");
      this.inboundBaselineEnqueued = void 0;
      this.inboundBaselineDelivered = 0;
      this.inboundDivergenceStreak = 0;
      this.consecutiveInboundHealthyProbes = 0;
      try {
        const listed = await client.listTools();
        this.connectorTools = new Set(listed.tools.map((t) => t.name));
      } catch {
        this.connectorTools = /* @__PURE__ */ new Set();
      }
      await this.triggerDrain();
      this.firstConnect?.resolve();
      this.firstConnect = null;
    } finally {
      this.attaching = false;
    }
  }
  onAttachError(e) {
    this.status.connected = false;
    this.status.lastError = e.message;
    if (!/connection closed|-32000/i.test(e.message)) this.emit("error", e);
    this.firstConnect?.reject(e);
    this.firstConnect = null;
  }
  /**
   * The startup MCP handshake exceeded its deadline (attach()): the child spawned but its MCP server
   * never answered `initialize` — a deaf/wedged startup. This is definitionally a PRE-ATTACH failure
   * (noteAttached() runs only after a successful handshake), the same class the supervisor's breaker
   * already routes to needs_attention on a crash-loop. Rather than recycle-loop a child that will only
   * re-hang (5×30s never fits the 60s breaker window), land the EXISTING needs_attention lane directly
   * — mirroring the upstream/inbound bounded-ladder exhaustion paths — with a plain reason.
   *
   * Kill the deaf child via stop() (SIGTERM/SIGKILL) so the supervisor does NOT keep-alive respawn into
   * another hang; `stopped` is cleared by a later heal/manual retry (connect() → supervisor.start(),
   * which also resets the hold + breaker state). Rejecting firstConnect lets boot's awaited connect()
   * proceed: the autowire loop logs + continues to the next transport, and the heartbeat starts.
   */
  onHandshakeTimeout() {
    this.status.connected = false;
    this.status.running = false;
    this.status.lastError = `startup MCP handshake did not complete within ${this.handshakeTimeoutMs}ms`;
    void this.supervisor.stop().catch(() => {
    });
    this.transitionRecoveryState("needs_attention", "startup-handshake-timeout");
    this.firstConnect?.reject(new Error(this.status.lastError));
    this.firstConnect = null;
  }
  /** Single-flight drain (DECISIONS.md D6): never run two drains concurrently; if a wakeup
   *  arrives mid-drain, queue exactly one more pass. Read cursor → emit all → persist last. */
  async triggerDrain() {
    if (this.draining) {
      this.drainQueued = true;
      return;
    }
    this.draining = true;
    try {
      do {
        this.drainQueued = false;
        await this.drainOnce();
      } while (this.drainQueued);
    } finally {
      this.draining = false;
    }
  }
  async drainOnce() {
    const client = this.client;
    if (!client) return;
    const sinceCursor = this.cursor.load();
    let res;
    try {
      res = await client.callTool({ name: DRAIN_TOOL, arguments: { sinceCursor } });
    } catch (err) {
      if (err instanceof Error && /connection closed|not connected|-32000/i.test(err.message)) return;
      throw err;
    }
    const rawMessages = parseDrain(res);
    const after = sinceCursor != null ? Number(sinceCursor) : 0;
    const messages = after > 0 ? rawMessages.filter((m) => Number(m.cursor) > after) : rawMessages;
    if (messages.length !== rawMessages.length) {
      console.warn(
        `[wire:${this.id}] CONNECTOR CONTRACT VIOLATION: drain(sinceCursor=${sinceCursor}) returned ${rawMessages.length - messages.length} entr(ies) at/below the cursor \u2014 dropped (never re-emitted). The connector's cursor assignment or drain filter is buggy.`
      );
    }
    if (messages.length === 0) return;
    this.inboundDelivered += messages.length;
    let lastCursor = null;
    for (const m of messages) {
      const kind = m.kind ?? "message";
      let content;
      let emitKind;
      if (kind === "reaction") {
        const emoji = m.emoji ?? "?";
        content = m.content && m.content.trim().length > 0 ? m.content : `*reacted ${emoji} to your message*`;
        emitKind = "reaction";
      } else if (kind === "edit") {
        content = `*edited their earlier message \u2014 new text: ${m.content}*`;
        emitKind = "edit";
      } else {
        content = m.content;
        emitKind = "message";
      }
      const incoming = {
        id: m.cursor,
        from: m.from,
        // UNTRUSTED raw provider sender id — passed through verbatim (owner-binding is Pass C)
        content,
        timestamp: new Date(m.timestamp),
        channelId: this.id,
        kind: emitKind,
        // S4 Pass A: passthrough media handles when present (JSON-only, never bytes).
        // Omit the field entirely for plain messages to preserve the undefined contract.
        ...m.media && m.media.length > 0 ? { mediaHandles: m.media } : {},
        // M10-S9 B3: thread providerMessageId through when the connector supplies it.
        // Enables react-to-user's-message. Omit the field when absent (back-compat).
        ...m.providerMessageId !== void 0 ? { providerMessageId: m.providerMessageId } : {}
      };
      this.emit("message", incoming);
      this.status.lastMessageAt = incoming.timestamp;
      if (lastCursor === null || Number(m.cursor) > Number(lastCursor)) lastCursor = m.cursor;
    }
    if (lastCursor !== null) this.cursor.save(lastCursor);
  }
  // ── test hook ──────────────────────────────────────────────────
  /** Returns the persisted inbound offset (proves a fresh adapter reloads it). */
  loadedCursorForTest() {
    return this.cursor.load();
  }
  /** Exposes the captured connector tool set for tests (proves listTools capture). */
  connectorToolsForTest() {
    return this.connectorTools;
  }
  /** Counts successful Client.connect() calls (proves attach() is single-flight). */
  connectCountForTest() {
    return this.connectCount;
  }
  /** Returns the supervisor's total spawn count (proves recycle produced a fresh process). */
  spawnCountForTest() {
    return this.supervisor.status().spawnCount;
  }
  // ── self-heal bridge surface (S5 Pass B + P4 B1) ────────────────
  /** The supervisor instance — the bridge attaches an `escalate` listener to it (D-B1). */
  getSupervisor() {
    return this.supervisor;
  }
  /** Whether this transport opted into the Tier-2 self-heal bridge (D-B1). */
  isSelfHeal() {
    return this.selfHeal;
  }
  /**
   * P4 B1: the adapter's 'needs-attention' event emitter surface for the self-heal bridge.
   * The adapter already emits `'needs-attention'` on pairing failures; the bridge subscribes
   * here to escalate pairing reasons into the CFR repair path (without a new timer or socket).
   * Returns `this` — McpWireAdapter extends EventEmitter.
   */
  getNeedsAttentionEmitter() {
    return this;
  }
  /**
   * M10-S9 discoverability: whether the connected connector declares a given MCP tool by name.
   * Returns false when not connected or when `listTools()` failed at attach time.
   * Use `supportsTool('transport/reaction')` to gate the reaction announcement + honest error.
   */
  supportsTool(name) {
    return this.connectorTools.has(name);
  }
  /**
   * Drive the §6 status dot from the self-heal bridge (D-B7): `self_healing` while the CFR
   * re-derives, `healthy` on a restored reconnect, `needs_attention` on surrender / re-pair.
   * Core stays ignorant of the CFR — the BRIDGE owns the CFR-lifecycle → state mapping.
   */
  setRecoveryState(state) {
    this.transitionRecoveryState(state, "heal-outcome", { forceEmit: state === "needs_attention" });
  }
  /**
   * Tier-0 S3 (2026-07-02): the SINGLE recoveryState transition point. Every write goes through
   * here so the two observable edges fire consistently for ALL incident classes, not just the
   * upstream ladder:
   *   → 'needs_attention'  emits 'needs-attention' (pager pages; plugin refreshes the dashboard tier)
   *   → 'healthy' (from a non-healthy state) emits 'recovered' (pager latch clears per incident;
   *     the manager status cache — which otherwise lags attach(), the S1 stale-cache hang — refreshes)
   * Before this, 'recovered' fired only on the upstream-ladder clear, so a crash-loop / inbound /
   * logged_out incident latched the pager for the transport's lifetime after its first page.
   */
  transitionRecoveryState(next, reason, opts) {
    const prev = this.status.recoveryState;
    this.status.recoveryState = next;
    if (prev === next && !opts?.forceEmit) return;
    if (next === "needs_attention") this.emit("needs-attention", reason ?? "needs_attention");
    else if (next === "healthy" && prev !== next) this.emit("recovered");
  }
  /** The class of the most recent classified disconnect (DEV-2 wiring proof). */
  lastDisconnectClassForTest() {
    return this.lastDisconnectClass;
  }
};
function parseDrain(res) {
  const content = res.content;
  const text = content?.find((c) => c.type === "text")?.text;
  if (!text) return [];
  const parsed = JSON.parse(text);
  return Array.isArray(parsed.messages) ? parsed.messages : [];
}
function parseHealth(res) {
  try {
    const r = res;
    const payload = r.structuredContent ?? (() => {
      const text = r.content?.find((c) => c.type === "text")?.text;
      return text ? JSON.parse(text) : void 0;
    })();
    if (payload === void 0) return void 0;
    const parsed = HealthToolOutputSchema.safeParse(payload);
    return parsed.success ? parsed.data : void 0;
  } catch {
    return void 0;
  }
}
function parseSendResult(res) {
  try {
    const content = res.content;
    const text = content?.find((c) => c.type === "text")?.text;
    if (!text) return void 0;
    const parsed = JSON.parse(text);
    const id = parsed?.messageId;
    return typeof id === "string" && id.length > 0 ? id : void 0;
  } catch {
    return void 0;
  }
}

// ../core/src/transports/transport-icon.ts
import { createRequire } from "node:module";
var requireCjs = createRequire(import.meta.url);

// ../core/src/transports/generic-transport-plugin.ts
function buildAdapter(o) {
  const resolveScopedEnv = () => {
    const scopedEnv = { ...o.extraEnv ?? {} };
    for (const k of o.requiredEnv) {
      const v = process.env[k];
      if (v != null) scopedEnv[k] = v;
    }
    return scopedEnv;
  };
  return new McpWireAdapter({
    id: o.id,
    command: o.mcp.command,
    args: o.mcp.args,
    cwd: o.folderAbs,
    scopedEnv: resolveScopedEnv,
    cursorDir: o.cursorDir,
    dialect: o.dialect,
    sendRatePerMin: o.sendRatePerMin,
    keepaliveMs: o.keepaliveMs,
    // A3: manifest-upstream-derived cadence for the staleThreshold
    upstreamSelfWindowMs: o.upstreamSelfWindowMs,
    // 2a.2: connector-exported self-window (coupling)
    // D-A1: generic (agent-authored) transports opt into the heartbeat liveness sweep by default.
    // Post-de-framework the live Telegram runs through THIS generic factory too (selfHeal:true) —
    // the old "named factories excluded" carve-out no longer applies; any connector that doesn't
    // want it passes livenessProbe:false explicitly.
    livenessProbe: o.livenessProbe ?? true,
    // D-B1: generic transports also opt into the Tier-2 self-heal bridge. With the live Telegram now
    // on this generic path, the no-live-bot-regression invariant rests on session_held / secret-only
    // onboarding semantics + the live-pair gates (A7), NOT on a "named factory is excluded" assumption.
    selfHeal: true,
    preSpawn: async () => connectorDepsResolve(o.folderAbs) ? "ready" : (await runConnectorSetup(o.folderAbs)).ready ? "ready" : "needs-setup"
  });
}
function createGenericTransportPlugin(o) {
  return buildAdapter(o);
}

// ../core/src/transports/conformance/outcome.ts
function decideOutcome(probe, surface, echoMatched) {
  if (!probe.canMockExercise(surface)) return "declared-unproven";
  return echoMatched ? "proven" : "fail";
}
function emptyReport() {
  return { certified: false, provenFlags: [], declaredUnproven: [], failures: [], structural: [], ranAt: "" };
}

// ../core/src/transports/conformance/cert-fingerprint.ts
import { createHash } from "node:crypto";
import { existsSync as existsSync2, readFileSync as readFileSync4, readdirSync, statSync } from "node:fs";
import { join as join3 } from "node:path";
var COSMETIC_FRONTMATTER_KEYS = /* @__PURE__ */ new Set([
  "icon",
  "description"
]);
function stableStringify(v) {
  if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
  if (Array.isArray(v)) return "[" + v.map(stableStringify).join(",") + "]";
  const o = v;
  return "{" + Object.keys(o).sort().map((k) => JSON.stringify(k) + ":" + stableStringify(o[k])).join(",") + "}";
}
function hashDirInto(h, dir, folderAbs, label) {
  if (!existsSync2(dir)) return;
  const files = [];
  const walk = (d) => {
    for (const e of readdirSync(d)) {
      if (e === "node_modules" || e.startsWith(".")) continue;
      const full = join3(d, e);
      let s;
      try {
        s = statSync(full);
      } catch {
        continue;
      }
      if (s.isDirectory()) walk(full);
      else files.push(full);
    }
  };
  walk(dir);
  for (const f of files.sort()) {
    h.update("\0" + label + ":" + f.slice(folderAbs.length) + "\0");
    h.update(readFileSync4(f));
  }
}
function certInputFingerprint(folderAbs) {
  try {
    const capPath = join3(folderAbs, "CAPABILITY.md");
    if (!existsSync2(capPath)) return null;
    const h = createHash("sha256");
    const { data } = readFrontmatter(capPath);
    const fm = data ?? {};
    const relevant = {};
    for (const k of Object.keys(fm)) {
      if (!COSMETIC_FRONTMATTER_KEYS.has(k)) relevant[k] = fm[k];
    }
    h.update("frontmatter\0" + stableStringify(relevant) + "\0");
    const mcpPath = join3(folderAbs, ".mcp.json");
    if (existsSync2(mcpPath)) {
      h.update("mcp\0");
      h.update(readFileSync4(mcpPath));
      h.update("\0");
    }
    for (const dep of ["package.json", "package-lock.json"]) {
      const p = join3(folderAbs, dep);
      if (existsSync2(p)) {
        h.update(dep + "\0");
        h.update(readFileSync4(p));
        h.update("\0");
      }
    }
    hashDirInto(h, join3(folderAbs, "scripts"), folderAbs, "script");
    hashDirInto(h, join3(folderAbs, "src"), folderAbs, "file");
    return h.digest("hex");
  } catch {
    return null;
  }
}

// ../core/src/transports/conformance/live-socket-suite.ts
import { join as join4 } from "node:path";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
function runLiveSocketSuite(adapter, received) {
  const probes = [];
  probes.push(...cursorProbes(adapter.loadedCursorForTest()));
  const hasFrom = received.some((m) => typeof m.from === "string" && m.from.length > 0);
  probes.push({ name: "untrusted-from-surfaced", passed: hasFrom, detail: hasFrom ? void 0 : "no raw sender surfaced for a downstream gate" });
  return probes;
}
function cursorProbes(cursor) {
  const probes = [];
  probes.push({
    name: "cursor-durability",
    passed: cursor !== null,
    detail: cursor === null ? "no offset persisted after round-trip (connector dropped the message?)" : `cursor=${cursor}`
  });
  if (cursor !== null) {
    const t = cursor.trim();
    const safe = t.length > 0 && Number.isSafeInteger(Number(t));
    probes.push({
      name: "cursor-numeric",
      passed: safe,
      detail: safe ? void 0 : `persisted cursor ${JSON.stringify(cursor)} is not a comparison-safe integer \u2192 the durable monotonic guard at cursor-store.ts:14 (Number(cursor) <= Number(cur)) would mis-compare: a non-numeric value coerces to NaN (guard silently no-ops, offset regresses), and a value above 2^53 (e.g. a raw Discord snowflake) collides with its neighbours under Number() (distinct ids compare EQUAL \u2192 offset stalls). Use a small connector-owned monotonic integer counter as the cursor (WhatsApp String(++seq) precedent), NOT a raw snowflake.`
    });
  }
  return probes;
}
async function onboardingHandshakeProbe(adapter, opts = {}) {
  const NAME = "onboarding-handshake";
  const maxPolls = opts.maxPolls ?? 5;
  const delayMs = opts.delayMs ?? 50;
  const requirePairingTruth = opts.requirePairingTruth ?? false;
  let sawRecognized = false;
  let sawLinked = false;
  let sawPreflight = false;
  let linkedRegistered;
  for (let i = 0; i < maxPolls; i++) {
    let arts = [];
    try {
      arts = await adapter.onboardingPoll();
    } catch {
      arts = [];
    }
    if (arts.length > 0) sawRecognized = true;
    if (i === 0) {
      const hasLinkingArtifact = arts.some((a) => a.kind === "image" || a.kind === "code");
      const hasPreflight = arts.some(
        (a) => a.kind === "status" && (a.state === "preflight_ok" || a.state === "preflight_rejected")
      );
      const resumedLinked = arts.some(
        (a) => a.kind === "status" && a.state === "linked" && a.registered === true
      );
      if (resumedLinked && !hasLinkingArtifact && !hasPreflight) {
        return {
          name: NAME,
          passed: true,
          detail: "inapplicable: connector resumed an already-registered session (no onboarding handshake to verify) \u2014 cert-lockout recurrence guard"
        };
      }
    }
    if (requirePairingTruth) {
      for (const a of arts) {
        if (a.kind === "status" && (a.state === "preflight_ok" || a.state === "preflight_rejected")) {
          sawPreflight = true;
        }
        if (a.kind === "status" && a.state === "linked") {
          linkedRegistered = a.registered;
        }
      }
    }
    if (arts.some((a) => a.kind === "status" && a.state === "linked")) {
      sawLinked = true;
      break;
    }
    if (i < maxPolls - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  if (!sawRecognized) {
    return { name: NAME, passed: false, detail: `connector declared transport/onboarding_poll but never yielded a recognized artifact within ${maxPolls} polls` };
  }
  if (!sawLinked) {
    return { name: NAME, passed: false, detail: `onboarding never reached status:linked within ${maxPolls} polls` };
  }
  if (requirePairingTruth) {
    if (!sawPreflight) {
      return { name: NAME, passed: false, detail: "pairing connector never emitted a preflight status (preflight_ok/rejected) before linked" };
    }
    if (linkedRegistered !== true) {
      return { name: NAME, passed: false, detail: "reached status:linked without registered:true (ground-truth attestation missing)" };
    }
  }
  return { name: NAME, passed: true };
}
var ADVERSARIAL_FLAGS = ["open_not_registered", "selftest_during_pairing"];
async function pairingAdversarialProbe(o) {
  const NAME = "pairing-adversarial";
  const MAX_POLLS = 8;
  const POLL_DELAY_MS = 150;
  for (const flag of ADVERSARIAL_FLAGS) {
    const cursorDir = mkdtempSync(join4(tmpdir(), `adv-probe-${flag}-`));
    const adapter = new McpWireAdapter({
      id: `adv-${flag}`,
      command: o.mcp.command,
      args: o.mcp.args,
      cwd: o.folderAbs,
      scopedEnv: { HARNESS_SIMULATE: flag },
      cursorDir,
      backoffMs: 500
    });
    try {
      await adapter.connect();
      for (let i = 0; i < MAX_POLLS; i++) {
        let arts = [];
        try {
          arts = await adapter.onboardingPoll();
        } catch {
          arts = [];
        }
        for (const a of arts) {
          if (a.kind === "status") {
            const s = a;
            if (s.state === "linked" || s.registered === true) {
              return {
                name: NAME,
                passed: false,
                detail: `connector reported ${JSON.stringify({ state: s.state, registered: s.registered })} under HARNESS_SIMULATE=${flag} (${flag}: connector must not promote to linked/registered under this adversarial condition)`
              };
            }
          }
        }
        if (i < MAX_POLLS - 1) await new Promise((r) => setTimeout(r, POLL_DELAY_MS));
      }
    } catch {
    } finally {
      await adapter.disconnect().catch(() => {
      });
    }
  }
  return { name: NAME, passed: true };
}

// ../core/src/transports/conformance/style-probes.ts
function surfaceHasDialectSupporting(surface, feature) {
  return surface.dialects.some((d) => {
    const resolved = resolveDialect(d);
    return resolved ? dialectSupports(resolved, feature) : false;
  });
}
function echoEqualsRender(payload) {
  return (echo, dialect, render) => echo.trim() === render(payload, dialect).trim();
}
function makeProbe(opts) {
  const { flag, payload, feature, surfaceGate } = opts;
  return {
    flag,
    makePayload: () => payload,
    expectedRender: (dialect, render) => render(payload, dialect),
    assertEcho: echoEqualsRender(payload),
    canMockExercise: opts.canMockExercise ?? ((surface) => (feature ? surfaceHasDialectSupporting(surface, feature) : surface.dialects.length > 0) && (surfaceGate ? surfaceGate(surface) : true))
  };
}
var DIALECT_PROBE_PAYLOAD = "Render **bold**, a [site](https://example.com), and `code`.";
var dialectProbe = makeProbe({
  flag: "dialect",
  payload: DIALECT_PROBE_PAYLOAD,
  // Any advertised dialect can carry the basic inline render; the mock just needs to exist.
  canMockExercise: (surface) => surface.dialects.length > 0
});
function makeOverCapPayload(cap) {
  const lineCount = Math.ceil((cap + 1) / 28) + 5;
  return Array.from({ length: lineCount }, (_, i) => `line ${String(i).padStart(4, "0")} of the max-length over-cap probe`).join("\n");
}
function stripMarker(s) {
  return s.replace(/\s*\(\d+\/\d+\)\s*$/, "");
}
function reassemble(parts) {
  return parts.map(stripMarker).join("\n").replace(/\n{2,}/g, "\n").trim();
}
function judgeMaxLengthParts(parts, cap, original) {
  if (parts.some((p) => p.length > cap)) return false;
  return reassemble(parts) === original.replace(/\n{2,}/g, "\n").trim();
}
var MAX_LENGTH_PAYLOAD = Array.from({ length: 12 }, (_, i) => `line ${i} of the size probe`).join("\n");
var maxLengthProbe = {
  ...makeProbe({
    flag: "max_length",
    payload: MAX_LENGTH_PAYLOAD,
    // Not a per-dialect markup feature — the mock can exercise size carry whenever it advertises
    // a finite ceiling (and any dialect to render into).
    canMockExercise: (s) => s.dialects.length > 0 && s.maxLength != null && s.maxLength > 0
  }),
  /** S3 Pass B: over-cap payload for the bounded-split assertion (length > cap guaranteed). */
  overCapPayload: makeOverCapPayload,
  /** S3 Pass B: proven iff every part ≤ cap AND parts reassemble to the original. */
  assertParts: judgeMaxLengthParts
};
var NESTED_LIST_PAYLOAD = "- alpha\n- beta\n  - gamma\n- delta";
var nestedListProbe = {
  flag: "nestedList",
  makePayload: () => NESTED_LIST_PAYLOAD,
  expectedRender: (dialect, render) => render(NESTED_LIST_PAYLOAD, dialect),
  canMockExercise: (surface) => surface.dialects.length > 0,
  assertEcho(echo, dialect, render) {
    if (echo.trim() !== render(NESTED_LIST_PAYLOAD, dialect).trim()) return false;
    const lines = echo.split("\n");
    const hasGamma = lines.some((l) => l.trimEnd().endsWith("gamma"));
    if (!hasGamma) return false;
    const glued = lines.some((l) => /beta.{0,2}-/.test(l) && /gamma/.test(l));
    return !glued;
  }
};
var PROBE_REGISTRY = {
  tables: makeProbe({
    flag: "tables",
    payload: "| a | b |\n|---|---|\n| 1 | 2 |",
    feature: "tables",
    surfaceGate: (s) => s.tables === true
  }),
  bold: makeProbe({ flag: "bold", payload: "**bold**", feature: "bold" }),
  anchorLinks: makeProbe({
    flag: "anchorLinks",
    payload: "[site](https://example.com)",
    feature: "anchorLinks"
  }),
  max_length: maxLengthProbe,
  dialect: dialectProbe,
  nestedList: nestedListProbe
};
var SYNTHETIC_IMAGE_HANDLE = {
  id: "probe-media-1",
  mimeType: "image/png",
  size: 1024,
  kind: "image",
  caption: "conformance probe image"
};
var IMAGES_MEDIA_PROBE = {
  flag: "images",
  canMockExercise: (surface) => surface.images === true,
  syntheticHandle: () => SYNTHETIC_IMAGE_HANDLE,
  assertMediaEcho(echoedHandles) {
    if (!echoedHandles || echoedHandles.length === 0) return false;
    return echoedHandles.some((h) => h.id === SYNTHETIC_IMAGE_HANDLE.id);
  }
};

// ../core/src/transports/conformance/mock-provider.ts
var MOCK_SURFACE = {
  dialects: ["plaintext", "whatsapp", "telegram-markdownv2", "commonmark"],
  images: true,
  tables: true,
  maxLength: 4096,
  upstreamControl: true
};
var MOCK_UPSTREAM_CONTROL_TOOL = "mock/upstream";
async function driveMockUpstream(plugin, up) {
  await plugin.callControlTool(MOCK_UPSTREAM_CONTROL_TOOL, { up });
}
async function readHealth(plugin) {
  const res = await plugin.callControlTool(HEALTH_TOOL, {});
  const r = res;
  let payload = r.structuredContent;
  if (payload === void 0) {
    const text = r.content?.find((c) => c.type === "text")?.text;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch {
        return void 0;
      }
    }
  }
  if (payload === void 0) return void 0;
  const parsed = HealthToolOutputSchema.safeParse(payload);
  return parsed.success ? parsed.data : void 0;
}
function stripFraming(content) {
  return content.replace(/^\[[a-z][a-z-]*:(?:local|steered)\] /, "");
}
function sendAndCapture(plugin, content, opts) {
  const timeoutMs = opts?.timeoutMs ?? 3e3;
  return new Promise((resolve2, reject) => {
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      plugin.off("message", onMessage);
    };
    const onMessage = (m) => {
      if (settled) return;
      const stripped = stripFraming(m.content);
      if (stripped.length === 0) return;
      settled = true;
      cleanup();
      resolve2(stripped);
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`sendAndCapture: no echo within ${timeoutMs}ms`));
    }, timeoutMs);
    plugin.on("message", onMessage);
    plugin.send("probe", { content }).catch((e) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(e);
    });
  });
}
function sendAndCaptureMedia(plugin, content, media, opts) {
  const timeoutMs = opts?.timeoutMs ?? 3e3;
  return new Promise((resolve2, reject) => {
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      plugin.off("message", onMessage);
    };
    const onMessage = (m) => {
      if (settled) return;
      const stripped = stripFraming(m.content);
      if (stripped.length === 0) return;
      settled = true;
      cleanup();
      const handles = m.mediaHandles;
      resolve2(Array.isArray(handles) ? handles : void 0);
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`sendAndCaptureMedia: no echo within ${timeoutMs}ms`));
    }, timeoutMs);
    plugin.on("message", onMessage);
    plugin.callControlTool("transport/send", { to: "probe", content, media }).catch((e) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(e);
    });
  });
}
async function sendAndCaptureAll(plugin, parts, opts) {
  const echoes = [];
  for (const part of parts) {
    const echo = await sendAndCapture(plugin, part, opts);
    echoes.push(echo);
  }
  return echoes;
}

// ../core/src/transports/conformance/health-probe.ts
var HEALTH_PROBE_NAME = "transport-health";
var FRESH_WINDOW_MS = 5 * 6e4;
function requiresHealthTool(upstream) {
  return upstream === "socket" || upstream === "push";
}
async function upstreamHealthProbe(plugin, manifest) {
  const required = requiresHealthTool(manifest.upstream);
  const declared = plugin.supportsTool(HEALTH_TOOL);
  if (required && !declared) {
    return {
      name: HEALTH_PROBE_NAME,
      passed: false,
      detail: `connector declares upstream:${manifest.upstream} (a persistent/provider-held connection that can go silently zombie/half-open) but does NOT advertise the ${HEALTH_TOOL} tool \u2014 the upstream-liveness mandate is not opt-out`
    };
  }
  if (!declared) return void 0;
  let up;
  try {
    up = await readHealth(plugin);
  } catch {
    up = void 0;
  }
  if (!up) {
    return { name: HEALTH_PROBE_NAME, passed: false, detail: `${HEALTH_TOOL} returned no valid HealthToolOutput while the upstream was up` };
  }
  if (up.upstreamConnected !== true) {
    return { name: HEALTH_PROBE_NAME, passed: false, detail: `connector reported upstreamConnected:false while its upstream is live (a healthy, idle-but-up connector must report true + advancing activity \u2014 false alarm)` };
  }
  if (up.lastUpstreamActivityMs !== void 0) {
    const age = Date.now() - up.lastUpstreamActivityMs;
    if (!(age >= 0 && age < FRESH_WINDOW_MS)) {
      return { name: HEALTH_PROBE_NAME, passed: false, detail: `connector reported upstreamConnected:true but a non-advancing lastUpstreamActivityMs (age=${age}ms) while its upstream is live` };
    }
  }
  if (MOCK_SURFACE.upstreamControl && plugin.supportsTool(MOCK_UPSTREAM_CONTROL_TOOL)) {
    let dead;
    let droveDown = false;
    try {
      await driveMockUpstream(plugin, false);
      droveDown = true;
      try {
        dead = await readHealth(plugin);
      } catch {
        dead = void 0;
      }
    } finally {
      if (droveDown) await driveMockUpstream(plugin, true).catch(() => {
      });
    }
    if (!dead) {
      return { name: HEALTH_PROBE_NAME, passed: false, detail: `${HEALTH_TOOL} returned no valid HealthToolOutput after the mock upstream was driven down` };
    }
    if (dead.upstreamConnected !== false) {
      return { name: HEALTH_PROBE_NAME, passed: false, detail: `NEVER-LIE VIOLATION: connector reported upstreamConnected:true while its mock upstream was driven DOWN (a lies-when-dead zombie that the framework probe would never recover)` };
    }
  }
  return { name: HEALTH_PROBE_NAME, passed: true };
}

// ../core/src/transports/conformance/inbound-probe.ts
var INBOUND_PROBE_NAME = "transport-inbound-delivery";
var DELIVER_INBOUND_CONTROL_TOOL = "mock/deliver-inbound";
async function inboundDeliveryProbe(plugin) {
  let before;
  try {
    before = await readHealth(plugin);
  } catch {
    before = void 0;
  }
  if (!before || before.inboundReceived === void 0) return void 0;
  if (!plugin.supportsTool(DELIVER_INBOUND_CONTROL_TOOL)) return void 0;
  const baselineCursor = await drainAndGetLastCursor(plugin, null);
  if (baselineCursor === void 0) {
    return { name: INBOUND_PROBE_NAME, passed: false, detail: "transport/drain call failed while baselining the cursor" };
  }
  try {
    await plugin.callControlTool(DELIVER_INBOUND_CONTROL_TOOL, {});
  } catch (e) {
    return { name: INBOUND_PROBE_NAME, passed: false, detail: `mock/deliver-inbound call failed: ${e.message}` };
  }
  let after;
  try {
    after = await readHealth(plugin);
  } catch {
    after = void 0;
  }
  if (!after || after.inboundReceived === void 0) {
    return { name: INBOUND_PROBE_NAME, passed: false, detail: "transport/health returned no inboundReceived after a simulated delivery" };
  }
  if (after.inboundReceived <= (before.inboundReceived ?? 0)) {
    return { name: INBOUND_PROBE_NAME, passed: false, detail: "inboundReceived did not advance after a simulated delivery \u2014 the control tool is not wired to the counter" };
  }
  let postDeliveryMessages;
  try {
    const res = await plugin.callControlTool(DRAIN_TOOL, { sinceCursor: baselineCursor });
    postDeliveryMessages = parseDrainMessages(res);
  } catch (e) {
    return { name: INBOUND_PROBE_NAME, passed: false, detail: `transport/drain call failed post-delivery: ${e.message}` };
  }
  if (!postDeliveryMessages || postDeliveryMessages.length === 0) {
    return {
      name: INBOUND_PROBE_NAME,
      passed: false,
      detail: "SILENT-INBOUND-FAILURE DETECTED: inboundReceived advanced (the connector claims traffic arrived) but transport/drain returned nothing new since the delivery \u2014 it never reached the backlog"
    };
  }
  return { name: INBOUND_PROBE_NAME, passed: true };
}
function parseDrainMessages(res) {
  const r = res;
  let payload = r.structuredContent;
  if (payload === void 0) {
    const text = r.content?.find((c) => c.type === "text")?.text;
    if (text) payload = JSON.parse(text);
  }
  const messages = payload?.messages;
  return Array.isArray(messages) ? messages : void 0;
}
async function drainAndGetLastCursor(plugin, sinceCursor) {
  try {
    const res = await plugin.callControlTool(DRAIN_TOOL, { sinceCursor });
    const messages = parseDrainMessages(res);
    if (!messages) return void 0;
    if (messages.length === 0) return sinceCursor;
    const last = messages[messages.length - 1];
    return typeof last.cursor === "string" ? last.cursor : sinceCursor;
  } catch {
    return void 0;
  }
}

// ../core/src/transports/conformance/temporal-probe.ts
var TEMPORAL_PROBE_NAME = "transport-inbound-temporal-contract";
var ECHO_COUNT = 3;
var ECHO_WAIT_MS = 3e3;
function fail(detail) {
  return { name: TEMPORAL_PROBE_NAME, passed: false, detail };
}
function waitFor(pred, timeoutMs) {
  return new Promise((resolve2, reject) => {
    const started = Date.now();
    const tick = () => {
      if (pred()) return resolve2();
      if (Date.now() - started > timeoutMs) return reject(new Error("timeout"));
      setTimeout(tick, 50);
    };
    tick();
  });
}
async function inboundTemporalProbe(plugin, received) {
  let before;
  try {
    before = await readHealth(plugin);
  } catch {
    before = void 0;
  }
  if (!before || before.inboundEnqueued === void 0) return void 0;
  const baseline = await drainMessages(plugin, null);
  if (baseline === void 0) return fail("transport/drain failed while baselining the cursor");
  const baseNum = baseline.length > 0 ? Math.max(...baseline.map((m) => Number(m.cursor))) : 0;
  const stamp = Math.random().toString(36).slice(2, 8);
  const markers = Array.from({ length: ECHO_COUNT }, (_, i) => `temporal-contract-${i + 1}-${stamp}`);
  for (const [i, marker] of markers.entries()) {
    try {
      await plugin.send("probe", { content: marker });
    } catch (e) {
      return fail(`echo send ${i + 1}/${ECHO_COUNT} failed: ${e.message}`);
    }
    try {
      await waitFor(() => received.some((m) => m.content.includes(marker)), ECHO_WAIT_MS);
    } catch {
      return fail(
        `WAKEUP STALL: committed delivery ${i + 1}/${ECHO_COUNT} never became adapter-visible \u2014 message_available is not emitted per message (inbound would stall behind a green dot)`
      );
    }
  }
  let after;
  try {
    after = await readHealth(plugin);
  } catch {
    after = void 0;
  }
  if (!after || after.inboundEnqueued === void 0) {
    return fail("inboundEnqueued disappeared from transport/health mid-probe");
  }
  const delta = after.inboundEnqueued - (before.inboundEnqueued ?? 0);
  if (delta !== ECHO_COUNT) {
    return fail(
      `COUNTER IDENTITY violated: ${ECHO_COUNT} committed deliveries but inboundEnqueued advanced by ${delta} \u2014 over-counting false-recycles a healthy connector; under-counting disables the drop net`
    );
  }
  const drained = await drainMessages(plugin, baseNum > 0 ? String(baseNum) : null);
  if (drained === void 0) return fail("transport/drain failed post-echo");
  for (const marker of markers) {
    if (!drained.some((m) => String(m.content).includes(marker))) {
      return fail(`drain from the baseline cursor lost committed delivery "${marker}" \u2014 an entry was born at/below the drained cursor (the mint-before-await collision class)`);
    }
  }
  let prev = baseNum;
  for (const m of drained) {
    const c = Number(m.cursor);
    if (!(c > prev)) {
      return fail(
        `CURSOR ORDER violated: drained cursor ${m.cursor} is not strictly above its predecessor/baseline (${prev}) \u2014 cursor-order must equal push-order or entries become undrainable`
      );
    }
    prev = c;
  }
  if (drained.length >= 2) {
    const mid = drained[drained.length - 2].cursor;
    const tail = await drainMessages(plugin, String(mid));
    if (tail === void 0) return fail("transport/drain failed on the mid-cursor check");
    if (tail.some((m) => Number(m.cursor) <= Number(mid))) {
      return fail(
        `DRAIN FILTER violated: drain(sinceCursor=${mid}) returned entries at/below the cursor \u2014 re-emitting already-drained entries duplicates inbound and masks real deficits`
      );
    }
    if (!tail.some((m) => String(m.content).includes(markers[ECHO_COUNT - 1]))) {
      return fail(`drain from a mid-batch cursor lost the newest committed delivery`);
    }
  }
  return { name: TEMPORAL_PROBE_NAME, passed: true };
}
async function drainMessages(plugin, sinceCursor) {
  try {
    const res = await plugin.callControlTool(DRAIN_TOOL, { sinceCursor });
    const r = res;
    let payload = r.structuredContent;
    if (payload === void 0) {
      const text = r.content?.find((c) => c.type === "text")?.text;
      if (text) payload = JSON.parse(text);
    }
    const messages = payload?.messages;
    if (!Array.isArray(messages)) return void 0;
    return messages;
  } catch {
    return void 0;
  }
}

// ../core/src/transports/conformance/degrade-probe.ts
var DEGRADE_PROBE_NAME = "transport-inbound-honest-degrade";
var DRAIN_POLL_MS = 250;
var DRAIN_WAIT_MS = 3e3;
function fail2(detail) {
  return { name: DEGRADE_PROBE_NAME, passed: false, detail };
}
async function inboundHonestDegradeProbe(plugin) {
  const stamp = Math.random().toString(36).slice(2, 8);
  const handle = {
    id: `degrade-probe-${stamp}`,
    mimeType: "image/png",
    size: 2048,
    kind: "image",
    caption: "degrade probe attachment"
  };
  const captionMarker = `degrade-caption-${stamp}`;
  const baselineA = await maxCursor(plugin);
  if (baselineA === void 0) return fail2("transport/drain failed while baselining (case A)");
  try {
    await plugin.callControlTool("transport/send", { to: "probe", content: captionMarker, media: [handle] });
  } catch (e) {
    return fail2(
      `transport/send THREW on the standard media field: ${e.message} \u2014 the wire send shape includes optional media; an unsupported kind must DEGRADE (caption + placeholder), never reject`
    );
  }
  const newA = await pollNewEntries(plugin, baselineA);
  if (newA === void 0) return fail2("transport/drain failed post-send (case A)");
  const captionCarried = newA.some(
    (m) => String(m.content ?? "").includes(captionMarker) || hasHandles(m)
  );
  if (newA.length === 0) {
    return fail2(
      "SILENT DROP (case A): an inbound with a caption + unsupported attachment produced NOTHING in the drainable backlog \u2014 the caption must always survive (degrade to text), never vanish"
    );
  }
  if (!captionCarried) {
    return fail2(
      "CAPTION SWALLOWED (case A): the inbound drained but the caption text was lost \u2014 degrade must carry the caption verbatim (plus a bracketed placeholder for the attachment)"
    );
  }
  const baselineB = await maxCursor(plugin);
  if (baselineB === void 0) return fail2("transport/drain failed while baselining (case B)");
  try {
    await plugin.callControlTool("transport/send", { to: "probe", content: "", media: [handle] });
  } catch (e) {
    return fail2(`transport/send threw on an attachment-only send: ${e.message} \u2014 degrade, never reject`);
  }
  const newB = await pollNewEntries(plugin, baselineB);
  if (newB === void 0) return fail2("transport/drain failed post-send (case B)");
  if (newB.length === 0) {
    return fail2(
      'SILENT DROP (case B): an attachment-only inbound (no caption \u2014 e.g. a bare photo) produced NOTHING in the drainable backlog. The user sent something and got silence \u2014 enqueue a degrade entry (e.g. "[photo]") for kinds you do not support'
    );
  }
  const nonSilent = newB.some((m) => stripFraming(String(m.content ?? "")).trim().length > 0 || hasHandles(m));
  if (!nonSilent) {
    return fail2(
      'EMPTY DEGRADE (case B): the attachment-only inbound drained as an EMPTY entry with no media handles \u2014 indistinguishable from silence downstream. Carry a bracketed placeholder (e.g. "[photo]")'
    );
  }
  return { name: DEGRADE_PROBE_NAME, passed: true };
}
function hasHandles(m) {
  return Array.isArray(m.media) && m.media.length > 0;
}
async function maxCursor(plugin) {
  const all = await drainFrom(plugin, null);
  if (all === void 0) return void 0;
  return all.length > 0 ? Math.max(...all.map((m) => Number(m.cursor))) : 0;
}
async function pollNewEntries(plugin, baseline) {
  const started = Date.now();
  for (; ; ) {
    const entries = await drainFrom(plugin, baseline > 0 ? String(baseline) : null);
    if (entries === void 0) return void 0;
    const fresh = entries.filter((m) => Number(m.cursor) > baseline);
    if (fresh.length > 0) return fresh;
    if (Date.now() - started > DRAIN_WAIT_MS) return [];
    await new Promise((r) => setTimeout(r, DRAIN_POLL_MS));
  }
}
async function drainFrom(plugin, sinceCursor) {
  try {
    const res = await plugin.callControlTool(DRAIN_TOOL, { sinceCursor });
    const r = res;
    let payload = r.structuredContent;
    if (payload === void 0) {
      const text = r.content?.find((c) => c.type === "text")?.text;
      if (text) payload = JSON.parse(text);
    }
    const messages = payload?.messages;
    return Array.isArray(messages) ? messages : void 0;
  } catch {
    return void 0;
  }
}

// ../core/src/transports/conformance/harness.ts
function waitFor2(pred, timeoutMs) {
  return new Promise((resolve2, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pred()) return resolve2();
      if (Date.now() - start > timeoutMs) return reject(new Error("waitFor timeout"));
      setTimeout(tick, 50);
    };
    tick();
  });
}
function declaredRenderFlags(m) {
  const flags = [];
  const style = m.style ?? {};
  if (typeof style.dialect === "string") flags.push("dialect");
  if (style.tables === true) flags.push("tables");
  if (style.bold === true) flags.push("bold");
  if (style.anchorLinks === true || style.links === true) flags.push("anchorLinks");
  if (style.max_length != null) flags.push("max_length");
  if (typeof style.dialect === "string") flags.push("nestedList");
  return flags;
}
function declaredNonRenderFlags(m) {
  const flags = [];
  for (const hook of m.lifecycle ?? []) flags.push(`lifecycle:${hook}`);
  return flags;
}
var renderForProver = (md, dialect) => renderForDialect(md, dialect, {}).parts.join("\n");
async function runRenderProver(plugin, manifest, report) {
  const declaredDialect = typeof manifest.style?.dialect === "string" ? manifest.style.dialect : "plaintext";
  if (!resolveDialect(declaredDialect)) {
    for (const flag of declaredRenderFlags(manifest)) {
      report.declaredUnproven.push({ flag, reason: `unknown dialect "${declaredDialect}"`, clearAtLivePair: true });
    }
    return;
  }
  for (const flag of declaredRenderFlags(manifest)) {
    const probe = PROBE_REGISTRY[flag];
    if (!probe || !probe.canMockExercise(MOCK_SURFACE)) {
      report.declaredUnproven.push({
        flag,
        reason: probe ? "mock cannot exercise (dialect/surface)" : "no render probe",
        clearAtLivePair: true
      });
      continue;
    }
    let echoMatched;
    try {
      if (probe.assertParts && probe.overCapPayload && MOCK_SURFACE.maxLength != null) {
        const cap = MOCK_SURFACE.maxLength;
        const overCapPayload = probe.overCapPayload(cap);
        const parts = splitToParts(overCapPayload, cap);
        const echoes = await sendAndCaptureAll(plugin, parts);
        echoMatched = probe.assertParts(echoes, cap, overCapPayload);
      } else {
        const wire = probe.expectedRender(declaredDialect, renderForProver);
        const echo = await sendAndCapture(plugin, wire);
        echoMatched = probe.assertEcho(echo, declaredDialect, renderForProver);
      }
    } catch {
      echoMatched = false;
    }
    const outcome = decideOutcome(probe, MOCK_SURFACE, echoMatched);
    if (outcome === "proven") report.provenFlags.push(flag);
    else if (outcome === "fail") {
      report.failures.push({
        flag,
        disproof: `declared dialect "${declaredDialect}" supports ${flag}, but the connector stripped/mangled it in carry`
      });
    } else report.declaredUnproven.push({ flag, reason: "mock cannot exercise (dialect/surface)", clearAtLivePair: true });
  }
}
async function runMediaProver(plugin, manifest, report) {
  const style = manifest.style ?? {};
  if (style.images !== true) return;
  const probe = IMAGES_MEDIA_PROBE;
  if (!probe.canMockExercise(MOCK_SURFACE)) {
    report.declaredUnproven.push({ flag: "images", reason: "mock cannot exercise (images not in surface)", clearAtLivePair: true });
    return;
  }
  const handle = probe.syntheticHandle();
  let echoedHandles;
  try {
    echoedHandles = await sendAndCaptureMedia(plugin, "conformance-media-probe", [handle]);
  } catch {
    echoedHandles = void 0;
  }
  const asShape = echoedHandles;
  if (probe.assertMediaEcho(asShape)) {
    report.provenFlags.push("images");
  } else {
    report.failures.push({
      flag: "images",
      disproof: `connector declared style.images:true but the media handle (id="${handle.id}") was not carried intact in the echoed inbound`
    });
  }
}
async function runTransportConformance(launch) {
  const report = emptyReport();
  report.ranAt = (/* @__PURE__ */ new Date()).toISOString();
  report.inputFingerprint = certInputFingerprint(launch.folderAbs) ?? void 0;
  const raw = launch.manifestOverride ?? readFrontmatter(join5(launch.folderAbs, "CAPABILITY.md")).data;
  const v = validateTransportManifest(raw);
  if (!v.valid) {
    report.structural.push({ name: "manifest-valid", passed: false, detail: v.errors.join("; ") });
    report.certified = false;
    return report;
  }
  report.structural.push({ name: "manifest-valid", passed: true });
  const plugin = createGenericTransportPlugin({
    id: launch.id,
    folderAbs: launch.folderAbs,
    mcp: launch.mcp,
    requiredEnv: launch.requiredEnv,
    cursorDir: launch.cursorDir,
    // S1.3 (a): tell the connector EXPLICITLY this is a cert run. The no-phone-but-REGISTERED recert
    // case has ECHO_MODE=false; without this flag a connector can only infer cert-ness from the
    // target shape — the exact live-poisoning the fix removes. The live auto-wire path never sets it.
    extraEnv: { TRANSPORT_CONFORMANCE: "1" }
  });
  try {
    const received = [];
    plugin.on("message", (m) => received.push({ from: m.from, content: m.content }));
    await plugin.connect();
    report.structural.push({ name: "spawn", passed: true });
    report.structural.push({ name: "handshake", passed: true });
    report.negotiatedProtocolVersion = plugin.negotiatedProtocolVersion();
    report.structural.push({ name: "protocolVersion", passed: !!report.negotiatedProtocolVersion });
    await plugin.send("probe", { content: "conformance-ping" });
    let roundTripped = false;
    try {
      await waitFor2(() => received.some((m) => m.content.includes("conformance-ping")), 3e3);
      roundTripped = true;
    } catch {
      roundTripped = false;
    }
    report.structural.push({ name: "wire-round-trip", passed: roundTripped });
    for (const p of runLiveSocketSuite(plugin, received)) report.structural.push(p);
    if (v.manifest.session_held !== false && !plugin.supportsTool("transport/onboarding_poll")) {
      report.structural.push({
        name: "onboarding-tool-declared",
        passed: false,
        detail: "session-holding connector does not expose transport/onboarding_poll (cannot be pairing-truth-probed)"
      });
    }
    if (plugin.supportsTool("transport/onboarding_poll")) {
      const isPairing = v.manifest.onboarding != null;
      report.structural.push(await onboardingHandshakeProbe(plugin, { requirePairingTruth: isPairing }));
      if (isPairing) {
        report.structural.push(
          await pairingAdversarialProbe({ folderAbs: launch.folderAbs, mcp: launch.mcp })
        );
      }
    }
    await runRenderProver(plugin, v.manifest, report);
    await runMediaProver(plugin, v.manifest, report);
    for (const flag of declaredNonRenderFlags(v.manifest)) {
      report.declaredUnproven.push({ flag, reason: "no probe (lifecycle=Pass B)", clearAtLivePair: true });
    }
    const healthProbe = await upstreamHealthProbe(plugin, v.manifest);
    if (healthProbe) report.structural.push(healthProbe);
    const inboundProbe = await inboundDeliveryProbe(plugin);
    if (inboundProbe) report.structural.push(inboundProbe);
    const temporalProbe = await inboundTemporalProbe(plugin, received);
    if (temporalProbe) report.structural.push(temporalProbe);
    report.structural.push(await inboundHonestDegradeProbe(plugin));
    report.certified = report.structural.every((s) => s.passed) && report.failures.length === 0;
  } catch (e) {
    report.structural.push({ name: "spawn", passed: false, detail: e.message });
    report.certified = false;
  } finally {
    await plugin.disconnect().catch(() => {
    });
  }
  if (v.manifest.session_held !== false) {
    report.livePairing = "declared-unproven";
  }
  return report;
}

// ../core/src/transports/conformance/types.ts
var CONFORMANCE_FILENAME = ".conformance.json";

// src/cli.ts
async function certifyFolder(folderArg) {
  if (!folderArg)
    return {
      exitCode: 2,
      error: "usage: my-agent-transport-kit certify <connector-folder>"
    };
  const folderAbs = resolve(folderArg);
  const capPath = join6(folderAbs, "CAPABILITY.md");
  const mcpPath = join6(folderAbs, ".mcp.json");
  if (!existsSync3(capPath) || !existsSync3(mcpPath)) {
    return {
      exitCode: 2,
      error: `folder missing CAPABILITY.md or .mcp.json: ${folderAbs}`
    };
  }
  const { mcp, requiredEnv } = deriveConnectorLaunch(folderAbs);
  const setup = await runConnectorSetup(folderAbs);
  if (!setup.ready)
    return {
      exitCode: 1,
      error: `setup failed: ${setup.error ?? "deps did not resolve"}`
    };
  const cursorDir = mkdtempSync2(join6(tmpdir2(), "tk-certify-"));
  try {
    const report = await runTransportConformance({
      id: basename(folderAbs),
      folderAbs,
      mcp,
      requiredEnv,
      cursorDir
    });
    if (!report.ranAt) report.ranAt = (/* @__PURE__ */ new Date()).toISOString();
    try {
      writeFileSync3(join6(folderAbs, CONFORMANCE_FILENAME), JSON.stringify(report, null, 2) + "\n");
    } catch (err) {
      return {
        exitCode: 1,
        report,
        error: `conformance ran (certified=${report.certified}) but persisting ${CONFORMANCE_FILENAME} failed: ${err instanceof Error ? err.message : String(err)}`
      };
    }
    return { exitCode: report.certified ? 0 : 1, report };
  } finally {
    rmSync(cursorDir, { recursive: true, force: true });
  }
}
function printReport(r) {
  if (r.error) console.error(r.error);
  if (r.report) {
    const isPairing = r.report.livePairing === "declared-unproven";
    const verdict = r.report.certified ? isPairing ? "STRUCTURALLY CERTIFIED" : "CERTIFIED" : "FAILED";
    const livePairingTag = isPairing ? " [live-pairing: unproven]" : "";
    console.error(
      `conformance: ${verdict}${livePairingTag} (protocolVersion=${r.report.negotiatedProtocolVersion ?? "none"})`
    );
    for (const s of r.report.structural)
      console.error(
        `  [${s.passed ? "PASS" : "FAIL"}] ${s.name}${s.detail ? ` \u2014 ${s.detail}` : ""}`
      );
    if (r.report.provenFlags.length) {
      console.error(`  proven render flags: ${r.report.provenFlags.join(", ")}`);
    }
    for (const f of r.report.failures) {
      console.error(`  [FAIL] render:${f.flag} \u2014 ${f.disproof}`);
    }
    if (r.report.declaredUnproven.length) {
      console.error(
        `  declared-unproven (live-pair): ${r.report.declaredUnproven.map((d) => d.flag).join(", ")}`
      );
    }
    if (isPairing) {
      console.error("  live-pairing: unproven (requires live-pair)");
    }
  }
}
var USAGE = `my-agent-transport-kit \u2014 certify a my-agent transport connector

Usage:
  my-agent-transport-kit certify <connector-folder>   Certify a self-contained connector folder
  my-agent-transport-kit --help                        Show this help

The folder must contain CAPABILITY.md and .mcp.json. Folder-local dependencies are installed
automatically before the conformance run. The verdict is written to <folder>/.conformance.json.

Exit codes: 0 certified \xB7 1 setup failed or not certified \xB7 2 usage / malformed folder.`;
async function main(argv) {
  const args = argv.slice(2);
  if (args.length === 0 || args[0] === "--help" || args[0] === "-h") {
    console.error(USAGE);
    return args.length === 0 ? 2 : 0;
  }
  const folderArg = args[0] === "certify" ? args[1] : args[0];
  const r = await certifyFolder(folderArg ?? "");
  printReport(r);
  return r.exitCode;
}
void main(process.argv).then((code) => process.exit(code));
