/*
 * Сервер для совместной игры: раздаёт игру (public/) и синхронизирует фишки по WebSocket.
 * Без зависимостей: нужен только Node.js 16+ (проверено на 22).
 *
 *   node server.js
 *
 * Переменные окружения:
 *   PORT        порт (по умолчанию 3000)
 *   HOST        адрес прослушивания (по умолчанию 0.0.0.0)
 *   DATA_DIR    куда сохранять состояние (по умолчанию ./data)
 *   ADMIN_KEY   если задан — работает сброс:  /reset?key=ADMIN_KEY
 */
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "3000", 10);
const HOST = process.env.HOST || "0.0.0.0";
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const STATE_FILE = path.join(DATA_DIR, "state.json");
const ADMIN_KEY = process.env.ADMIN_KEY || "";

const MAX_MSG_BYTES = 256 * 1024;   // максимальный размер одного сообщения
const MAX_CUSTOMS = 200;            // максимум своих фишек на всех
const MAX_MSGS_PER_SEC = 120;       // защита от флуда (на одного клиента)
const CHAR_KEYS = new Set(["ch:t5", "ch:t7", "ch:t6", "ch:t189"]);

/* ======================= состояние ======================= */
let state = { initialized: false, chars: {}, customs: {} };

function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (s && typeof s === "object") {
      state = {
        initialized: !!s.initialized,
        chars: s.chars && typeof s.chars === "object" ? s.chars : {},
        customs: s.customs && typeof s.customs === "object" ? s.customs : {},
      };
      console.log("Состояние загружено:", Object.keys(state.chars).length, "персонажей,",
        Object.keys(state.customs).length, "своих фишек");
    }
  } catch (e) { /* файла нет — начинаем с чистого листа */ }
}

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const tmp = STATE_FILE + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(state));
      fs.renameSync(tmp, STATE_FILE);
    } catch (e) { console.error("Не удалось сохранить состояние:", e.message); }
  }, 1000);
}
function flushSave() {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));
  } catch (e) {}
}

/* ======================= проверка данных ======================= */
const isNum = (v) => typeof v === "number" && Number.isFinite(v) && Math.abs(v) < 1e6;
const cleanStr = (v, n) => String(v == null ? "" : v).slice(0, n);

function cleanPos(it) {
  if (!it || typeof it !== "object") return null;
  if (!isNum(it.x) || !isNum(it.y)) return null;
  return { x: Math.round(it.x * 10) / 10, y: Math.round(it.y * 10) / 10, l: cleanStr(it.l, 40) };
}
function cleanCustom(c) {
  if (!c || typeof c !== "object") return null;
  if (!Number.isSafeInteger(c.id) || c.id < 0) return null;
  if (!isNum(c.x) || !isNum(c.y)) return null;
  const name = cleanStr(c.name, 24).trim();
  if (!name) return null;
  return {
    id: c.id, name,
    color: /^#[0-9a-fA-F]{6}$/.test(c.color) ? c.color : "#9aa5b1",
    layer: cleanStr(c.layer, 40),
    x: Math.round(c.x * 10) / 10, y: Math.round(c.y * 10) / 10,
  };
}

/* ======================= минимальный WebSocket (RFC 6455) ======================= */
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const clients = new Set();

function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | opcode;
  return Buffer.concat([head, payload]);
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragSize = 0;
    this.alive = true;
    this.lastPong = Date.now();
    this.winStart = Date.now();
    this.winCount = 0;
    this.closed = false;
    socket.setNoDelay(true);
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }
  send(obj) {
    if (this.closed) return;
    try { this.socket.write(frame(1, Buffer.from(JSON.stringify(obj), "utf8"))); } catch (e) {}
  }
  close(code) {
    if (this.closed) return;
    try {
      const p = Buffer.alloc(2); p.writeUInt16BE(code || 1000, 0);
      this.socket.write(frame(8, p));
    } catch (e) {}
    try { this.socket.end(); } catch (e) {}
    setTimeout(() => { try { this.socket.destroy(); } catch (e) {} }, 500);
    this.onClose();
  }
  onClose() {
    if (this.closed) return;
    this.closed = true;
    clients.delete(this);
    broadcastOnline();
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_MSG_BYTES * 2) return this.close(1009);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0;
      const opcode = b[0] & 0x0f;
      const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (b.length < 10) return;
        const big = b.readBigUInt64BE(2);
        if (big > BigInt(MAX_MSG_BYTES)) return this.close(1009);
        len = Number(big); off = 10;
      }
      if (!masked) return this.close(1002);          // клиент обязан маскировать
      if (len > MAX_MSG_BYTES) return this.close(1009);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const data = Buffer.alloc(len);
      for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
      this.buf = b.subarray(off + 4 + len);

      if (opcode === 8) return this.close(1000);
      if (opcode === 9) { try { this.socket.write(frame(10, data)); } catch (e) {} continue; }
      if (opcode === 10) { this.lastPong = Date.now(); continue; }
      if (opcode === 0 || opcode === 1 || opcode === 2) {
        if (opcode !== 0) { this.frags = []; this.fragSize = 0; this.fragOp = opcode; }
        this.frags.push(data); this.fragSize += len;
        if (this.fragSize > MAX_MSG_BYTES) return this.close(1009);
        if (fin) {
          const full = Buffer.concat(this.frags);
          this.frags = []; this.fragSize = 0;
          if (this.fragOp === 1) this.onText(full.toString("utf8"));
        }
      }
    }
  }
  onText(text) {
    const now = Date.now();
    if (now - this.winStart > 1000) { this.winStart = now; this.winCount = 0; }
    if (++this.winCount > MAX_MSGS_PER_SEC) return;       // слишком часто — игнорируем
    let m;
    try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    handleMessage(this, m);
  }
}

function broadcast(obj, except) {
  const payload = frame(1, Buffer.from(JSON.stringify(obj), "utf8"));
  for (const c of clients) {
    if (c === except || c.closed) continue;
    try { c.socket.write(payload); } catch (e) {}
  }
}
function broadcastOnline() { broadcast({ t: "online", n: clients.size }); }

function snapshot() {
  return { t: "snap", init: state.initialized, chars: state.chars,
           customs: Object.values(state.customs), n: clients.size };
}

/* ======================= логика сообщений ======================= */
function handleMessage(c, m) {
  switch (m.t) {
    case "init": {
      // Первый подключившийся «засевает» мир своими позициями. Дальше мир ведёт сервер.
      if (state.initialized) { c.send(snapshot()); return; }
      const chars = {};
      if (m.chars && typeof m.chars === "object") {
        for (const k of Object.keys(m.chars)) {
          if (!CHAR_KEYS.has(k)) continue;
          const p = cleanPos(m.chars[k]); if (p) chars[k] = p;
        }
      }
      const customs = {};
      if (Array.isArray(m.customs)) {
        for (const raw of m.customs.slice(0, MAX_CUSTOMS)) {
          const cc = cleanCustom(raw); if (cc) customs[cc.id] = cc;
        }
      }
      state.chars = chars; state.customs = customs; state.initialized = true;
      scheduleSave();
      broadcast(snapshot(), c);
      return;
    }
    case "pos": {
      if (!Array.isArray(m.items)) return;
      const out = [];
      for (const it of m.items.slice(0, 40)) {
        if (!it || typeof it.k !== "string") continue;
        const p = cleanPos(it); if (!p) continue;
        if (CHAR_KEYS.has(it.k)) {
          state.chars[it.k] = p;
        } else if (/^c:\d+$/.test(it.k)) {
          const cu = state.customs[it.k.slice(2)];
          if (!cu) continue;
          cu.x = p.x; cu.y = p.y; if (p.l) cu.layer = p.l;
        } else continue;
        out.push({ k: it.k, x: p.x, y: p.y, l: p.l });
      }
      if (out.length) { state.initialized = true; scheduleSave(); broadcast({ t: "pos", items: out }, c); }
      return;
    }
    case "cadd": {
      const cc = cleanCustom(m.c);
      if (!cc) return;
      if (!state.customs[cc.id] && Object.keys(state.customs).length >= MAX_CUSTOMS) return;
      state.customs[cc.id] = cc; state.initialized = true;
      scheduleSave();
      broadcast({ t: "cadd", c: cc }, c);
      return;
    }
    case "crem": {
      if (!Number.isSafeInteger(m.id)) return;
      if (state.customs[m.id]) {
        delete state.customs[m.id];
        scheduleSave();
        broadcast({ t: "crem", id: m.id }, c);
      }
      return;
    }
    default: return;
  }
}

/* ======================= HTTP: раздача файлов ======================= */
const MIME = {
  ".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8",
  ".png": "image/png", ".jpg": "image/jpeg", ".webp": "image/webp", ".svg": "image/svg+xml",
  ".ico": "image/x-icon", ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".txt": "text/plain; charset=utf-8",
};
const fileCache = new Map();   // путь -> { sig, raw, gz, etag }

function getFile(fp) {
  const st = fs.statSync(fp);
  const sig = st.mtimeMs + ":" + st.size;
  const hit = fileCache.get(fp);
  if (hit && hit.sig === sig) return hit;
  const raw = fs.readFileSync(fp);
  const ext = path.extname(fp).toLowerCase();
  const compressible = [".html", ".js", ".css", ".json", ".svg", ".txt"].includes(ext);
  const entry = {
    sig, raw, ext,
    gz: compressible ? zlib.gzipSync(raw, { level: 9 }) : null,
    etag: '"' + crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20) + '"',
  };
  fileCache.set(fp, entry);
  return entry;
}

function serveStatic(req, res, urlPath) {
  let rel = decodeURIComponent(urlPath);
  if (rel === "/" || rel === "") rel = "/index.html";
  const fp = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!fp.startsWith(PUBLIC_DIR + path.sep) && fp !== PUBLIC_DIR) { res.writeHead(403); return res.end("Forbidden"); }
  let f;
  try {
    if (!fs.statSync(fp).isFile()) throw new Error("not file");
    f = getFile(fp);
  } catch (e) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("Не найдено"); }

  const headers = {
    "Content-Type": MIME[f.ext] || "application/octet-stream",
    "ETag": f.etag,
    "Cache-Control": "no-cache",       // браузер каждый раз спрашивает «не изменилось?» (304) — быстро
    "Vary": "Accept-Encoding",
  };
  if (req.headers["if-none-match"] === f.etag) { res.writeHead(304, headers); return res.end(); }
  const useGz = f.gz && /\bgzip\b/.test(req.headers["accept-encoding"] || "");
  const body = useGz ? f.gz : f.raw;
  if (useGz) headers["Content-Encoding"] = "gzip";
  headers["Content-Length"] = body.length;
  res.writeHead(200, headers);
  if (req.method === "HEAD") return res.end();
  res.end(body);
}

const server = http.createServer((req, res) => {
  let u;
  try { u = new URL(req.url, "http://x"); } catch (e) { res.writeHead(400); return res.end(); }

  if (u.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true, online: clients.size }));
  }
  if (u.pathname === "/state") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify(snapshot(), null, 2));
  }
  if (u.pathname === "/reset") {
    if (!ADMIN_KEY || u.searchParams.get("key") !== ADMIN_KEY) { res.writeHead(403); return res.end("Нужен верный ADMIN_KEY"); }
    state = { initialized: false, chars: {}, customs: {} };
    flushSave();
    // все подключённые клиенты получают «пустой» мир и перезасеивают его своими позициями
    broadcast(snapshot());
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("Состояние сброшено. Первый, кто пошевелит фишкой/обновит страницу, задаст новые позиции.");
  }
  if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405); return res.end(); }
  try { serveStatic(req, res, u.pathname); }
  catch (e) { res.writeHead(500); res.end("Ошибка сервера"); }
});

server.on("upgrade", (req, socket) => {
  let u;
  try { u = new URL(req.url, "http://x"); } catch (e) { return socket.destroy(); }
  const key = req.headers["sec-websocket-key"];
  if (u.pathname !== "/ws" || !key || String(req.headers.upgrade).toLowerCase() !== "websocket") {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n"); return socket.destroy();
  }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
               "Sec-WebSocket-Accept: " + accept + "\r\n\r\n");
  const c = new Client(socket);
  clients.add(c);
  c.send(snapshot());
  broadcastOnline();
});

/* пинги: убираем «мёртвые» соединения */
setInterval(() => {
  const now = Date.now();
  for (const c of clients) {
    if (now - c.lastPong > 70000) { c.close(1001); continue; }
    try { c.socket.write(frame(9, Buffer.alloc(0))); } catch (e) {}
  }
}, 25000).unref();

loadState();
server.listen(PORT, HOST, () => {
  console.log("Игра запущена:  http://localhost:" + PORT);
  const nets = require("os").networkInterfaces();
  for (const name of Object.keys(nets)) for (const n of nets[name] || [])
    if (n.family === "IPv4" && !n.internal) console.log("Для других в вашей сети:  http://" + n.address + ":" + PORT);
});

function shutdown() { flushSave(); process.exit(0); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
