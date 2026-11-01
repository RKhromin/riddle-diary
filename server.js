"use strict";
/*
  Сервер «Дневника». Он делает две вещи:
  1) отдаёт страницу index.html;
  2) пересылает штрихи, курсоры и настройки между людьми в одной комнате (WebSocket).
  Рисунки нигде не сохраняются — сервер только передаёт их дальше.
*/
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, "index.html");
const MAX_PEERS = 8;                 // максимум людей в одной комнате
const EMPTY_ROOM_TTL = 30 * 60 * 1000; // пустую комнату помним 30 минут
const MAX_BG = 1500000;              // фон-картинка: не больше ~1,5 МБ

// Загрузка фона: только администратор (по секретному ключу), только JPEG.
function handleBg(req, res, u) {
  const reply = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };
  const name = (u.searchParams.get("room") || "").toLowerCase();
  const key = String(req.headers["x-admin-key"] || "").slice(0, 64);
  const room = rooms.get(name);
  if (!room || !room.adminKey || key !== room.adminKey) return reply(403, { error: "forbidden" });
  const now = Date.now();
  if (now - room.lastBg < 2000) return reply(429, { error: "slow down" });
  room.lastBg = now;
  if (u.searchParams.get("clear") === "1") {
    room.bg = null;
    broadcast(room, { t: "bg", ver: 0 });
    return reply(200, { ver: 0 });
  }
  const chunks = [];
  let size = 0, dead = false;
  req.on("data", (c) => {
    if (dead) return;
    size += c.length;
    if (size > MAX_BG) { dead = true; return reply(413, { error: "too large" }); }
    chunks.push(c);
  });
  req.on("end", () => {
    if (dead) return;
    const buf = Buffer.concat(chunks);
    if (buf.length < 4 || buf[0] !== 0xff || buf[1] !== 0xd8 || buf[2] !== 0xff) return reply(415, { error: "jpeg only" });
    const lum = clamp(parseFloat(u.searchParams.get("lum")) || 0.7, 0, 1);
    room.bg = { data: buf, ver: now, lum };
    broadcast(room, { t: "bg", ver: now, lum });
    reply(200, { ver: now, lum });
  });
}

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (req.method === "POST" && url === "/bg") {
    let u;
    try { u = new URL(req.url, "http://localhost"); } catch (e) { res.writeHead(400); return res.end(); }
    return handleBg(req, res, u);
  }
  if (req.method === "GET" && url.startsWith("/bg/")) {
    let name = "";
    try { name = decodeURIComponent(url.slice(4)).toLowerCase(); } catch (e) {}
    const room = rooms.get(name);
    if (!room || !room.bg) { res.writeHead(404); return res.end(); }
    res.writeHead(200, {
      "Content-Type": "image/jpeg",
      "Cache-Control": "public, max-age=86400",
      "X-Content-Type-Options": "nosniff",
    });
    return res.end(room.bg.data);
  }
  if (url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }
  if (url === "/" || url === "/index.html") {
    return fs.readFile(INDEX, (err, data) => {
      if (err) {
        res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
        return res.end("Не найден index.html рядом с server.js");
      }
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache",
        "X-Content-Type-Options": "nosniff",
      });
      res.end(data);
    });
  }
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 16 * 1024 });
const rooms = new Map(); // имя комнаты -> { peers, adminKey, cfg, timer }

const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const broadcast = (room, obj, except) => {
  const s = JSON.stringify(obj);
  for (const p of room.peers.values()) {
    if (p !== except && p.ws.readyState === 1) p.ws.send(s);
  }
};

wss.on("connection", (ws, req) => {
  let u;
  try { u = new URL(req.url, "http://localhost"); } catch (e) { return ws.close(1008, "bad url"); }
  const name = (u.searchParams.get("room") || "").toLowerCase();
  const key = (u.searchParams.get("key") || "").slice(0, 64);
  if (!/^[a-z0-9][a-z0-9-]{3,40}$/.test(name)) return ws.close(1008, "bad room");

  let room = rooms.get(name);
  if (!room) {
    room = { peers: new Map(), adminKey: "", cfg: { life: 8, title: "", fx: 0 }, bg: null, lastBg: 0, timer: null };
    rooms.set(name, room);
  }
  clearTimeout(room.timer);
  if (room.peers.size >= MAX_PEERS) return ws.close(1013, "room full");

  // Администратор — тот, кто пришёл с секретным ключом создателя комнаты.
  if (key && !room.adminKey) room.adminKey = key;
  const admin = !!key && key === room.adminKey;

  const id = crypto.randomBytes(4).toString("hex");
  const me = { ws, id, admin, presence: {}, tokens: 200, last: Date.now() };
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  send(ws, {
    t: "hello",
    peer: id,
    admin,
    cfg: room.cfg,
    bg: room.bg ? { ver: room.bg.ver, lum: room.bg.lum } : null,
    peers: [...room.peers.values()].map((p) => ({ peer: p.id, presence: p.presence })),
  });
  room.peers.set(id, me);
  broadcast(room, { t: "join", peer: id }, me);

  ws.on("message", (raw) => {
    // защита от «заливки» сообщениями: не больше ~100 в секунду
    const now = Date.now();
    me.tokens = Math.min(200, me.tokens + (now - me.last) * 0.1);
    me.last = now;
    if (me.tokens < 1) return;
    me.tokens -= 1;

    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== "object") return;

    if (m.t === "ev" && m.topic === "stroke" && m.data && typeof m.data === "object") {
      if (JSON.stringify(m.data).length > 4096) return;
      broadcast(room, { t: "ev", topic: "stroke", data: m.data, peer: id }, me);
    } else if (m.t === "ev" && m.topic === "msg" && me.admin && m.data && typeof m.data === "object") {
      // сообщение с клавиатуры: только от администратора, нигде не хранится
      const text = typeof m.data.t === "string" ? m.data.t.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 200) : "";
      if (!text.trim()) return;
      broadcast(room, { t: "ev", topic: "msg", data: { t: text, l: clamp(Number(m.data.l) || 8, 2, 30), f: clamp(Math.round(Number(m.data.f)) || 0, 0, 9) }, peer: id }, me);
    } else if (m.t === "pr" && m.p && typeof m.p === "object") {
      const ok = (v) => v === null || (typeof v === "number" && v >= 0 && v <= 1);
      if (!ok(m.p.cx) || !ok(m.p.cy)) return;
      me.presence = { cx: m.p.cx, cy: m.p.cy };
      broadcast(room, { t: "pres", peer: id, presence: me.presence }, me);
    } else if (m.t === "cfg" && me.admin) {
      const life = clamp(Number(m.life) || 8, 2, 30);
      const title = typeof m.title === "string" ? m.title.replace(/[\u0000-\u001f\u007f]/g, "").slice(0, 60) : "";
      const fx = clamp(Math.round(Number(m.fx)) || 0, 0, 9); // вариант исчезновения чернил
      room.cfg = { life, title, fx };
      broadcast(room, { t: "cfg", life, title, fx }, me);
    } else if (m.t === "ping") {
      send(ws, { t: "pong" });
    }
  });

  ws.on("close", () => {
    room.peers.delete(id);
    broadcast(room, { t: "left", peer: id });
    if (room.peers.size === 0) {
      room.timer = setTimeout(() => { if (room.peers.size === 0) rooms.delete(name); }, EMPTY_ROOM_TTL);
    }
  });
  ws.on("error", () => {});
});

// раз в 30 секунд проверяем, что соединения живы
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log("Дневник запущен на порту " + PORT));
