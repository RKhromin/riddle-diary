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

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
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
    room = { peers: new Map(), adminKey: "", cfg: { life: 8, title: "" }, timer: null };
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
    } else if (m.t === "pr" && m.p && typeof m.p === "object") {
      const ok = (v) => v === null || (typeof v === "number" && v >= 0 && v <= 1);
      if (!ok(m.p.cx) || !ok(m.p.cy)) return;
      me.presence = { cx: m.p.cx, cy: m.p.cy };
      broadcast(room, { t: "pres", peer: id, presence: me.presence }, me);
    } else if (m.t === "cfg" && me.admin) {
      const life = clamp(Number(m.life) || 8, 2, 30);
      const title = typeof m.title === "string" ? m.title.replace(/[\u0000-\u001f\u007f]/g, "").slice(0, 60) : "";
      room.cfg = { life, title };
      broadcast(room, { t: "cfg", life, title }, me);
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
