// 의존성 없는 실시간 채팅 서버 (Node 내장 http + 롱폴링)
// SSE/WebSocket은 터널·보안 프록시가 스트림을 버퍼링하면 멈추므로,
// 짧은 요청을 반복하는 롱폴링으로 어떤 네트워크에서도 동작하게 함.
// 실행: node server.js   (포트 변경: PORT=4000 node server.js)
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT) || 3100;
const HISTORY_LIMIT = 200;
const MAX_TEXT = 1000;
const MAX_NAME = 20;
const POLL_WAIT = 25000; // 새 소식이 없을 때 응답을 붙잡아 두는 시간
const CLIENT_TIMEOUT = 45000; // 이 시간 동안 폴링이 없으면 퇴장 처리

const INDEX_HTML = fs.readFileSync(path.join(__dirname, "index.html"));

// room -> { seq, history: [{seq, ...msg}], clients: Map<clientId, {name, publicId, lastSeen}>, waiters: Set<fn> }
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, { seq: 0, history: [], clients: new Map(), waiters: new Set() });
  return rooms.get(id);
}

function cleanRoomId(raw) {
  const id = String(raw || "lobby").trim().slice(0, 40);
  return /^[\w가-힣-]+$/.test(id) ? id : "lobby";
}

function cleanName(raw) {
  return String(raw || "").replace(/\s+/g, " ").trim().slice(0, MAX_NAME);
}

function presence(room) {
  return [...room.clients.values()].map((c) => c.name);
}

function wake(room) {
  const list = [...room.waiters];
  room.waiters.clear();
  list.forEach((fn) => fn());
}

function pushMessage(room, msg) {
  room.seq += 1;
  room.history.push({ seq: room.seq, ...msg });
  if (room.history.length > HISTORY_LIMIT) room.history.shift();
  wake(room);
}

function removeClient(room, clientId) {
  const c = room.clients.get(clientId);
  if (!c) return;
  room.clients.delete(clientId);
  rate.delete(clientId);
  pushMessage(room, { type: "system", text: `${c.name}님이 나갔어요`, at: Date.now() });
}

// 간단한 도배 방지: 클라이언트당 5초에 10개
const rate = new Map();
function allowed(clientId) {
  const now = Date.now();
  const list = (rate.get(clientId) || []).filter((t) => now - t < 5000);
  if (list.length >= 10) return false;
  list.push(now);
  rate.set(clientId, list);
  return true;
}

function readBody(req, limit = 8 * 1024) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > limit) {
        reject(new Error("too large"));
        req.destroy();
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function json(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(data));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");

  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    return res.end(INDEX_HTML);
  }

  // 입장
  if (req.method === "POST" && url.pathname === "/join") {
    try {
      const body = JSON.parse(await readBody(req));
      const name = cleanName(body.name);
      if (!name) return json(res, 400, { error: "name required" });
      const room = getRoom(cleanRoomId(body.room));
      const clientId = crypto.randomUUID(); // 전송 권한용(본인만 앎)
      const publicId = crypto.randomUUID().slice(0, 8); // 메시지에 공개되는 발신자 id
      room.clients.set(clientId, { name, publicId, lastSeen: Date.now() });
      pushMessage(room, { type: "system", text: `${name}님이 들어왔어요`, at: Date.now() });
      return json(res, 200, { clientId, publicId, seq: room.seq, history: room.history, presence: presence(room) });
    } catch {
      return json(res, 400, { error: "bad request" });
    }
  }

  // 새 소식 받기: since 이후 메시지가 생기거나 POLL_WAIT가 지나면 응답
  if (req.method === "GET" && url.pathname === "/poll") {
    const room = rooms.get(cleanRoomId(url.searchParams.get("room")));
    const clientId = url.searchParams.get("clientId");
    const client = room && room.clients.get(clientId);
    if (!client) return json(res, 403, { error: "not joined" });
    const since = Number(url.searchParams.get("since")) || 0;

    let done = false;
    const reply = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      room.waiters.delete(reply);
      client.lastSeen = Date.now();
      json(res, 200, {
        seq: room.seq,
        events: room.history.filter((m) => m.seq > since),
        presence: presence(room),
      });
    };
    const timer = setTimeout(reply, POLL_WAIT);
    req.on("close", () => {
      done = true;
      clearTimeout(timer);
      room.waiters.delete(reply);
    });
    client.lastSeen = Date.now();
    if (room.seq > since) return reply();
    room.waiters.add(reply);
    return;
  }

  // 메시지 전송
  if (req.method === "POST" && url.pathname === "/send") {
    try {
      const { room: roomId, clientId, text } = JSON.parse(await readBody(req));
      const room = rooms.get(cleanRoomId(roomId));
      const client = room && room.clients.get(clientId);
      const clean = String(text || "").trim().slice(0, MAX_TEXT);
      if (!client) return json(res, 403, { error: "not joined" });
      if (!clean) return json(res, 400, { error: "empty" });
      if (!allowed(clientId)) return json(res, 429, { error: "slow down" });
      client.lastSeen = Date.now();
      pushMessage(room, {
        type: "chat",
        name: client.name,
        from: client.publicId,
        text: clean,
        at: Date.now(),
      });
      return json(res, 200, { ok: true });
    } catch {
      return json(res, 400, { error: "bad request" });
    }
  }

  // 퇴장 (창 닫을 때 sendBeacon)
  if (req.method === "POST" && url.pathname === "/leave") {
    try {
      const { room: roomId, clientId } = JSON.parse(await readBody(req));
      const room = rooms.get(cleanRoomId(roomId));
      if (room) removeClient(room, clientId);
    } catch {}
    res.writeHead(204);
    return res.end();
  }

  res.writeHead(404);
  res.end("not found");
});

// 창을 닫았는데 퇴장 신호가 못 온 사람 정리
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    for (const [id, c] of room.clients) {
      if (now - c.lastSeen > CLIENT_TIMEOUT) removeClient(room, id);
    }
  }
}, 10000);

server.listen(PORT, () => {
  console.log(`채팅 서버 실행 중: http://localhost:${PORT}`);
});
