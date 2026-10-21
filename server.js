// 의존성 없는 실시간 채팅 서버 (Node 내장 http + 롱폴링)
// SSE/WebSocket은 터널·보안 프록시가 스트림을 버퍼링하면 멈추므로,
// 짧은 요청을 반복하는 롱폴링으로 어떤 네트워크에서도 동작하게 함.
// 실행: node server.js   (포트 변경: PORT=4000 node server.js)
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT) || 3100;
const HISTORY_LIMIT = 200; // 방마다 보관하는 메시지 수
const LOG_LIMIT = 1000; // 폴링으로 전달하는 변경 기록 수
const IMAGE_LIMIT = 30; // 방마다 보관하는 사진 수 (넘으면 오래된 것부터 만료)
const MAX_IMAGE_BYTES = 1.5 * 1024 * 1024;
const MAX_ROOMS = 100;
const MAX_TEXT = 1000;
const MAX_NAME = 20;
const POLL_WAIT = 25000; // 새 소식이 없을 때 응답을 붙잡아 두는 시간
const CLIENT_TIMEOUT = 45000; // 이 시간 동안 폴링이 없으면 퇴장 처리
const LEAVE_GRACE = 8000; // 새로고침처럼 곧바로 돌아오면 입장/퇴장 안내를 생략
const ROOM_CLEAR_AFTER = 60000; // 방이 비고 이 시간이 지나면 대화 기록 삭제
const REACTIONS = ["👍", "❤️", "😂", "😮", "😢", "🙏"];

const INDEX_HTML = fs.readFileSync(path.join(__dirname, "index.html"));

/*
 room = {
   id, seq, log: [{seq, kind, ...}], history: [msg],
   clients: Map<clientId, {publicId, name, lastSeen}>,
   reads: Map<publicId, lastReadSeq>,
   pendingLeave: Map<publicId, {name, timer}>,
   imageOrder: [token], waiters: Set<fn>, emptySince
 }
 msg = {id, type: "chat"|"image"|"system", name, from, text, image, expired, at, reactions: {emoji: [publicId]}}
*/
const rooms = new Map();
const images = new Map(); // token -> {buf, type, roomId, msgId}

function cleanRoomId(raw) {
  const id = String(raw || "lobby").trim().slice(0, 40);
  return /^[\w가-힣-]+$/.test(id) ? id : "lobby";
}

function cleanName(raw) {
  return String(raw || "").replace(/\s+/g, " ").trim().slice(0, MAX_NAME);
}

function getRoom(id, create) {
  if (!rooms.has(id) && create) {
    if (rooms.size >= MAX_ROOMS) return null;
    rooms.set(id, {
      id,
      seq: 0,
      log: [],
      history: [],
      clients: new Map(),
      reads: new Map(),
      pendingLeave: new Map(),
      imageOrder: [],
      waiters: new Set(),
      emptySince: null,
    });
  }
  return rooms.get(id);
}

function deleteRoom(room) {
  for (const token of room.imageOrder) images.delete(token);
  for (const p of room.pendingLeave.values()) clearTimeout(p.timer);
  for (const fn of room.waiters) fn();
  rooms.delete(room.id);
}

function wake(room) {
  const list = [...room.waiters];
  room.waiters.clear();
  list.forEach((fn) => fn());
}

function pushLog(room, entry) {
  room.seq += 1;
  room.log.push({ seq: room.seq, ...entry });
  if (room.log.length > LOG_LIMIT) room.log.shift();
  wake(room);
  return room.seq;
}

function addMessage(room, msg) {
  const id = room.seq + 1;
  const full = { id, reactions: {}, ...msg };
  room.history.push(full);
  if (room.history.length > HISTORY_LIMIT) room.history.shift();
  pushLog(room, { kind: "msg", msg: full });
  return full;
}

function system(room, text) {
  addMessage(room, { type: "system", text, at: Date.now() });
}

// 접속자 목록: 같은 사람(publicId)의 여러 연결은 하나로 묶음
function presence(room) {
  const seen = new Map();
  for (const c of room.clients.values()) {
    if (!seen.has(c.publicId)) {
      seen.set(c.publicId, { publicId: c.publicId, name: c.name, lastRead: room.reads.get(c.publicId) || 0 });
    }
  }
  return [...seen.values()];
}

function isPresent(room, publicId) {
  for (const c of room.clients.values()) if (c.publicId === publicId) return true;
  return false;
}

function removeClient(room, clientId) {
  const c = room.clients.get(clientId);
  if (!c) return;
  room.clients.delete(clientId);
  rate.delete(clientId);
  pushLog(room, { kind: "tick" });
  if (room.clients.size === 0) room.emptySince = Date.now();
  if (isPresent(room, c.publicId) || room.pendingLeave.has(c.publicId)) return;
  // 새로고침이면 곧 다시 들어오므로 퇴장 안내를 잠시 미룸
  const timer = setTimeout(() => {
    room.pendingLeave.delete(c.publicId);
    if (rooms.get(room.id) === room && !isPresent(room, c.publicId)) system(room, `${c.name}님이 나갔어요`);
  }, LEAVE_GRACE);
  room.pendingLeave.set(c.publicId, { name: c.name, timer });
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
    const chunks = [];
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new Error("too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function json(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(data));
}

// data URL을 디코딩하고 실제 파일 시그니처로 이미지인지 확인
function decodeImage(dataUrl) {
  const m = /^data:image\/(jpeg|png|webp|gif);base64,([A-Za-z0-9+/=]+)$/.exec(String(dataUrl || ""));
  if (!m) return null;
  const buf = Buffer.from(m[2], "base64");
  if (!buf.length || buf.length > MAX_IMAGE_BYTES) return null;
  const sig = buf.subarray(0, 12);
  const ok =
    (sig[0] === 0xff && sig[1] === 0xd8 && sig[2] === 0xff) || // jpeg
    sig.subarray(0, 4).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47])) || // png
    (sig.subarray(0, 4).toString() === "RIFF" && sig.subarray(8, 12).toString() === "WEBP") ||
    sig.subarray(0, 4).toString() === "GIF8";
  if (!ok) return null;
  return { buf, type: `image/${m[1]}` };
}

function storeImage(room, msg, img) {
  const token = crypto.randomBytes(16).toString("hex");
  images.set(token, { buf: img.buf, type: img.type, roomId: room.id });
  room.imageOrder.push(token);
  if (room.imageOrder.length > IMAGE_LIMIT) {
    const old = room.imageOrder.shift();
    const oldMsg = room.history.find((m) => m.image === `/img/${old}`);
    images.delete(old);
    if (oldMsg) {
      oldMsg.expired = true;
      pushLog(room, { kind: "expire", id: oldMsg.id });
    }
  }
  return `/img/${token}`;
}

// 요청 본문의 room/clientId로 접속 중인 클라이언트를 찾음
function auth(body) {
  const room = rooms.get(cleanRoomId(body.room));
  const client = room && room.clients.get(body.clientId);
  if (client) client.lastSeen = Date.now();
  return { room, client };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");

  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    return res.end(INDEX_HTML);
  }

  if (req.method === "GET" && url.pathname.startsWith("/img/")) {
    const img = images.get(url.pathname.slice(5));
    if (!img) {
      res.writeHead(404);
      return res.end();
    }
    res.writeHead(200, {
      "Content-Type": img.type,
      "Cache-Control": "private, max-age=86400",
      "X-Content-Type-Options": "nosniff",
    });
    return res.end(img.buf);
  }

  // 입장. deviceKey는 브라우저 탭마다 고정이라 새로고침해도 같은 사람으로 인식됨
  if (req.method === "POST" && url.pathname === "/join") {
    try {
      const body = JSON.parse(await readBody(req));
      const name = cleanName(body.name);
      const deviceKey = String(body.deviceKey || "").slice(0, 64);
      if (!name || deviceKey.length < 16) return json(res, 400, { error: "bad request" });
      const room = getRoom(cleanRoomId(body.room), true);
      if (!room) return json(res, 503, { error: "too many rooms" });

      const clientId = crypto.randomUUID(); // 요청 권한용(본인만 앎)
      const publicId = crypto.createHash("sha256").update(`${room.id}:${deviceKey}`).digest("hex").slice(0, 12);
      const returning = isPresent(room, publicId) || room.pendingLeave.has(publicId);
      const pending = room.pendingLeave.get(publicId);
      if (pending) {
        clearTimeout(pending.timer);
        room.pendingLeave.delete(publicId);
      }
      room.clients.set(clientId, { publicId, name, lastSeen: Date.now() });
      room.emptySince = null;
      if (!room.reads.has(publicId)) room.reads.set(publicId, room.seq);
      if (returning) pushLog(room, { kind: "tick" });
      else system(room, `${name}님이 들어왔어요`);
      return json(res, 200, { clientId, publicId, seq: room.seq, history: room.history, presence: presence(room) });
    } catch {
      return json(res, 400, { error: "bad request" });
    }
  }

  // 새 소식 받기: since 이후 변경이 생기거나 POLL_WAIT가 지나면 응답
  if (req.method === "GET" && url.pathname === "/poll") {
    const { room, client } = auth({ room: url.searchParams.get("room"), clientId: url.searchParams.get("clientId") });
    if (!client) return json(res, 403, { error: "not joined" });
    const since = Number(url.searchParams.get("since")) || 0;

    let done = false;
    const reply = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      room.waiters.delete(reply);
      client.lastSeen = Date.now();
      const oldest = room.log.length ? room.log[0].seq : room.seq + 1;
      if (since < oldest - 1 || since > room.seq) {
        // 놓친 변경이 너무 많으면 전체를 다시 보냄
        return json(res, 200, { reset: true, seq: room.seq, history: room.history, presence: presence(room) });
      }
      json(res, 200, { seq: room.seq, events: room.log.filter((e) => e.seq > since), presence: presence(room) });
    };
    const timer = setTimeout(reply, POLL_WAIT);
    req.on("close", () => {
      done = true;
      clearTimeout(timer);
      room.waiters.delete(reply);
    });
    if (room.seq !== since) return reply();
    room.waiters.add(reply);
    return;
  }

  // 메시지·사진 전송
  if (req.method === "POST" && url.pathname === "/send") {
    try {
      const body = JSON.parse(await readBody(req, 2.2 * 1024 * 1024));
      const { room, client } = auth(body);
      if (!client) return json(res, 403, { error: "not joined" });
      if (!allowed(body.clientId)) return json(res, 429, { error: "slow down" });
      const base = { name: client.name, from: client.publicId, at: Date.now() };

      if (body.image) {
        const img = decodeImage(body.image);
        if (!img) return json(res, 400, { error: "bad image" });
        const msg = { type: "image", ...base, image: null };
        msg.image = storeImage(room, msg, img);
        addMessage(room, msg);
      } else {
        const text = String(body.text || "").trim().slice(0, MAX_TEXT);
        if (!text) return json(res, 400, { error: "empty" });
        addMessage(room, { type: "chat", ...base, text });
      }
      room.reads.set(client.publicId, room.seq); // 내가 보낸 건 읽은 것으로
      return json(res, 200, { ok: true });
    } catch (e) {
      return json(res, e.message === "too large" ? 413 : 400, { error: "bad request" });
    }
  }

  // 반응 달기/취소
  if (req.method === "POST" && url.pathname === "/react") {
    try {
      const body = JSON.parse(await readBody(req));
      const { room, client } = auth(body);
      if (!client) return json(res, 403, { error: "not joined" });
      if (!REACTIONS.includes(body.emoji)) return json(res, 400, { error: "bad emoji" });
      const msg = room.history.find((m) => m.id === Number(body.id) && m.type !== "system");
      if (!msg) return json(res, 404, { error: "no message" });
      const list = msg.reactions[body.emoji] || [];
      const i = list.indexOf(client.publicId);
      if (i >= 0) list.splice(i, 1);
      else list.push(client.publicId);
      if (list.length) msg.reactions[body.emoji] = list;
      else delete msg.reactions[body.emoji];
      pushLog(room, { kind: "react", id: msg.id, reactions: msg.reactions });
      return json(res, 200, { ok: true });
    } catch {
      return json(res, 400, { error: "bad request" });
    }
  }

  // 읽음 위치 갱신 (안 읽은 사람 수 계산용)
  if (req.method === "POST" && url.pathname === "/read") {
    try {
      const body = JSON.parse(await readBody(req));
      const { room, client } = auth(body);
      if (!client) return json(res, 403, { error: "not joined" });
      const seq = Math.min(Number(body.seq) || 0, room.seq);
      if (seq > (room.reads.get(client.publicId) || 0)) {
        room.reads.set(client.publicId, seq);
        pushLog(room, { kind: "tick" });
      }
      return json(res, 200, { ok: true });
    } catch {
      return json(res, 400, { error: "bad request" });
    }
  }

  // 퇴장 (창 닫을 때 sendBeacon)
  if (req.method === "POST" && url.pathname === "/leave") {
    try {
      const body = JSON.parse(await readBody(req));
      const room = rooms.get(cleanRoomId(body.room));
      if (room) removeClient(room, body.clientId);
    } catch {}
    res.writeHead(204);
    return res.end();
  }

  res.writeHead(404);
  res.end("not found");
});

setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    // 창을 닫았는데 퇴장 신호가 못 온 사람 정리
    for (const [id, c] of room.clients) {
      if (now - c.lastSeen > CLIENT_TIMEOUT) removeClient(room, id);
    }
    // 마지막 사람이 나가고 일정 시간이 지나면 방과 대화 기록 삭제
    if (room.clients.size === 0 && room.emptySince && now - room.emptySince > ROOM_CLEAR_AFTER) deleteRoom(room);
  }
}, 10000);

server.listen(PORT, () => {
  console.log(`채팅 서버 실행 중: http://localhost:${PORT}`);
});
