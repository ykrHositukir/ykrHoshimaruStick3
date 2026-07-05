/**
 * Stick Fight Online - WebSocket relay server
 * Run: npm install && npm start
 * Default port: 8765
 */
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8765;
const MAX_ROOMS = 200;
const ROOM_TTL_MS = 30 * 60 * 1000;

const rooms = new Map(); // code -> room

function genCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? genCode() : code;
}

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function broadcast(room, data, exceptWs) {
  room.members.forEach((m) => {
    if (m.ws !== exceptWs) send(m.ws, data);
  });
}

function roomInfo(room) {
  return {
    code: room.code,
    hostSlot: room.hostSlot,
    members: room.members.map((m) => ({
      slot: m.slot,
      name: m.name,
      ready: m.ready,
      isHost: m.slot === room.hostSlot,
    })),
    playing: room.playing,
  };
}

function cleanupRooms() {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastActive > ROOM_TTL_MS) rooms.delete(code);
  }
}
setInterval(cleanupRooms, 60000);

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Stick Fight Online Server OK\n");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  let member = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "create") {
      if (rooms.size >= MAX_ROOMS) {
        send(ws, { type: "error", message: "サーバーが満員です" });
        return;
      }
      const code = genCode();
      const name = (msg.name || "Host").slice(0, 12);
      member = { ws, slot: 0, name, ready: true, roomCode: code };
      const room = {
        code,
        hostSlot: 0,
        members: [member],
        playing: false,
        lastActive: Date.now(),
      };
      rooms.set(code, room);
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: 0 });
      return;
    }

    if (msg.type === "join") {
      const code = (msg.code || "").toUpperCase().slice(0, 4);
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: "error", message: "ルームが見つかりません" });
        return;
      }
      if (room.playing) {
        send(ws, { type: "error", message: "試合中のルームです" });
        return;
      }
      if (room.members.length >= 4) {
        send(ws, { type: "error", message: "ルームが満員です" });
        return;
      }
      const used = new Set(room.members.map((m) => m.slot));
      let slot = 0;
      while (used.has(slot) && slot < 4) slot++;
      const name = (msg.name || `P${slot + 1}`).slice(0, 12);
      member = { ws, slot, name, ready: false, roomCode: code };
      room.members.push(member);
      room.lastActive = Date.now();
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: slot });
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (!member) return;
    const room = rooms.get(member.roomCode || msg.room);
    if (!room) return;
    room.lastActive = Date.now();

    if (msg.type === "ready") {
      member.ready = !!msg.ready;
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (msg.type === "start") {
      if (member.slot !== room.hostSlot) return;
      const humans = room.members.length;
      if (humans < 2) {
        send(ws, { type: "error", message: "2人以上必要です" });
        return;
      }
      room.playing = true;
      const startMsg = {
        type: "start",
        stageSeed: Date.now(),
        humanSlots: room.members.map((m) => m.slot),
        roomSettings: msg.roomSettings || null,
      };
      room.members.forEach((m) => send(m.ws, startMsg));
      return;
    }

    if (msg.type === "input") {
      if (member.slot !== msg.slot) return;
      const host = room.members.find((m) => m.slot === room.hostSlot);
      if (host) {
        send(host.ws, { type: "input", slot: msg.slot, input: msg.input, seq: msg.seq || 0 });
      }
      return;
    }

    if (msg.type === "state") {
      if (member.slot !== room.hostSlot) return;
      broadcast(room, { type: "state", state: msg.state }, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        slot: member.slot,
        name: member.name,
        text: (msg.text || "").slice(0, 80),
      });
    }
  });

  ws.on("close", () => {
    if (!member) return;
    for (const [code, room] of rooms) {
      const idx = room.members.findIndex((m) => m.ws === ws);
      if (idx === -1) continue;
      const wasHost = member.slot === room.hostSlot;
      room.members.splice(idx, 1);
      if (room.members.length === 0) {
        rooms.delete(code);
      } else {
        if (wasHost) {
          room.hostSlot = room.members[0].slot;
          room.members[0].ready = true;
          room.playing = false;
        }
        broadcast(room, { type: "lobby", ...roomInfo(room) });
        if (room.playing) broadcast(room, { type: "host_left" });
      }
      break;
    }
  });
});

server.listen(PORT, () => {
  console.log(`Stick Fight Online server: ws://localhost:${PORT}`);
});
