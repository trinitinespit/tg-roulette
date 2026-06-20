const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

// очередь
let queue = [];

// пары
let pairs = new Map();

// heartbeat
let alive = new Map();

function removeFromQueue(id) {
  queue = queue.filter(s => s !== id);
}

function cleanup(id) {
  removeFromQueue(id);
  alive.delete(id);

  const partner = pairs.get(id);

  if (partner) {
    pairs.delete(partner);
    pairs.delete(id);

    io.to(partner).emit("partner_disconnected");
    io.to(id).emit("partner_disconnected");
  }
}

// матчинг
function matchUsers() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();

    if (!a || !b) return;

    const room = a + "#" + b;

    pairs.set(a, b);
    pairs.set(b, a);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
  }
}

io.on("connection", (socket) => {
  console.log("connected", socket.id);

  alive.set(socket.id, Date.now());

  queue.push(socket.id);
  matchUsers();

  // heartbeat
  socket.on("ping_alive", () => {
    alive.set(socket.id, Date.now());
  });

  // сигнал WebRTC
  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  // next
  socket.on("next", () => {
    cleanup(socket.id);

    queue.push(socket.id);
    matchUsers();
  });

  socket.on("disconnect", () => {
    cleanup(socket.id);
  });
});

// чистка мёртвых сокетов
setInterval(() => {
  const now = Date.now();

  for (let [id, last] of alive.entries()) {
    if (now - last > 15000) {
      cleanup(id);
    }
  }
}, 10000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("Server running on", PORT);
});