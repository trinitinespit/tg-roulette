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

function removeFromQueue(id) {
  queue = queue.filter(sid => sid !== id);
}

function disconnectPair(id) {
  const partner = pairs.get(id);

  if (partner) {
    pairs.delete(partner);
    pairs.delete(id);

    io.to(partner).emit("partner_disconnected");
    io.to(id).emit("partner_disconnected");
  }
}

function tryMatch() {
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
  console.log("connected:", socket.id);

  // авто-очередь при подключении
  queue.push(socket.id);
  tryMatch();

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("next", () => {
    removeFromQueue(socket.id);
    disconnectPair(socket.id);

    queue.push(socket.id);
    tryMatch();
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);
    disconnectPair(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("Server running on", PORT);
});