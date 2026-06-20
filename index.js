const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

let queue = [];
let pair = new Map();

function removeFromQueue(id) {
  queue = queue.filter(x => x !== id);
}

function unpair(id) {
  const partner = pair.get(id);
  if (!partner) return;

  pair.delete(id);
  pair.delete(partner);

  io.to(partner).emit("partner_left");
  io.to(id).emit("partner_left");
}

function matchUsers() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();

    if (!a || !b) return;

    const room = a + "#" + b;

    pair.set(a, b);
    pair.set(b, a);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
  }
}

io.on("connection", (socket) => {
  console.log("CONNECTED", socket.id);

  socket.on("find", () => {
    console.log("FIND", socket.id);

    removeFromQueue(socket.id);
    unpair(socket.id);

    queue.push(socket.id);

    matchUsers();
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("next", () => {
    unpair(socket.id);
    removeFromQueue(socket.id);

    queue.push(socket.id);
    matchUsers();
  });

  socket.on("disconnect", () => {
    unpair(socket.id);
    removeFromQueue(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("SERVER RUNNING ON", PORT);
});