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

let queue = [];
let pair = new Map();

function remove(id) {
  queue = queue.filter(x => x !== id);
}

function unpair(id) {
  const p = pair.get(id);
  if (!p) return;

  pair.delete(p);
  pair.delete(id);

  io.to(p).emit("partner_left");
  io.to(id).emit("partner_left");
}

function match() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();

    if (!a || !b) return;

    const room = `${a}#${b}`;

    pair.set(a, b);
    pair.set(b, a);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
  }
}

io.on("connection", (socket) => {
  console.log("connected", socket.id);

  socket.on("find", () => {
    remove(socket.id);
    unpair(socket.id);

    if (!queue.includes(socket.id)) {
      queue.push(socket.id);
    }

    match();
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("next", () => {
    unpair(socket.id);
    remove(socket.id);

    queue.push(socket.id);
    match();
  });

  socket.on("disconnect", () => {
    unpair(socket.id);
    remove(socket.id);
  });
});

server.listen(3000, () => {
  console.log("server running");
});