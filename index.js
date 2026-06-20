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
let pairs = new Map();

function match() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();

    if (!a || !b) return;

    const room = `${a}#${b}`;

    pairs.set(a, b);
    pairs.set(b, a);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
  }
}

function remove(id) {
  queue = queue.filter(x => x !== id);
}

io.on("connection", (socket) => {
  console.log("connect", socket.id);

  socket.on("find", () => {
    remove(socket.id);

    if (pairs.has(socket.id)) return;

    queue.push(socket.id);
    match();
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("next", () => {
    const partner = pairs.get(socket.id);

    if (partner) {
      pairs.delete(partner);
      pairs.delete(socket.id);

      io.to(partner).emit("partner_disconnected");
      io.to(socket.id).emit("partner_disconnected");
    }

    remove(socket.id);
    queue.push(socket.id);
    match();
  });

  socket.on("disconnect", () => {
    const partner = pairs.get(socket.id);

    if (partner) {
      pairs.delete(partner);
      pairs.delete(socket.id);

      io.to(partner).emit("partner_disconnected");
    }

    remove(socket.id);
  });
});

server.listen(3000, () => {
  console.log("server running");
});