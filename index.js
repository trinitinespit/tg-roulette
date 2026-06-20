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
let paired = new Map();

function remove(id) {
  queue = queue.filter(x => x !== id);
}

function unpair(id) {
  const partner = paired.get(id);
  if (!partner) return;

  paired.delete(id);
  paired.delete(partner);

  io.to(id).emit("partner_left");
  io.to(partner).emit("partner_left");
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();

    if (!a || !b) return;

    const room = `${a}#${b}`;

    paired.set(a, b);
    paired.set(b, a);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
  }
}

io.on("connection", (socket) => {
  console.log("CONNECTED", socket.id);

  socket.on("find", () => {
    console.log("FIND", socket.id);

    unpair(socket.id);
    remove(socket.id);

    queue.push(socket.id);
    tryMatch();
  });

  socket.on("next", () => {
    unpair(socket.id);
    remove(socket.id);

    queue.push(socket.id);
    tryMatch();
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("disconnect", () => {
    unpair(socket.id);
    remove(socket.id);
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log("SERVER RUNNING");
});