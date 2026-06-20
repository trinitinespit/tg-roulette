const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// ===== STATIC =====
app.use(express.static(path.join(__dirname, "public")));

// ===== ГЛАВНАЯ СТРАНИЦА =====
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

// ===== MATCHMAKING =====
let queue = [];

io.on("connection", (socket) => {
  console.log("user connected:", socket.id);

  socket.on("find", () => {
    queue = queue.filter(s => s.id !== socket.id);

    if (queue.length > 0) {
      const partner = queue.shift();

      const room = socket.id + "#" + partner.id;

      socket.join(room);
      partner.join(room);

      socket.emit("matched", { room, initiator: true });
      partner.emit("matched", { room, initiator: false });

      console.log("MATCH:", room);
    } else {
      queue.push(socket);
    }
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("disconnect", () => {
    queue = queue.filter(s => s.id !== socket.id);
  });
});

// ===== START SERVER =====
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("Server running on port", PORT);
});