const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

let queue = [];

io.on("connection", (socket) => {
  console.log("connect:", socket.id);

  socket.on("find", () => {
    // убрать из очереди если уже был
    queue = queue.filter(s => s.id !== socket.id);

    if (queue.length > 0) {
      const partner = queue.shift();

      if (!partner) return;

      const room = socket.id + "#" + partner.id;

      socket.join(room);
      partner.join(room);

      socket.emit("matched", { room, initiator: true });
      partner.emit("matched", { room, initiator: false });

      console.log("MATCH:", room);
    } else {
      queue.push(socket);
      console.log("QUEUE:", socket.id);
    }
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("disconnect", () => {
    queue = queue.filter(s => s.id !== socket.id);
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log("Server running on port " + PORT);
  const express = require("express");
const app = express();
const http = require("http").createServer(app);
const io = require("socket.io")(http);
const path = require("path");

// 👇 ВАЖНО: отдаём статические файлы
app.use(express.static(__dirname));

// 👇 ГЛАВНАЯ СТРАНИЦА
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "test.html"));
});

// socket логика (если у тебя уже есть — оставь свою)
io.on("connection", (socket) => {
  console.log("user connected");
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
  console.log("Server running on " + PORT);
});