const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, "public")));

// 💥 ВАЖНО: ЖЁСТКО ОТДАЁМ HTML
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

let waiting = null;

io.on("connection", (socket) => {
  console.log("[connect]", socket.id, "| waiting was:", waiting);

  socket.on("find", () => {
    console.log("[find] received from", socket.id, "| current waiting:", waiting);

    if (waiting && waiting !== socket.id && io.sockets.sockets.has(waiting)) {
      const a = waiting;
      const b = socket.id;

      const room = a + "#" + b;

      // КЛЮЧЕВОЙ ФИКС: оба сокета должны реально войти в комнату,
      // иначе socket.to(room) в обработчике "signal" будет улетать в пустоту
      const socketA = io.sockets.sockets.get(a);
      socketA?.join(room);
      socket.join(room);

      console.log("[match]", a, "<->", b, "| room:", room);

      io.to(a).emit("matched", { room, initiator: true });
      io.to(b).emit("matched", { room, initiator: false });

      waiting = null;
    } else {
      waiting = socket.id;
      console.log("[waiting] set to", socket.id);
    }
  });

  socket.on("signal", ({ room, data }) => {
    console.log("[signal]", socket.id, "-> room", room, "| keys:", Object.keys(data));
    socket.to(room).emit("signal", data);
  });

  socket.on("disconnect", (reason) => {
    console.log("[disconnect]", socket.id, "| reason:", reason);
    if (waiting === socket.id) {
      waiting = null;
      console.log("[waiting] cleared, was", socket.id);
    }
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log("SERVER RUNNING");
});