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
  console.log("connected", socket.id);

  socket.on("find", () => {
    if (waiting && waiting !== socket.id) {
      const a = waiting;
      const b = socket.id;

      const room = a + "#" + b;

      io.to(a).emit("matched", { room, initiator: true });
      io.to(b).emit("matched", { room, initiator: false });

      waiting = null;
    } else {
      waiting = socket.id;
    }
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("disconnect", () => {
    if (waiting === socket.id) waiting = null;
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log("SERVER RUNNING");
});