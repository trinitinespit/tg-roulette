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

server.listen(3000, () => {
  console.log("SERVER http://localhost:3000");
});