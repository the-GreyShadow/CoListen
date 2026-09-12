export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (parts[0] !== "room" || !parts[1]) {
      return new Response("Not found", { status: 404 });
    }

    const roomId = parts[1];

    const id = env.ROOMS.idFromName(roomId);
    const stub = env.ROOMS.get(id);

    return stub.fetch(request);
  }
};

export class Room {
  constructor(state) {
    this.state = state;
    this.sessions = new Map();
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const url = new URL(request.url);
    const name = url.searchParams.get("name") || "Anonymous";

    const [client, server] = Object.values(new WebSocketPair());

    server.accept();

    const sessionId = crypto.randomUUID();

    this.sessions.set(sessionId, {
      ws: server,
      name
    });

    this.broadcast(
      {
        type: "joined",
        user: name
      },
      sessionId
    );

    this.broadcast(
      {
        type: "members",
        members: [...this.sessions.values()].map(s => s.name)
      },
      null
    );

    server.addEventListener("message", evt => {
      try {
        const msg = JSON.parse(evt.data);
        this.broadcast(msg, sessionId);
      } catch {}
    });

    server.addEventListener("close", () => {
      this.sessions.delete(sessionId);

      this.broadcast(
        {
          type: "left",
          user: name
        },
        sessionId
      );

      this.broadcast(
        {
          type: "members",
          members: [...this.sessions.values()].map(s => s.name)
        },
        null
      );
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }

  broadcast(msg, excludeId) {
    const data = JSON.stringify(msg);

    for (const [id, session] of this.sessions) {
      if (id !== excludeId) {
        try {
          session.ws.send(data);
        } catch {}
      }
    }
  }
}
