import { DurableObject } from "cloudflare:workers";

const ROOM_ID = "AN26FC";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("CoListen server is running. Permanent room: AN26FC", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        room: ROOM_ID,
        hostTokenConfigured: !!env.HOST_TOKEN,
      });
    }

    const match = url.pathname.match(/^\/room\/([A-Z0-9]{6})$/i);
    if (!match || match[1].toUpperCase() !== ROOM_ID) {
      return new Response("Room not found", { status: 404 });
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }

    const room = env.ROOMS.get(env.ROOMS.idFromName(ROOM_ID));
    return room.fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.hostSid = null;
    this.loaded = false;
    this.hostName = "";
    this.lastState = null;
    this.queueSnapshot = [];
    this.latestGuestState = null;
    this.initialState = null;
    this.firstJoinSid = null;
  }

  async load() {
    if (this.loaded) return;
    const stored = await this.ctx.storage.get(["hostName", "lastState", "queueSnapshot", "latestGuestState", "initialState"]);
    this.hostName = stored.hostName || "";
    this.lastState = stored.lastState || null;
    this.queueSnapshot = Array.isArray(stored.queueSnapshot) ? stored.queueSnapshot : [];
    this.latestGuestState = stored.latestGuestState || null;
    this.initialState = stored.initialState || null;
    this.loaded = true;

    // Recover host identity after a DO restart by inspecting accepted sockets.
    for (const ws of this.ctx.getWebSockets()) {
      const meta = ws.deserializeAttachment?.();
      if (meta?.isHost) {
        this.hostSid = meta.sid;
        break;
      }
    }
  }

  sockets() {
    return this.ctx.getWebSockets();
  }

  send(ws, msg) {
    try {
      ws.send(JSON.stringify(msg));
    } catch {}
  }

  broadcast(msg, exceptSid = null) {
    const data = JSON.stringify(msg);
    for (const ws of this.sockets()) {
      const meta = ws.deserializeAttachment?.();
      if (exceptSid && meta?.sid === exceptSid) continue;
      try {
        ws.send(data);
      } catch {}
    }
  }

  findHostSocket() {
    for (const ws of this.sockets()) {
      const meta = ws.deserializeAttachment?.();
      if (meta?.isHost) return ws;
    }
    return null;
  }

  async fetch(request) {
    await this.load();

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }

    const url = new URL(request.url);
    const name = (url.searchParams.get("name") || "Guest").trim().slice(0, 64) || "Guest";
    const suppliedToken = url.searchParams.get("hostToken") || "";
    const expectedToken = this.env.HOST_TOKEN || "";

    // A host connection MUST prove possession of the Cloudflare secret.
    // Supplying a wrong token is an authentication failure, not a guest login.
    const wantsHost = suppliedToken.length > 0;
    const isHost = wantsHost && suppliedToken === expectedToken;

    if (wantsHost && !isHost) {
      return new Response("Invalid host token", { status: 401 });
    }

    if (!expectedToken) {
      return new Response("HOST_TOKEN is not configured in Worker secrets", { status: 500 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    const sid = crypto.randomUUID();
    const isFirstJoiner = this.getMembers().length === 0;
    if (isFirstJoiner) this.firstJoinSid = sid;

    // If the permanent host reconnects, the new connection replaces the old
    // host connection. The identity remains the same because it is token-based.
    if (isHost) {
      const oldHost = this.findHostSocket();
      if (oldHost) {
        try {
          oldHost.close(4001, "Permanent host reconnected");
        } catch {}
      }
    }

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      sid,
      name,
      isHost,
      isFirstJoiner,
    });

    if (isHost) {
      this.hostSid = sid;
      this.hostName = name;
      await this.ctx.storage.put("hostName", name);
    }

    const members = this.getMembers();

    this.send(server, {
      type: "room_info",
      room: ROOM_ID,
      isHost,
      hostName: this.hostName || "Permanent host",
      hostConnected: !!this.findHostSocket(),
      members,
    });

    // The first client into an empty room establishes the initial playback
    // state. A later client adopts that state, regardless of whether the first
    // client was the host or the guest. This is separate from the permanent
    // host role: after both are connected, the host is the canonical authority.
    if (this.initialState && !isHost) {
      this.send(server, { type: "initial_state", state: this.initialState });
    } else if (this.initialState && isHost && this.getMembers().length > 1) {
      this.send(server, { type: "initial_state", state: this.initialState });
    }

    // Give a joining guest the last durable state immediately. If the host is
    // online, it will then send an even fresher canonical state.
    if (!isHost) {
      if (this.lastState) this.send(server, { type: "state", state: this.lastState });
      this.send(server, { type: "queue_snapshot", uris: this.queueSnapshot });
    } else if (this.latestGuestState) {
      this.send(server, {
        type: "startup_sync_state",
        state: this.latestGuestState,
      });
      this.latestGuestState = null;
      await this.ctx.storage.put("latestGuestState", null);
    }

    this.broadcast(
      {
        type: "joined",
        user: name,
      },
      sid
    );

    this.broadcast({
      type: "members",
      members: this.getMembers(),
    });

    this.broadcast({
      type: "host_status",
      connected: !!this.findHostSocket(),
      hostName: this.hostName || "Permanent host",
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  getMembers() {
    const names = [];
    for (const ws of this.sockets()) {
      const meta = ws.deserializeAttachment?.();
      if (meta?.name && !names.includes(meta.name)) names.push(meta.name);
    }
    return names;
  }

  async webSocketMessage(ws, message) {
    await this.load();

    let msg;
    try {
      msg = JSON.parse(message);
    } catch {
      return;
    }

    const meta = ws.deserializeAttachment?.();
    if (!meta) return;

    // Never trust the client-supplied _sid for authorization.
    const senderSid = meta.sid;

    if (msg.type === "ts_req") {
      const host = this.findHostSocket();
      if (!host) {
        this.send(ws, { type: "host_unavailable" });
        return;
      }

      // Host must answer its own timestamp request; route guest requests only
      // to the authenticated permanent host.
      if (meta.isHost) {
        this.send(ws, {
          type: "ts_resp",
          id: msg.id,
          t0: msg.t0,
          t1: Date.now(),
        });
      } else {
        this.send(host, {
          type: "ts_req",
          id: msg.id,
          t0: msg.t0,
          requestFrom: senderSid,
        });
      }
      return;
    }

    if (msg.type === "ts_resp") {
      // Timestamp responses from the host are routed only to the guest that
      // originally requested them.
      if (!meta.isHost) return;
      const targetSid = msg.requestFrom || msg._sid;
      if (!targetSid) return;

      for (const peer of this.sockets()) {
        const peerMeta = peer.deserializeAttachment?.();
        if (peerMeta?.sid === targetSid) {
          this.send(peer, {
            type: "ts_resp",
            id: msg.id,
            t0: msg.t0,
            t1: msg.t1,
          });
          break;
        }
      }
      return;
    }

    if (msg.type === "initial_state") {
      // Only the actual first connection is allowed to establish the initial
      // state. This makes "first to join wins" deterministic even if the two
      // clients send their state at nearly the same time.
      if (meta.sid !== this.firstJoinSid || this.initialState || !msg.state?.uri) return;

      this.initialState = {
        uri: String(msg.state.uri),
        name: String(msg.state.name || ""),
        position: Number(msg.state.position || 0),
        isPlaying: !!msg.state.isPlaying,
        sentAt: Number(msg.state.sentAt || Date.now()),
      };
      await this.ctx.storage.put("initialState", this.initialState);

      // The second client may already be connected by the time the first
      // client's playback state arrives, so always forward the first state
      // to every other client.
      this.broadcast({ type: "initial_state", state: this.initialState }, senderSid);
      return;
    }

    if (msg.type === "control_state") {
      // A guest's native Spotify controls become a canonical state change by
      // the permanent host. Never let a guest write the canonical state store.
      if (meta.isHost || !msg.state?.uri) return;
      const host = this.findHostSocket();
      if (!host) {
        // Host offline: keep only the latest playback state for startup sync.
        this.latestGuestState = {
          uri: String(msg.state.uri),
          name: String(msg.state.name || ""),
          position: Number(msg.state.position || 0),
          isPlaying: !!msg.state.isPlaying,
          sentAt: Number(msg.state.sentAt || Date.now()),
        };
        await this.ctx.storage.put("latestGuestState", this.latestGuestState);
        return;
      }
      this.send(host, {
        type: "control_state",
        state: msg.state,
        requestFrom: senderSid,
      });
      return;
    }

    if (msg.type === "guest_state") {
      // Keep only the latest state while there is no permanent host.
      // There is deliberately no command queue.
      if (meta.isHost || this.findHostSocket() || !msg.state?.uri) return;

      this.latestGuestState = {
        uri: String(msg.state.uri),
        name: String(msg.state.name || ""),
        position: Number(msg.state.position || 0),
        isPlaying: !!msg.state.isPlaying,
        sentAt: Number(msg.state.sentAt || Date.now()),
      };

      await this.ctx.storage.put("latestGuestState", this.latestGuestState);
      return;
    }

    if (msg.type === "cmd") {
      const host = this.findHostSocket();

      if (!host) {
        this.send(ws, { type: "host_unavailable" });
        return;
      }

      // Commands are always executed by the permanent host, regardless of
      // which guest sent them.
      if (meta.isHost) {
        return;
      }

      this.send(host, {
        type: "cmd",
        cmd: msg.cmd,
        requestFrom: senderSid,
      });
      return;
    }

    if (msg.type === "state") {
      // Only the permanent host may publish canonical playback state.
      if (!meta.isHost) return;
      if (!msg.state?.uri) return;

      this.lastState = {
        uri: String(msg.state.uri),
        name: String(msg.state.name || ""),
        position: Number(msg.state.position || 0),
        isPlaying: !!msg.state.isPlaying,
        sentAt: Number(msg.state.sentAt || Date.now()),
      };

      await this.ctx.storage.put("lastState", this.lastState);
      this.broadcast(
        {
          type: "state",
          state: this.lastState,
        },
        senderSid
      );
      return;
    }

    if (msg.type === "queue_add") {
      if (!meta.isHost || !msg.uri) return;

      const uri = String(msg.uri);
      if (!this.queueSnapshot.includes(uri)) {
        this.queueSnapshot.push(uri);
        if (this.queueSnapshot.length > 50) this.queueSnapshot = this.queueSnapshot.slice(-50);
      }

      await this.ctx.storage.put("queueSnapshot", this.queueSnapshot);
      this.broadcast({ type: "queue_add", uri }, senderSid);
      return;
    }

    if (msg.type === "queue_snapshot") {
      if (!meta.isHost || !Array.isArray(msg.uris)) return;

      this.queueSnapshot = msg.uris
        .filter((x) => typeof x === "string" && x.length > 0)
        .slice(0, 50);

      await this.ctx.storage.put("queueSnapshot", this.queueSnapshot);
      this.broadcast({ type: "queue_snapshot", uris: this.queueSnapshot }, senderSid);
      return;
    }


  }

  async webSocketClose(ws) {
    await this.load();

    const meta = ws.deserializeAttachment?.();
    if (!meta) return;

    const wasHost = !!meta.isHost;

    if (wasHost && this.hostSid === meta.sid) {
      this.hostSid = null;
    }

    this.broadcast({ type: "left", user: meta.name });
    const remaining = this.getMembers();
    this.broadcast({ type: "members", members: remaining });

    if (remaining.length === 0) {
      this.initialState = null;
      this.firstJoinSid = null;
      await this.ctx.storage.put("initialState", null);
      // A new empty session may establish a fresh first-joiner state.
    }

    if (wasHost) {
      this.broadcast({
        type: "host_status",
        connected: !!this.findHostSocket(),
        hostName: this.hostName || "Permanent host",
      });
    }
  }

}
