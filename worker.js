import { DurableObject } from "cloudflare:workers";

const ROOM_ID = "AN26FC";

/*
 * IMPORTANT:
 * Generate your own long random secret.
 *
 * The SAME value must be placed in coListen.js on YOUR
 * permanent-host Spotify installation.
 *
 * NEVER give this value to guests.
 */
const HOST_TOKEN =
  "REPLACE_WITH_YOUR_LONG_RANDOM_SECRET";


export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (parts.length === 0) {
      return new Response("CoListen server OK");
    }

    if (parts[0] !== "room") {
      return new Response("Not found", { status: 404 });
    }

    const roomId = parts[1];

    /*
     * AN26FC is the ONLY room.
     */
    if (roomId !== ROOM_ID) {
      return new Response("Invalid room", { status: 404 });
    }

    /*
     * A deterministic Durable Object ID means the same room
     * is always represented by the same Durable Object.
     */
    const id = env.ROOMS.idFromName(ROOM_ID);
    const stub = env.ROOMS.get(id);

    return stub.fetch(request);
  }
};


export class Room extends DurableObject {

  constructor(ctx, env) {
    super(ctx, env);

    this.ctx = ctx;
    this.env = env;

    /*
     * Connected WebSocket clients.
     */
    this.sessions = new Map();

    /*
     * Persistent room data.
     */
    this.hostName = null;
    this.lastState = null;
    this.queueSnapshot = [];

    this.initialized = false;
  }


  async initialize() {
    if (this.initialized) {
      return;
    }

    this.hostName =
      await this.ctx.storage.get("hostName") || null;

    this.lastState =
      await this.ctx.storage.get("lastState") || null;

    this.queueSnapshot =
      await this.ctx.storage.get("queueSnapshot") || [];

    this.initialized = true;
  }


  async fetch(request) {
    await this.initialize();

    /*
     * HTTP health/status request.
     */
    if (
      request.headers.get("Upgrade") !== "websocket"
    ) {
      return new Response(
        JSON.stringify({
          room: ROOM_ID,
          host: this.hostName,
          hostConnected: this.isHostConnected(),
          members: this.getMembers(),
          hasState: !!this.lastState
        }),
        {
          headers: {
            "Content-Type": "application/json"
          }
        }
      );
    }


    const url = new URL(request.url);

    const name =
      url.searchParams.get("name") ||
      "Anonymous";

    const hostToken =
      url.searchParams.get("hostToken") ||
      "";


    /*
     * The host is identified by the permanent secret,
     * NOT by the WebSocket ID.
     */
    const isHost =
      hostToken === HOST_TOKEN;


    const pair =
      new WebSocketPair();

    const client =
      pair[0];

    const server =
      pair[1];

    server.accept();


    /*
     * A new temporary connection ID.
     *
     * This is NOT the host identity.
     */
    const sid =
      crypto.randomUUID();


    /*
     * If this is the permanent host installation,
     * remember its display name.
     */
    if (isHost) {
      this.hostName = name;

      await this.ctx.storage.put(
        "hostName",
        name
      );

      /*
       * If the same host reconnects while an old connection
       * still exists, close the old host connection.
       */
      for (
        const [oldSid, oldSession]
        of this.sessions
      ) {
        if (oldSession.isHost) {

          try {
            oldSession.ws.close(
              1000,
              "Host reconnected"
            );
          } catch {}

          this.sessions.delete(oldSid);
        }
      }
    }


    this.sessions.set(
      sid,
      {
        ws: server,
        name,
        isHost
      }
    );


    /*
     * Tell this client everything it needs to know.
     */
    this.sendTo(
      sid,
      {
        type: "room_info",

        room: ROOM_ID,

        hostName: this.hostName,

        isHost,

        hostConnected:
          this.isHostConnected(),

        members:
          this.getMembers()
      }
    );


    /*
     * A guest immediately receives the last known
     * canonical playback state.
     */
    if (
      !isHost &&
      this.lastState
    ) {
      this.sendTo(
        sid,
        {
          type: "state",
          state: this.lastState
        }
      );
    }


    /*
     * Give a newly joined guest the current queue.
     */
    if (
      !isHost &&
      Array.isArray(this.queueSnapshot)
    ) {
      this.sendTo(
        sid,
        {
          type: "queue_snapshot",
          uris: this.queueSnapshot
        }
      );
    }


    /*
     * Tell everybody else about the new member.
     */
    this.broadcast(
      {
        type: "joined",
        user: name
      },
      sid
    );

    this.broadcastMembers();


    /*
     * WebSocket message handler.
     */
    server.addEventListener(
      "message",
      async event => {

        try {

          const msg =
            JSON.parse(event.data);

          await this.handleMessage(
            sid,
            msg
          );

        } catch (error) {

          console.error(
            "CoListen message error:",
            error
          );

        }
      }
    );


    /*
     * Disconnect.
     */
    server.addEventListener(
      "close",
      () => {

        const session =
          this.sessions.get(sid);

        this.sessions.delete(sid);

        if (session) {

          this.broadcast({
            type: "left",
            user: session.name
          });

        }

        this.broadcastMembers();


        /*
         * IMPORTANT:
         *
         * We do NOT delete the permanent host.
         *
         * AN26FC remains associated with the same host
         * token even while the host is offline.
         */
        if (
          session &&
          session.isHost
        ) {

          this.broadcast({
            type: "host_status",
            connected: false,
            hostName: this.hostName
          });

        }
      }
    );


    server.addEventListener(
      "error",
      () => {
        this.sessions.delete(sid);
      }
    );


    return new Response(
      null,
      {
        status: 101,
        webSocket: client
      }
    );
  }


  /*
   * ============================================
   * HOST
   * ============================================
   */

  isHostConnected() {

    for (
      const session
      of this.sessions.values()
    ) {

      if (session.isHost) {
        return true;
      }

    }

    return false;
  }


  getHost() {

    for (
      const [sid, session]
      of this.sessions
    ) {

      if (session.isHost) {

        return {
          sid,
          ...session
        };

      }
    }

    return null;
  }


  /*
   * ============================================
   * MEMBERS
   * ============================================
   */

  getMembers() {

    return [
      ...this.sessions.values()
    ].map(
      session => session.name
    );
  }


  broadcastMembers() {

    this.broadcast({
      type: "members",
      members: this.getMembers()
    });

  }


  /*
   * ============================================
   * MESSAGE ROUTER
   * ============================================
   */

  async handleMessage(
    senderId,
    msg
  ) {

    if (!msg?.type) {
      return;
    }


    const sender =
      this.sessions.get(senderId);

    if (!sender) {
      return;
    }


    /*
     * --------------------------------------------
     * Guest → Permanent Host
     * --------------------------------------------
     *
     * Every guest can request playback actions.
     *
     * Only the permanent host executes them.
     */
    if (msg.type === "cmd") {

      const host =
        this.getHost();

      if (!host) {

        this.sendTo(
          senderId,
          {
            type: "host_unavailable",
            hostName: this.hostName
          }
        );

        return;
      }


      this.sendTo(
        host.sid,
        {
          type: "cmd",

          cmd: msg.cmd,

          requestFrom: senderId
        }
      );

      return;
    }


    /*
     * --------------------------------------------
     * Host → Server: canonical playback state
     * --------------------------------------------
     */
    if (msg.type === "state") {

      /*
       * Guests cannot publish canonical state.
       */
      if (!sender.isHost) {
        return;
      }


      this.lastState =
        msg.state;


      await this.ctx.storage.put(
        "lastState",
        this.lastState
      );


      /*
       * Send canonical state to everyone except host.
       */
      this.broadcast(
        {
          type: "state",
          state: this.lastState
        },
        senderId
      );

      return;
    }


    /*
     * --------------------------------------------
     * Guest → Host time synchronization
     * --------------------------------------------
     */
    if (msg.type === "ts_req") {

      const host =
        this.getHost();

      if (!host) {
        return;
      }


      this.sendTo(
        host.sid,
        {
          type: "ts_req",

          id: msg.id,

          t0: msg.t0,

          requestFrom: senderId
        }
      );

      return;
    }


    /*
     * --------------------------------------------
     * Host → Guest time synchronization response
     * --------------------------------------------
     */
    if (msg.type === "ts_resp") {

      if (!sender.isHost) {
        return;
      }


      if (msg.requestFrom) {

        this.sendTo(
          msg.requestFrom,
          msg
        );

      }

      return;
    }


    /*
     * --------------------------------------------
     * Queue addition
     * --------------------------------------------
     */
    if (msg.type === "queue_add") {

      if (!sender.isHost) {
        return;
      }


      if (!msg.uri) {
        return;
      }


      if (
        !this.queueSnapshot.includes(
          msg.uri
        )
      ) {

        this.queueSnapshot.push(
          msg.uri
        );


        /*
         * Avoid an infinitely growing room queue.
         */
        if (
          this.queueSnapshot.length > 100
        ) {

          this.queueSnapshot =
            this.queueSnapshot.slice(-100);

        }


        await this.ctx.storage.put(
          "queueSnapshot",
          this.queueSnapshot
        );
      }


      this.broadcast(
        {
          type: "queue_add",
          uri: msg.uri
        },
        senderId
      );

      return;
    }


    /*
     * --------------------------------------------
     * Queue snapshot
     * --------------------------------------------
     */
    if (
      msg.type === "queue_snapshot"
    ) {

      if (!sender.isHost) {
        return;
      }


      if (
        !Array.isArray(msg.uris)
      ) {
        return;
      }


      this.queueSnapshot =
        msg.uris.slice(0, 100);


      await this.ctx.storage.put(
        "queueSnapshot",
        this.queueSnapshot
      );


      this.broadcast(
        {
          type: "queue_snapshot",
          uris: this.queueSnapshot
        },
        senderId
      );

      return;
    }


    /*
     * --------------------------------------------
     * Host heartbeat/status
     * --------------------------------------------
     */
    if (
      msg.type === "host_ping"
    ) {

      if (!sender.isHost) {
        return;
      }


      this.broadcast({
        type: "host_status",
        connected: true,
        hostName: this.hostName
      });

      return;
    }
  }


  sendTo(
    sid,
    msg
  ) {

    const session =
      this.sessions.get(sid);

    if (!session) {
      return;
    }


    try {

      session.ws.send(
        JSON.stringify(msg)
      );

    } catch (error) {

      console.error(
        "CoListen sendTo error:",
        error
      );

    }
  }


  broadcast(
    msg,
    excludeId = null
  ) {

    const data =
      JSON.stringify(msg);


    for (
      const [sid, session]
      of this.sessions
    ) {

      if (
        sid === excludeId
      ) {
        continue;
      }


      try {

        session.ws.send(data);

      } catch (error) {

        console.error(
          "CoListen broadcast error:",
          error
        );

      }
    }
  }
}
