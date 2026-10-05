import { WebSocketServer, WebSocket } from "ws";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT) || 8080;

type Client = {
  id: String;
  socket: WebSocket;
  username: String | null;
  lobbyId: String | null
};

type Lobby = {
    id: String
    hostId: String
    players: Set<String>
}

const clients = new Map<String, Client>();
const lobbies = new Map<String, Lobby>();

const wss = new WebSocketServer({
  host: "0.0.0.0",
  port: PORT
});

function send(socket: WebSocket, message: object): void {
  socket.send(JSON.stringify(message));
}

function generateClientId(): String {
  return crypto.randomUUID();
}

function isValidUsername(username: unknown): username is String {
  return (
    typeof username === "string" &&
    username.length >= 1 &&
    username.length <= 24
  );
}

// Lobby logic
//TODO: Review this in the future - Add more customization
function generateLobbyId(): string {
    return crypto.randomBytes(4).toString("hex").toUpperCase()
}

function broadcastToLobby(
    //Used to broadcast all types of messages into a Lobby
    lobby: Lobby,
    message: object
): void {
    for (const playerId of lobby.players) {
        const player = clients.get(playerId)

        if (player === undefined){
            continue //Skip if undefined
        }

        send(player.socket, message)
    }
}

wss.on("connection", (socket: WebSocket) => {
  const client: Client = {
    id: generateClientId(),
    socket,
    username: null,
    lobbyId: null
  };

  clients.set(client.id, client);

  console.log(`Client connected: ${client.id}`);

  send(socket, {
    type: "connected",
    player_id: client.id
  });

  socket.on("message", (data) => {
    let message: unknown;

    try {
      message = JSON.parse(data.toString());
    } catch {
      send(socket, {
        type: "error",
        code: "INVALID_JSON",
        message: "Message must contain valid JSON."
      });

      return;
    }

    if (
      typeof message !== "object" ||
      message === null ||
      !("type" in message) ||
      typeof message.type !== "string"
    ) {
      send(socket, {
        type: "error",
        code: "INVALID_MESSAGE",
        message: "Message must contain a string 'type'."
      });

      return;
    }

    if (message.type === "login") {
      if (client.username !== null) {
        send(socket, {
          type: "error",
          code: "ALREADY_LOGGED_IN",
          message: "This connection is already logged in."
        });

        return;
      }

      const username =
        "username" in message ? message.username : undefined;

      if (!isValidUsername(username)) {
        send(socket, {
          type: "error",
          code: "INVALID_USERNAME",
          message: "Username must be 1-24 characters."
        });

        console.log("Hello: ", username)
        return;
      }

      client.username = username;

      send(socket, {
        type: "login_ok",
        player_id: client.id,
        username: client.username
      });

      console.log(
        `Player logged in: ${client.username} (${client.id})`
      );

      return;
    }

    if (message.type === "logout") {
      client.username = null;

      send(socket, {
        type: "logout_ok"
      });

      return;
    }

    if (message.type === "create_lobby") {
        if (client.username === null) {
            send(socket, {
                type: "error",
                code: "NOT_LOGGED_IN",
                message: "You must be logged in to create a lobby"
            }); return // ERROR: User not logged in
        }

        if (client.lobbyId !== null) {
            send(socket, {
                type: "error",
                code: "ALREADY_IN_LOBBY",
                message: "you are already in a lobby"
            }); return //ERROR: Already in a lobby
        }

        let lobbyId = generateLobbyId();

        // Validation for dupicates
        while (lobbies.has(lobbyId)) {
            lobbyId = generateLobbyId()
        }

        const lobby: Lobby = {
            id: lobbyId,
            hostId: client.id,
            players: new Set([client.id])
        }

        lobbies.set(lobbyId, lobby) //id + lobby
        client.lobbyId = lobbyId //Set lobby id to client (creator)

        send(socket, {
            type: "lobby_created",
            lobby_id: lobbyId
        })

        console.log(`Lobby created: ${lobbyId} by ${client.username} (${client.id})`);

        return;
    }

    if (message.type === "join_lobby") {
        if (client.username === null) {
            send(socket, {
            type: "error",
            code: "NOT_LOGGED_IN",
            message: "You must be logged in to join a lobby."
            });

            return;
        }

        if (client.lobbyId !== null) {
            send(socket, {
            type: "error",
            code: "ALREADY_IN_LOBBY",
            message: "You are already in a lobby."
            });

            return;
        }

        const lobbyId =
            "lobby_id" in message ? message.lobby_id : undefined;

        if (typeof lobbyId !== "string") {
            send(socket, {
            type: "error",
            code: "INVALID_LOBBY_ID",
            message: "lobby_id must be a string."
            });

            return;
        }

        const lobby = lobbies.get(lobbyId);

        if (lobby === undefined) {
            send(socket, {
            type: "error",
            code: "LOBBY_NOT_FOUND",
            message: "Lobby does not exist."
            });

            return;
        }

        lobby.players.add(client.id);
        client.lobbyId = lobby.id;

        send(socket, {
            type: "lobby_joined",
            lobby_id: lobby.id,
            players: Array.from(lobby.players)
        });

        broadcastToLobby(lobby, {
            type: "player_joined",
            player_id: client.id,
            username: client.username
        });

        console.log(
            `Player joined lobby: ${client.username} → ${lobby.id}`
        );

        return;
    }

    send(socket, {
      type: "error",
      code: "UNKNOWN_MESSAGE",
      message: `Unknown message type: ${message.type}`
    });
  });

  socket.on("close", () => {
    const lobbyId = client.lobbyId;

    if (lobbyId !== null) {
        const lobby = lobbies.get(lobbyId);

        if (lobby !== undefined) {
        // Host disconnected: close the entire lobby.
        if (lobby.hostId === client.id) {
            for (const playerId of lobby.players) {
            if (playerId === client.id) {
                continue;
            }

            const player = clients.get(playerId);

            if (player === undefined) {
                continue;
            }

            send(player.socket, {
                type: "lobby_closed",
                reason: "HOST_DISCONNECTED"
            });

            player.lobbyId = null;
            }

            lobbies.delete(lobbyId);

            console.log(
            `Lobby closed because host disconnected: ${lobbyId}`
            );
        } else {
            // Normal player disconnected.
            lobby.players.delete(client.id);

            for (const playerId of lobby.players) {
            const player = clients.get(playerId);

            if (player === undefined) {
                continue;
            }

            send(player.socket, {
                type: "player_left",
                player_id: client.id
            });
            }

            if (lobby.players.size === 0) {
            lobbies.delete(lobbyId);

            console.log(`Lobby removed: ${lobbyId}`);
            }
        }
        }
    }

    clients.delete(client.id);

    console.log(`Client disconnected: ${client.id}`);
    });
});

console.log(`WebSocket server listening on port ${PORT}`);