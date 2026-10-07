"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ws_1 = require("ws");
const node_crypto_1 = __importDefault(require("node:crypto"));
const PORT = Number(process.env.PORT) || 8080;
const clients = new Map();
const lobbies = new Map();
const pendingAuthorityTransfers = new Map();
const wss = new ws_1.WebSocketServer({
    host: "0.0.0.0",
    port: PORT
});
function isKnownMessageType(value) {
    return (value === "login" ||
        value === "reconnect" ||
        value === "logout" ||
        value === "create_lobby" ||
        value === "join_lobby" ||
        value === "start_game" ||
        value === "offer" ||
        value === "answer" ||
        value === "candidate" ||
        value === "authority_transfer_request" ||
        value === "authority_transfer_ready");
}
function send(socket, message) {
    socket.send(JSON.stringify(message));
}
function sendToClient(client, message) {
    if (client.socket === null) {
        return;
    }
    send(client.socket, message);
}
function generateClientId() {
    return node_crypto_1.default.randomUUID();
}
function selectNextAuthority(lobby) {
    const candidates = [];
    for (const playerId of lobby.players) {
        const player = clients.get(playerId);
        if (player === undefined) {
            continue;
        }
        if (player.socket === null) {
            continue;
        }
        candidates.push(player.id);
    }
    candidates.sort();
    return candidates[0] ?? null;
}
function isValidUsername(username) {
    return (typeof username === "string" &&
        username.length >= 1 &&
        username.length <= 24);
}
// Lobby logic
//TODO: Review this in the future - Add more customization
function generateLobbyId() {
    return node_crypto_1.default.randomBytes(4).toString("hex").toUpperCase();
}
function broadcastToLobby(
//Used to broadcast all types of messages into a Lobby
lobby, message) {
    for (const playerId of lobby.players) {
        const player = clients.get(playerId);
        if (player === undefined) {
            continue; //Skip if undefined
        }
        sendToClient(player, message);
    }
}
function isValidReconnectMessage(message) {
    if (typeof message !== "object" || message === null) {
        return false;
    }
    if (!("type" in message) || !("session_token" in message)) {
        return false;
    }
    if (message.type !== "reconnect") {
        return false;
    }
    return (typeof message.session_token === "string" &&
        message.session_token.length > 0);
}
function findClientBySessionToken(sessionToken) {
    for (const client of clients.values()) {
        if (client.sessionToken === sessionToken) {
            return client;
        }
    }
    return undefined;
}
function isValidLobbyMode(mode) {
    return mode === "1v1" || mode === "2v2";
}
function handleDisconnect(client) {
    client.socket = null;
}
function handleAuthorityTransferRequest(client, targetPlayerId) {
    if (client.username === null) {
        send(client.socket, {
            type: "error",
            code: "NOT_LOGGED_IN"
        });
        return;
    }
    if (client.lobbyId === null) {
        send(client.socket, {
            type: "error",
            code: "NOT_IN_LOBBY"
        });
        return;
    }
    const lobby = lobbies.get(client.lobbyId);
    if (lobby === undefined) {
        return;
    }
    if (lobby.state !== "playing") {
        send(client.socket, {
            type: "error",
            code: "GAME_NOT_STARTED"
        });
        return;
    }
    if (lobby.authorityId !== client.id) {
        send(client.socket, {
            type: "error",
            code: "NOT_AUTHORITY"
        });
        return;
    }
    if (!lobby.players.has(targetPlayerId)) {
        send(client.socket, {
            type: "error",
            code: "PLAYER_NOT_IN_LOBBY"
        });
        return;
    }
    const target = clients.get(targetPlayerId);
    if (target === undefined || target.socket === null) {
        send(client.socket, {
            type: "error",
            code: "TARGET_NOT_CONNECTED"
        });
        return;
    }
    const transferId = node_crypto_1.default.randomBytes(16).toString("hex");
    const pendingTransfer = {
        transferId,
        lobbyId: lobby.id,
        oldAuthorityId: client.id,
        newAuthorityId: targetPlayerId
    };
    pendingAuthorityTransfers.set(transferId, pendingTransfer);
}
function attachSocket(client, socket) {
    client.socket = socket;
    socket.on("message", (data) => {
        handleMessage(client, data);
    });
    socket.on("close", () => {
        handleSocketClosed(client, socket);
    });
    socket.on("error", () => {
        handleSocketClosed(client, socket);
    });
}
function handleSocketClosed(client, socket) {
    //Compare sockets to avoid nulls due to stale sockets
    if (client.socket !== socket) {
        return;
    }
    client.socket = null;
}
function handleReconnect(newClient, sessionToken) {
    //new socket -> Existing player NOT new socket -> new player
    const existingClient = findClientBySessionToken(sessionToken);
    if (existingClient === undefined) {
        sendToClient(newClient, {
            type: "error",
            code: "INVALID_SESSION"
        });
        return;
    }
    if (existingClient.socket !== null) {
        sendToClient(newClient, {
            type: "error",
            code: "SESSION_ALREADY_CONNECTED"
        });
        return;
    }
    const newSocket = newClient.socket;
    if (newSocket === null) {
        return;
    }
    clients.delete(newClient.id);
    attachSocket(existingClient, newSocket);
    sendToClient(existingClient, {
        type: "reconnect_ok",
        player_id: existingClient.id,
        username: existingClient.username,
        lobby_id: existingClient.lobbyId
    });
    // After succesfull reconnect send lobby data to the reconnected player like a normal join event
    sendLobbyState(existingClient);
}
function getLobbyAuthority(lobbyId) {
    const lobby = lobbies.get(lobbyId);
    if (lobby === undefined) {
        return null;
    }
    const authority = clients.get(lobby.authorityId);
    if (authority === undefined) {
        return null;
    }
    return authority;
}
function sendLobbyState(client) {
    if (client.socket === null) {
        return;
    }
    if (client.lobbyId === null) {
        return;
    }
    const lobby = lobbies.get(client.lobbyId);
    if (lobby === undefined) {
        return;
    }
    const players = [];
    for (const playerId of lobby.players) {
        const player = clients.get(playerId);
        if (player === undefined) {
            continue;
        }
        players.push({
            id: player.id,
            username: player.username
        });
    }
    sendToClient(client, {
        type: "lobby_joined",
        lobby_id: lobby.id,
        mode: lobby.mode,
        max_players: lobby.maxPlayers,
        authority_id: lobby.authorityId,
        players
    });
}
// Helps validate the type of data received by WebRTC
function isValidSignalMessage(message) {
    // Input validations
    if (typeof message !== "object" || message === null)
        return false;
    if (!("type" in message))
        return false;
    if (!("to" in message))
        return false;
    if (!("data" in message))
        return false;
    if (message.type !== "offer" && message.type !== "answer" && message.type !== "candidate")
        return false;
    return typeof message.to === "string";
}
function handleMessage(client, data) {
    const socket = client.socket;
    if (socket === null) {
        return;
    }
    let message;
    try {
        message = JSON.parse(data.toString());
    }
    catch {
        sendToClient(client, {
            type: "error",
            code: "INVALID_JSON"
        });
        return;
    }
    if (typeof message !== "object" ||
        message === null ||
        !("type" in message) ||
        !isKnownMessageType(message.type)) {
        sendToClient(client, {
            type: "error",
            code: "UNKNOWN_MESSAGE_TYPE"
        });
        return;
    }
    if (message.type === "login") {
        // After logging in again, first check if it's a reconnect request, otherwise it will always be ALREADY_LOGGED_IN and reconnecting is not logged in yet
        if (isValidReconnectMessage(message)) {
            handleReconnect(client, message.session_token);
            return;
        }
        if (client.username !== null) {
            sendToClient(client, {
                type: "error",
                code: "ALREADY_LOGGED_IN",
                message: "This connection is already logged in."
            });
            return;
        }
        const username = "username" in message ? message.username : undefined;
        if (!isValidUsername(username)) {
            sendToClient(client, {
                type: "error",
                code: "INVALID_USERNAME",
                message: "Username must be 1-24 characters."
            });
            console.log("Hello: ", username);
            return;
        }
        client.username = username;
        sendToClient(client, {
            type: "login_ok",
            player_id: client.id,
            username: client.username,
            session_token: client.sessionToken
        });
        console.log(`Player logged in: ${client.username} (${client.id})`);
        return;
    }
    if (message.type === "logout") {
        client.username = null;
        sendToClient(client, {
            type: "logout_ok"
        });
        return;
    }
    if (message.type === "create_lobby") {
        if (client.username === null) {
            sendToClient(client, {
                type: "error",
                code: "NOT_LOGGED_IN",
                message: "You must be logged in to create a lobby"
            });
            return; // ERROR: User not logged in
        }
        if (client.lobbyId !== null) {
            sendToClient(client, {
                type: "error",
                code: "ALREADY_IN_LOBBY",
                message: "you are already in a lobby"
            });
            return; //ERROR: Already in a lobby
        }
        let lobbyId = generateLobbyId();
        // Validation for dupicates
        while (lobbies.has(lobbyId)) {
            lobbyId = generateLobbyId();
        }
        // Lobby mode is 1v1 or 2v2 only
        const mode = "mode" in message ? message.mode : undefined;
        if (!isValidLobbyMode(mode)) {
            sendToClient(client, {
                type: "error",
                code: "INVALID_LOBBY_MODE"
            });
            return; //ERROR: Invalid lobby mode, only 1v1 or 2v2
        }
        const maxPlayers = mode === "1v1" ? 2 : 4; //1v1 (2 players) 2v2 (4 players)
        const lobby = {
            id: lobbyId,
            hostId: client.id,
            authorityId: client.id,
            mode,
            maxPlayers,
            state: "waiting",
            players: new Set([client.id])
        };
        lobbies.set(lobbyId, lobby); //id + lobby
        client.lobbyId = lobbyId; //Set lobby id to client (creator)
        sendToClient(client, {
            type: "lobby_created",
            lobby_id: lobbyId,
            mode: lobby.mode,
            max_players: lobby.maxPlayers,
            host_id: lobby.hostId
        });
        console.log(`Lobby created: ${lobbyId} by ${client.username} (${client.id})`);
        return;
    }
    if (message.type === "join_lobby") {
        if (client.username === null) {
            sendToClient(client, {
                type: "error",
                code: "NOT_LOGGED_IN",
                message: "You must be logged in to join a lobby."
            });
            return;
        }
        if (client.lobbyId !== null) {
            sendToClient(client, {
                type: "error",
                code: "ALREADY_IN_LOBBY",
                message: "You are already in a lobby."
            });
            return;
        }
        const lobbyId = "lobby_id" in message ? message.lobby_id : undefined;
        if (typeof lobbyId !== "string") {
            sendToClient(client, {
                type: "error",
                code: "INVALID_LOBBY_ID",
                message: "lobby_id must be a string."
            });
            return;
        }
        const lobby = lobbies.get(lobbyId);
        if (lobby === undefined) {
            sendToClient(client, {
                type: "error",
                code: "LOBBY_NOT_FOUND",
                message: "Lobby does not exist."
            });
            return;
        }
        if (lobby.state !== "waiting") {
            sendToClient(client, {
                type: "error",
                code: "GAME_ALREADY_STARTED"
            });
            return;
        }
        if (lobby.players.size >= lobby.maxPlayers) {
            sendToClient(client, {
                type: "error",
                code: "LOBBY_FULL"
            });
            return;
        }
        lobby.players.add(client.id);
        client.lobbyId = lobby.id;
        sendToClient(client, {
            type: "lobby_joined",
            lobby_id: lobby.id,
            mode: lobby.mode,
            max_players: lobby.maxPlayers,
            host_id: lobby.hostId,
            authority_id: lobby.authorityId,
            players: Array.from(lobby.players)
        });
        broadcastToLobby(lobby, {
            type: "player_joined",
            player_id: client.id,
            username: client.username
        });
        console.log(`Player joined lobby: ${client.username} → ${lobby.id}`);
        return;
    }
    if (message.type === "start_game") {
        if (!client.username || !client.lobbyId) {
            sendToClient(client, {
                type: "error",
                code: "NOT_IN_LOBBY"
            });
            return;
        }
        const lobby = lobbies.get(client.lobbyId);
        if (!lobby) {
            sendToClient(client, {
                type: "error",
                code: "LOBBY_NOT_FOUND"
            });
            return;
        }
        if (lobby.hostId !== client.id) {
            sendToClient(client, {
                type: "error",
                code: "NOT_HOST"
            });
            return;
        }
        if (lobby.state !== "waiting") {
            sendToClient(client, {
                type: "error",
                code: "GAME_ALREADY_STARTED"
            });
            return;
        }
        if (lobby.players.size !== lobby.maxPlayers) {
            sendToClient(client, {
                type: "error",
                code: "LOBBY_NOT_FULL"
            });
            return;
        }
        lobby.state = "playing";
        console.log("PLAYING MODE STARTED");
        sendToClient(client, {
            type: "start_game_ok"
        });
        return;
    }
    if (message.type === "offer" || message.type === "answer" || message.type === "candidate") {
        if (client.username === null) {
            sendToClient(client, {
                type: "error",
                code: "NOT_LOGGED_IN",
                message: "You must be logged in to send signaling messages"
            });
            return; // ERROR: Not logged in
        }
        if (!isValidSignalMessage(message)) {
            sendToClient(client, {
                type: "error",
                code: "INVALID_SIGNAL",
                message: "Invalid signaling message"
            });
            return; // Invalid signal message
        }
        if (client.lobbyId === null) {
            sendToClient(client, {
                type: "error",
                code: "NOT_IN_LOBBY",
                message: "You must be in a lobby"
            });
            return;
        }
        const target = clients.get(message.to); //Who to send to
        if (target === undefined) {
            sendToClient(client, {
                type: "error",
                code: "PLAYER_NOT_FOUND",
                message: "The target player does not exist"
            });
            return;
        }
        if (target.lobbyId !== client.lobbyId) {
            sendToClient(client, {
                type: "error",
                code: "PLAYER_NOT_IN_LOBBY",
                message: "Target player is not on the lobby"
            });
            return;
        }
        // Send message OK
        sendToClient(target, {
            type: message.type,
            from: client.id,
            data: message.data
        });
        return;
    }
    sendToClient(client, {
        type: "error",
        code: "UNKNOWN_MESSAGE",
        message: `Unknown message type: ${message.type}`
    });
}
wss.on("connection", (socket) => {
    const client = {
        id: generateClientId(),
        socket: null,
        username: null,
        lobbyId: null,
        sessionToken: node_crypto_1.default.randomBytes(32).toString("hex")
    };
    clients.set(client.id, client);
    attachSocket(client, socket);
    console.log(`Client connected: ${client.id}`);
    sendToClient(client, {
        type: "connected",
        player_id: client.id
    });
});
console.log(`WebSocket server listening on port ${PORT}`);
