package dev.mccue.multiplayer;

import java.util.List;

public sealed class ServerContext<ToClient extends ToClientMessage, ClientState>
    permits RunningServer {
    private final Server<?, ToClient, ClientState> server;

    ServerContext(Server<?, ToClient, ClientState> server) {
        this.server = server;
    }


    public ClientState getState(ClientId clientId) {
        return server.getState(clientId);
    }


    public void setState(ClientId clientId, ClientState state) {
        server.setState(clientId, state);
    }

    /// Sends a message to all connected clients.
    public void sendToAllClients(ToClient message) {
        server.sendToAllClients(message);
    }

    /// Sends a message to a specific connected client
    public boolean sendToClient(ClientId id, ToClient message) {
        return server.sendToClient(id, message);
    }

    public List<ClientId> connected() {
        return server.connected();
    }
}
