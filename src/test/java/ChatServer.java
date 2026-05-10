import dev.mccue.multiplayer.AbstractServer;
import dev.mccue.multiplayer.ClientId;

public class ChatServer extends AbstractServer<ToServerChat, ToClientChat, Void> {
    public ChatServer(int port) {
        super(port);
    }

    @Override
    protected void onDisconnect(ClientId clientId) {
        broadcast(new ToClientChat(null, clientId + " has disconnected"));
    }

    @Override
    protected void onConnect(ClientId clientId) {
        broadcast(new ToClientChat(null, clientId + " has connected"));
    }

    @Override
    protected void onMessage(ClientId clientId, ToServerChat message) {
        broadcast(new ToClientChat(clientId, message.value()));
    }
}
