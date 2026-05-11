import dev.mccue.multiplayer.Server;
import dev.mccue.multiplayer.ClientId;

public class ChatServer extends Server<ToServerChat, ToClientChat, Void> {
    public ChatServer(int port) {
        super(port);
    }

    @Override
    protected void onDisconnect(ClientId clientId) {
        broadcast(new ToClientChat(null, clientId + " has disconnected"));
        IO.println(connected());
    }

    @Override
    protected void onConnect(ClientId clientId) {
        broadcast(new ToClientChat(null, clientId + " has connected"));
        IO.println(connected());
    }

    @Override
    protected void onMessage(ClientId clientId, ToServerChat message) {
        broadcast(new ToClientChat(clientId, message.value()));
    }
}
