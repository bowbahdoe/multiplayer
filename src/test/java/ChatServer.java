import dev.mccue.multiplayer.Server;
import dev.mccue.multiplayer.ClientId;
import dev.mccue.multiplayer.ToServerMessage;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ChatServer extends Server {
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
    protected void onMessage(ClientId clientId, ToServerMessage message) {
        switch (message) {
            case ToServerChat(var value) -> {
                broadcast(new ToClientChat(clientId, value));
            }
            default -> {
                send()
            }
        }
    }
}
