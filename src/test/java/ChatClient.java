import dev.mccue.multiplayer.AbstractClient;

import java.util.function.Consumer;

public class ChatClient extends AbstractClient<ToServerChat, ToClientChat> {
    public ChatClient(String host, int port) {
        super(host, port);
    }

    @Override
    public void onMessage(ToClientChat message, Consumer<ToServerChat> sendToServer) {
        IO.println("[" + (message.sender() == null ? "SYSTEM" : message.sender()) + "]: " + message.value());
    }

    @Override
    public void onDisconnect() {

    }
}
