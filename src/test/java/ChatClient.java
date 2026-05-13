import dev.mccue.multiplayer.Client;
import dev.mccue.multiplayer.ToClientMessage;

import java.util.function.Consumer;


@SuppressWarnings("rawtypes")
public class ChatClient extends Client {
    public ChatClient(String host, int port) {
        super(host, port);
    }


    @Override
    protected void onMessage(ToClientMessage message, Consumer sendToServer) {
        switch (message) {
            case ToClientChat(var sender, var value) -> {
                IO.println("[" + (sender == null ? "SYSTEM" : sender) + "]: " + value);
            }
            default -> {
                IO.println("Unhandled message: " + message);
            }
        }

    }

    @Override
    protected void onDisconnect() {

    }
}
