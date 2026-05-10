import dev.mccue.multiplayer.ClientId;
import dev.mccue.multiplayer.ToClientMessage;

public record ToClientChat(ClientId sender, String value) implements ToClientMessage {
}
