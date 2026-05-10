import dev.mccue.multiplayer.ToServerMessage;

public record ToServerChat(String value) implements ToServerMessage {
}
