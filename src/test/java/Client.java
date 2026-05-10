public class Client {
    static void main() {
        try (var client = new ChatClient("0.0.0.0", 9000).start()) {
            while (true) {
                var msg = IO.readln();
                client.sendToServer(new ToServerChat(msg));
            }
        }
    }
}
