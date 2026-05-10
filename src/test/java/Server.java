public class Server {
    static void main() throws InterruptedException {
        try (var server = new ChatServer(9000).start()) {
            server.join();
        }
    }
}
