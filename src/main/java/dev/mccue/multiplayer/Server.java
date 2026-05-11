package dev.mccue.multiplayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class Server<
        ToServer extends ToServerMessage,
        ToClient extends ToClientMessage,
        ClientState
        > {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private final int port;
    private int clientId = 0;
    private final ConcurrentHashMap<ClientId, ConnectedClient<ToClient, ClientState>> clients;
    // I synchronize all onMessage, onConnect, and onDisconnect.
    private final Object lock = new Object();

    private record ConnectedClient<ToClient, ClientState>(
            LinkedBlockingDeque<ToClient> outgoing,
            Thread outgoingThread,
            Thread incomingThread,
            AtomicReference<ClientState> state
    ) {
    }

    public Server(int port) {
        this.port = port;
        this.clients = new ConcurrentHashMap<>();
    }

    private record Msg<ToServer>(ClientId id, ToServer message){}

    public RunningServer<ToClient, ClientState> start() {
        var serverMessageQueue = new LinkedBlockingDeque<Msg<ToServer>>();
        var serverMessageThread = Thread.startVirtualThread(() -> {
            while (true) {
                if (Thread.interrupted()) {
                    break;
                }

                try {
                    var message = serverMessageQueue.take();
                    synchronized (lock) {
                        onMessage(message.id, message.message);
                    }
                } catch (InterruptedException e) {
                    break;
                }

            }
        });
        var serverThread = Thread.startVirtualThread(() -> {
            try (var server = new ServerSocket(port)) {
                while (true) {
                    var socket = server.accept();
                    var id = new ClientId(Integer.toString(clientId++));
                    var q = new LinkedBlockingDeque<ToClient>();
                    var out = new ObjectOutputStream(socket.getOutputStream());
                    var in = new ObjectInputStream(socket.getInputStream());
                    Thread outgoingThread = Thread.startVirtualThread(() -> {
                        while (true) {
                            if (Thread.interrupted()) {
                                break;
                            }
                            try {
                                try {
                                    var message = q.take();
                                    out.writeObject(message);
                                } catch (InterruptedException e) {
                                    LOG.info("Client connection closed. client_id={}", id);
                                    break;
                                }
                            } catch (Exception e) {
                                LOG.error("Error sending client message", e);
                            }
                        }
                    });
                    Thread incomingThread = Thread.startVirtualThread(() -> {
                        while (true) {
                            try {
                                if (Thread.interrupted()) {
                                    break;
                                }

                                var next = (ToServer) in.readObject();
                                serverMessageQueue.add(new Msg<>(id, next));
                            } catch (Exception e) {
                                LOG.error("Error receiving message from client", e);
                                outgoingThread.interrupt();
                                synchronized (lock) {
                                    clients.remove(id);
                                    onDisconnect(id);
                                }
                                break;
                            }
                        }
                    });

                    // We want the client to be in the map before any onMessage calls
                    synchronized (lock) {
                        clients.put(id, new ConnectedClient<>(
                                q,
                                outgoingThread,
                                incomingThread,
                                new AtomicReference<>(null)
                        ));
                        onConnect(id);
                    }
                }
            } catch (ClosedByInterruptException e) {
                clients.forEach((id, connected) -> {
                    LOG.info("Closing client connection. client_id={}", id);
                    connected.outgoingThread.interrupt();
                    connected.incomingThread.interrupt();
                    synchronized (lock) {
                        clients.remove(id);
                        onDisconnect(id);
                    }
                });
                clients.clear();
                LOG.info("Server shutting down");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return new RunningServer<>(this, () -> {
            serverMessageThread.interrupt();
            serverThread.interrupt();
        }, serverThread);
    }

    protected final ClientState getState(ClientId id) {
        var client = clients.get(id);
        if (client == null) {
            return null;
        }

        return client.state.get();
    }

    protected final void setState(ClientId id, ClientState state) {
        var client = clients.get(id);
        if (client == null) {
            return;
        }

        client.state.set(state);
    }

    /// Sends a message to all connected clients.
    protected final void broadcast(ToClient message) {
        clients.forEach((_, connected) -> {
            connected.outgoing.add(message);
        });
    }

    /// Sends a message to a specific connected client
    protected final boolean send(ClientId id, ToClient message) {
        var client = clients.get(id);
        if (client == null) {
            LOG.warn("No connected client with id: {}", id);
            return false;
        }

        client.outgoing.add(message);
        return true;
    }

    protected final List<ClientId> connected() {
        return List.copyOf(clients.keySet());
    }

    protected abstract void onDisconnect(ClientId clientId);


    protected abstract void onConnect(ClientId clientId);


    protected abstract void onMessage(ClientId clientId, ToServer message);

    private static final class BuiltServer<
            ToServer extends ToServerMessage,
            ToClient extends ToClientMessage,
            ClientState
            > extends Server<ToServer, ToClient, ClientState> {
        private final DisconnectHandler<ToClient, ClientState> onDisconnect;
        private final ConnectHandler<ToClient, ClientState> onConnect;
        private final MessageHandler<ToClient, ToServer, ClientState> onMessage;
        private final ServerContext<ToClient, ClientState> ctx;

        public BuiltServer(Builder<ToServer, ToClient, ClientState> builder) {
            super(builder.port);
            this.onDisconnect = builder.onDisconnect;
            this.onConnect = builder.onConnect;
            this.onMessage = builder.onMessage;
            this.ctx = new ServerContext<>(this);
        }

        @Override
        protected void onDisconnect(ClientId clientId) {
            onDisconnect.onDisconnect(ctx, clientId);
        }

        @Override
        protected void onConnect(ClientId clientId) {
            onConnect.onConnect(ctx, clientId);
        }

        @Override
        protected void onMessage(ClientId clientId, ToServer message) {
            onMessage.onMessage(ctx, clientId, message);
        }
    }

    public static <
            ToServer extends ToServerMessage,
            ToClient extends ToClientMessage,
            ClientState
            > Builder<ToServer, ToClient, ClientState> builder(int port) {
        return new Builder<>(port);
    }

    public static final class Builder<
            ToServer extends ToServerMessage,
            ToClient extends ToClientMessage,
            ClientState
            > {
        private String host;
        private int port;
        private DisconnectHandler<ToClient, ClientState> onDisconnect;
        private ConnectHandler<ToClient, ClientState> onConnect;
        private MessageHandler<ToClient, ToServer, ClientState> onMessage;

        private Builder(int port) {
            this.port = port;
            this.onDisconnect = (_, _) -> {};
            this.onConnect = (_, _) -> {};
            this.onMessage = (_, _, _) -> {};
        }

        public Builder<ToServer, ToClient, ClientState> onDisconnect(
                DisconnectHandler<ToClient, ClientState> onDisconnect
        ) {
            this.onDisconnect = Objects.requireNonNull(onDisconnect);
            return this;
        }

        public Builder<ToServer, ToClient, ClientState> onConnect(
                ConnectHandler<ToClient, ClientState> onConnect
        ) {
            this.onConnect = Objects.requireNonNull(onConnect);
            return this;
        }

        public Builder<ToServer, ToClient, ClientState> onMessage(
                MessageHandler<ToClient, ToServer, ClientState> onMessage
        ) {
            this.onMessage = Objects.requireNonNull(onMessage);
            return this;
        }

        public Server<ToServer, ToClient, ClientState> build() {
            return new BuiltServer<>(this);
        }
    }

    @FunctionalInterface
    public interface ConnectHandler<ToClient extends ToClientMessage, ClientState> {
        void onConnect(
                ServerContext<ToClient, ClientState> server,
                ClientId id
        );
    }

    @FunctionalInterface
    public interface DisconnectHandler<ToClient extends ToClientMessage, ClientState> {
        void onDisconnect(
                ServerContext<ToClient, ClientState> server,
                ClientId clientId
        );
    }

    @FunctionalInterface
    public interface MessageHandler<ToClient extends ToClientMessage, ToServer extends ToServerMessage, ClientState> {
        void onMessage(
                ServerContext<ToClient, ClientState> server,
                ClientId id,
                ToServer message
        );
    }

}
