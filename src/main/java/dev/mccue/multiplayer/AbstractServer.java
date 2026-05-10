package dev.mccue.multiplayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractServer<
        ToServer extends ToServerMessage,
        ToClient extends ToClientMessage,
        ClientState
        > {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

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

    public AbstractServer(int port) {
        this.port = port;
        this.clients = new ConcurrentHashMap<>();
    }

    private record Msg<ToServer>(ClientId id, ToServer message){}

    public RunningServer start() {
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
                        onDisconnect(id);
                    }
                });
                clients.clear();
                LOG.info("Server shutting down");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return new RunningServer(() -> {
            serverMessageThread.interrupt();
            serverThread.interrupt();
        }, serverThread);
    }

    public final ClientState getState(ClientId id) {
        var client = clients.get(id);
        if (client == null) {
            return null;
        }

        return client.state.get();
    }

    public final void setState(ClientId id, ClientState state) {
        var client = clients.get(id);
        if (client == null) {
            return;
        }

        client.state.set(state);
    }

    /// Sends a message to all connected clients.
    public final void broadcast(ToClient message) {
        clients.forEach((_, connected) -> {
            connected.outgoing.add(message);
        });
    }

    /// Sends a message to a specific connected client
    public final boolean send(ClientId id, ToClient message) {
        var client = clients.get(id);
        if (client == null) {
            LOG.warn("No connected client with id: {}", id);
            return false;
        }

        client.outgoing.add(message);
        return true;
    }

    protected abstract void onDisconnect(ClientId clientId);


    protected abstract void onConnect(ClientId clientId);


    protected abstract void onMessage(ClientId clientId, ToServer message);
}
