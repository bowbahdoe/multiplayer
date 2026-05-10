package dev.mccue.multiplayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public abstract class AbstractClient<
        ToServer extends ToServerMessage,
        ToClient extends ToClientMessage
        > {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

    private final Runnable start;
    private final Runnable stop;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private Thread backgroundThread;

    public AbstractClient(String host, int port) {
        var latch = new CountDownLatch(1);
        this.stop = latch::countDown;
        Thread.startVirtualThread(() -> {
            try {
                latch.await();
                backgroundThread.interrupt();
                socket.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        this.start = () -> {
            try {
                this.socket = new Socket(host, port);
                this.out = new ObjectOutputStream(socket.getOutputStream());
                this.in = new ObjectInputStream(socket.getInputStream());

                this.backgroundThread = Thread.startVirtualThread(() -> {
                    while (true) {
                        LOG.trace("Waiting for a Message");
                        if (Thread.interrupted()) {
                            break;
                        }
                        try {
                            Object o = this.in.readObject();
                            LOG.trace("Got Message: {}", o);
                            onMessage((ToClient) o, this::sendToServer);
                        } catch (EOFException e) {
                            break;
                        } catch (UncheckedIOException e) {
                            LOG.error("Error handling message", e.getCause());
                        } catch (Exception e) {
                            LOG.error("Error handling message", e);
                        }
                    }

                    this.stop.run();
                    onDisconnect();
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public RunningClient<ToServer> start() {
        this.start.run();
        return new RunningClient<>(this.stop, this::sendToServer);
    }

    public abstract void onMessage(ToClient message, Consumer<ToServer> sendToServer);

    public abstract void onDisconnect();

    private void sendToServer(ToServer toServer) {
        if (out == null) {
            throw new IllegalStateException("Server not started?");
        }
        try {
            out.writeObject((ToServer) toServer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
