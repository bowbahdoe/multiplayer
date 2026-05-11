package dev.mccue.multiplayer;

import java.util.List;

public final class RunningServer<ToClient extends ToClientMessage, ClientState>
        extends ServerContext
        implements AutoCloseable {
    private final Runnable close;
    private final Thread serverThread;

    RunningServer(Server<?, ToClient, ClientState> server, Runnable close, Thread serverThread) {
        super(server);
        this.close = close;
        this.serverThread = serverThread;
    }

    @Override
    public void close() {
        close.run();
    }

    public void join() throws InterruptedException {
        serverThread.join();
    }
}
