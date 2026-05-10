package dev.mccue.multiplayer;

public final class RunningServer implements AutoCloseable {
    private final Runnable close;
    private final Thread serverThread;

    RunningServer(Runnable close, Thread serverThread) {
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
