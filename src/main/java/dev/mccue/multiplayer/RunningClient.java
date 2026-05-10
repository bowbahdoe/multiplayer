package dev.mccue.multiplayer;

import java.util.function.Consumer;

public final class RunningClient<ToServer> implements AutoCloseable {
    private final Runnable close;
    private final Consumer<ToServer> toServerConsumer;

    RunningClient(Runnable close, Consumer<ToServer> toServerConsumer) {
        this.close = close;
        this.toServerConsumer = toServerConsumer;
    }

    @Override
    public void close() {
        close.run();
    }

    public void sendToServer(ToServer message) {
        this.toServerConsumer.accept(message);
    }
}
