package dev.mccue.multiplayer;

import java.io.Serializable;
import java.util.Objects;

public record ClientId(String value) implements Serializable {
    public ClientId {
        Objects.requireNonNull(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
