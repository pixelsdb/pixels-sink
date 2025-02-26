package io.pixelsdb.pixels.sink.pojo.enums;

import java.util.Objects;

public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    SNAPSHOT,
    UNKNOWN;

    public static OperationType fromString(String op) {
        if (Objects.equals(op, "c")) {
            return INSERT;
        }
        if (Objects.equals(op, "u")) {
            return UPDATE;
        }
        if (Objects.equals(op, "d")) {
            return DELETE;
        }

        if (Objects.equals(op, "r")) {
            return SNAPSHOT;
        }
        throw new RuntimeException(String.format("Can't convert %s to operation type", op));
    }
}
