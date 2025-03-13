package io.pixelsdb.pixels.sink.pojo.enums;

import io.pixelsdb.pixels.sink.proto.SinkProto;

import java.util.Objects;

public class OperationType {

    public static SinkProto.OperationType fromString(String op) {
        if (Objects.equals(op, "c")) {
            return SinkProto.OperationType.INSERT;
        }
        if (Objects.equals(op, "u")) {
            return SinkProto.OperationType.UPDATE;
        }
        if (Objects.equals(op, "d")) {
            return SinkProto.OperationType.DELETE;
        }

        if (Objects.equals(op, "r")) {
            return SinkProto.OperationType.SNAPSHOT;
        }
        throw new RuntimeException(String.format("Can't convert %s to operation type", op));
    }
}
