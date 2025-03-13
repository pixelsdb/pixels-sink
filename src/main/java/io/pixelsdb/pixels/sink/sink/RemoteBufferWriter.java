package io.pixelsdb.pixels.sink.sink;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.PixelsSinkServiceGrpc;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class RemoteBufferWriter implements PixelsSinkWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteBufferWriter.class);
    private final ManagedChannel channel;
    private final PixelsSinkServiceGrpc.PixelsSinkServiceBlockingStub blockingStub;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.BUFFER;

    RemoteBufferWriter(PixelsSinkConfig config) {
        this.channel = ManagedChannelBuilder.forAddress(
                        config.getSinkRemoteHost(),
                        config.getRemotePort()
                )
                .usePlaintext()
                .build();
        this.blockingStub = PixelsSinkServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void flush() {
    }

    @Override
    @Deprecated
    public boolean write(Map<String, Object> message) throws IOException {
        if (isClosed.get()) {
            throw new IOException("Writer is already closed");
        }
        writeLock.lock();
        SinkProto.WriteRequest request = SinkProto.WriteRequest.newBuilder().build();
        SinkProto.WriteResponse response = blockingStub.writeData(request);
        return response.getSuccess();
    }

    @Override
    public boolean write(RowChangeEvent event) {
        if (isClosed.get()) {
            LOGGER.warn("Attempted to write to closed writer");
            return false;
        }

        writeLock.lock();
        try {
            SinkProto.WriteRequest request = convertEventToRequest(event);
            SinkProto.WriteResponse response = blockingStub.writeData(request);
            return validateResponse(response, event);
        } catch (StatusRuntimeException e) {
            LOGGER.error("gRPC write failed for event {}: {}", event.getTransaction().getId(), e.getStatus());
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Channel shutdown interrupted", e);
            }
        }
    }

    private SinkProto.WriteRequest convertEventToRequest(RowChangeEvent event) {
        SinkProto.WriteRequest.Builder builder = SinkProto.WriteRequest.newBuilder()
                .setTransactionId(event.getTransaction().getId())
                .setTable(event.getTable())
                .setOperation(convertOperation(event.getOp()))
                .setTimestamp(event.getTimeStampUs());
        serializeData(builder, event);
        return builder.build();
    }

    private SinkProto.OperationType convertOperation(SinkProto.OperationType op) {
        switch (op) {
            case INSERT:
                return SinkProto.OperationType.INSERT;
            case UPDATE:
                return SinkProto.OperationType.UPDATE;
            case DELETE:
                return SinkProto.OperationType.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + op);
        }
    }

    private void serializeData(SinkProto.WriteRequest.Builder builder, RowChangeEvent event) {
        if (event.getBeforeData() != null) {
            event.getBeforeData().forEach((col, value) ->
                    builder.putBeforeData(col, convertValue(value)));
        }

        if (event.getAfterData() != null) {
            event.getAfterData().forEach((col, value) ->
                    builder.putAfterData(col, convertValue(value)));
        }
    }

    private SinkProto.ColumnValue convertValue(Object value) {
        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        if (value instanceof byte[]) {
            builder.setBytesVal(ByteString.copyFrom((byte[]) value));
        } else if (value instanceof String) {
            builder.setStringVal((String) value);
        } else if (value instanceof Number) {
            builder.setNumberVal(value.toString());
        } else if (value instanceof Boolean) {
            builder.setBoolVal((Boolean) value);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
        }
        return builder.build();
    }

    private boolean validateResponse(SinkProto.WriteResponse response, RowChangeEvent event) {
        if (!response.getSuccess()) {
            LOGGER.warn("Write failed for event {}, reason: {}",
                    event.getTransaction().getId(),
                    response.getMessage());
            return false;
        }
        return true;
    }

}
