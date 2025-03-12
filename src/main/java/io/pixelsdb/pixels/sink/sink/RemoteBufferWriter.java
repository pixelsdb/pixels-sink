package io.pixelsdb.pixels.sink.sink;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.proto.PixelsSinkServiceGrpc;
import io.pixelsdb.pixels.sink.proto.SinkProto;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class RemoteBufferWriter implements PixelsSinkWriter {

    private final ManagedChannel channel;
    private final PixelsSinkServiceGrpc.PixelsSinkServiceBlockingStub blockingStub;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);


    RemoteBufferWriter(PixelsSinkConfig config) {
        this.channel = ManagedChannelBuilder.forAddress(
                        config.getSinkRemoteHost(),
                        config.getRemotePort()
                )
                .build();
        this.blockingStub = PixelsSinkServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void flush() {

    }

    @Override
    public boolean write(Map<String, Object> message) throws IOException {
        if (isClosed.get()) {
            throw new IOException("Writer is already closed");
        }

        writeLock.lock();
        SinkProto.WriteRequest request = convertToRequest(message);
        SinkProto.WriteResponse response = blockingStub.writeData(request);
        return response.getSuccess();
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

    private SinkProto.WriteRequest convertToRequest(Map<String, Object> data) {
        SinkProto.WriteRequest.Builder builder = SinkProto.WriteRequest.newBuilder();
        data.forEach((key, value) -> {
            if (value instanceof byte[]) {
                builder.putData(key, ByteString.copyFrom((byte[]) value));
            } else {
                throw new IllegalArgumentException("Unsupported data type for key: " + key);
            }
        });
        return builder.build();
    }
}
