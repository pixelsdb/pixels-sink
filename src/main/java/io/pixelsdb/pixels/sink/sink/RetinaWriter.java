/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.sink;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class RetinaWriter implements PixelsSinkWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    private static final IndexService indexService = new IndexService();



    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    // TODO(LiZinuo): 使用RetinaService替换
    final ManagedChannel channel;
    final RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub blockingStub;

    RetinaWriter(PixelsSinkConfig config) {

        // TODO(LiZinuo): 使用RetinaService替换
        this.channel = ManagedChannelBuilder.forAddress(
                        config.getSinkRemoteHost(),
                        config.getRemotePort()
                )
                .usePlaintext()
                .build();
        this.blockingStub = RetinaWorkerServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void flush() {
    }

    //TODO: Move this method to somewhere else
    public static RetinaProto.Value convertValue(Object value) {
        RetinaProto.Value.Builder builder = RetinaProto.Value.newBuilder();
        if (value instanceof byte[]) {
            builder.setStringValueBytes(ByteString.copyFrom((byte[]) value));
        } else if (value instanceof String) {
            builder.setStringValue((String) value);
        } else if (value instanceof Integer) {
            builder.setIntegerValue((Integer) value);
        } else if (value instanceof Long) {
            builder.setLongValue((Long) value);
        } else if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
        }
        return builder.build();
    }

    @Override
    @Deprecated
    public boolean write(Map<String, Object> message) throws IOException {
        if (isClosed.get()) {
            throw new IOException("Writer is already closed");
        }
        writeLock.lock();
//        SinkProto.WriteRequest request = SinkProto.WriteRequest.newBuilder().build();
//        SinkProto.WriteResponse response = blockingStub.writeData(request);
        return false;
    }

    @Override
    public boolean write(RowChangeEvent event) {
        if (isClosed.get()) {
            LOGGER.warn("Attempted to write to closed writer");
            return false;
        }

        writeLock.lock();
        try {
            switch (event.getOp()) {
                case INSERT:
                case SNAPSHOT:
                    return sendInsertRequest(event);
                case UPDATE:
                    return sendUpdateRequest(event);
                case DELETE:
                    return sendDeleteRequest(event);
                case UNKNOWN:
                case UNRECOGNIZED:
                    break;
            }
        } catch (StatusRuntimeException e) {
            LOGGER.error("gRPC write failed for event {}: {}", event.getTransaction().getId(), e.getStatus());
            return false;
        } finally {
            writeLock.unlock();
        }
        // TODO: error handle
        return false;
    }

    private boolean sendInsertRequest(RowChangeEvent event) {
        // TODO 这里需要RetinaService提供包装接口
        RetinaProto.InsertRecordResponse insertRecordResponse = blockingStub.insertRecord(getInsertRecordRequest(event));
        return insertRecordResponse.getHeader().getErrorCode() == 0;
    }

    private RetinaProto.InsertRecordRequest getInsertRecordRequest(RowChangeEvent event) {
        // Step1. Serialize Row Data
        RetinaProto.InsertRecordRequest.Builder builder = RetinaProto.InsertRecordRequest.newBuilder();
        event.getAfterData().forEach((col, value) ->
                builder.addColValues(value.toString()));

        // Step2. Build Insert Request
        return builder
                .setTable(event.getTable())
                .setTimestamp(event.getTimeStamp()).build();
    }

    private boolean sendUpdateRequest(RowChangeEvent event) {
        RetinaProto.UpdateRecordResponse updateRecordResponse = blockingStub.updateRecord(getUpdateRecordRequest(event));
        return updateRecordResponse.getHeader().getErrorCode() == 0;
    }

    private RetinaProto.UpdateRecordRequest getUpdateRecordRequest(RowChangeEvent event) {
        // Step1. Look up unique index to find row location
        IndexProto.RowLocation rowLocation = indexService.lookupUniqueIndex(event.getIndexKey());

        // Step2. Serialize New Data
        RetinaProto.UpdateRecordRequest.Builder builder = RetinaProto.UpdateRecordRequest.newBuilder();
        event.getAfterData().forEach((col, value) ->
                builder.addNewValues(convertValue(value)));

        // Step3. Build Update Request
        return builder
                .setTableName(event.getTable())
                .setSchemaName(event.getSchemaName())
                .setTimestamp(event.getTimeStamp())
                .setPrimaryKey(event.getAfterPk())
                .setPkId(event.getPkId())
                .build();
    }

    private boolean sendDeleteRequest(RowChangeEvent event) {
        RetinaProto.UpdateRecordResponse updateRecordResponse = blockingStub.updateRecord(getUpdateRecordRequest(event));
        return updateRecordResponse.getHeader().getErrorCode() == 0;
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

    private RetinaProto.DeleteRecordRequest getDeleteRecordRequest(RowChangeEvent event) {
        // Step1. Look up unique index to find row location
        IndexProto.RowLocation rowLocation = indexService.lookupUniqueIndex(event.getIndexKey());

        // Step2. Build Delete Request
        RetinaProto.DeleteRecordRequest.Builder builder = RetinaProto.DeleteRecordRequest.newBuilder();
        return builder
                .setTableName(event.getTable())
                .setSchemaName(event.getSchemaName())
                .setTimestamp(event.getTimeStamp())
                .setPrimaryKey(event.getBeforePk())
                .setPkId(event.getPkId())
                .build();
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
