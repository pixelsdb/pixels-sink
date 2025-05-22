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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.pixelsdb.pixels.sink.util.LatencySimulator;
import io.prometheus.client.Summary;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaWriter implements PixelsSinkWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    private static final IndexService indexService = IndexService.Instance();


    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    // TODO(LiZinuo): 使用RetinaService替换
    final ManagedChannel channel;
    final RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub blockingStub;

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();

    RetinaWriter() {
        if (config.isRpcEnable()) {
            // TODO(LiZinuo): 使用RetinaService替换
            this.channel = ManagedChannelBuilder.forAddress(
                            config.getSinkRemoteHost(),
                            config.getRemotePort()
                    )
                    .usePlaintext()
                    .build();
            this.blockingStub = RetinaWorkerServiceGrpc.newBlockingStub(channel);
        } else {
            channel = null;
            blockingStub = null;
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean write(RowChangeEvent event) {
        if (isClosed.get()) {
            LOGGER.warn("Attempted to write to closed writer");
            return false;
        }

        try {
            if (config.isRpcEnable()) {
                switch (event.getOp()) {
                    case INSERT:
                    case SNAPSHOT:
                        return sendInsertRequest(event);
                    case UPDATE:
                        return sendUpdateRequest(event);
                    case DELETE:
                        return sendDeleteRequest(event);
                    case UNRECOGNIZED:
                        break;
                }
            } else {
                if (event.getOp() != SinkProto.OperationType.INSERT && event.getOp() != SinkProto.OperationType.SNAPSHOT) {
                    Summary.Timer indexLatencyTimer = metricsFacade.startIndexLatencyTimer();
                    LatencySimulator.smartDelay(); // Mock Look Up Index
                    indexLatencyTimer.close();
                }
                Summary.Timer retinaLatencyTimer = metricsFacade.startRetinaLatencyTimer();
                LatencySimulator.smartDelay(); // Call Retina
                retinaLatencyTimer.close();
                return true;
            }

        } catch (StatusRuntimeException e) {
            LOGGER.error("gRPC write failed for event {}: {}", event.getTransaction().getId(), e.getStatus());
            return false;
        } finally {

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
        RetinaProto.RowValue.Builder rowValueBuilder = builder.getRowBuilder();
        event.getAfterData().getValuesList().forEach(value ->
                builder.setRow(rowValueBuilder.addValues(value.getValue())));

        // Step2. Build Insert Request
        return builder
                .setSchema(event.getDb())
                .setTable(event.getTable())
                .setRow(rowValueBuilder.build())
                .setTimestamp(event.getTimeStamp())
                .setTransInfo(getTransinfo(event))
                .build();
    }

    private boolean sendDeleteRequest(RowChangeEvent event) {
        RetinaProto.DeleteRecordResponse deleteRecordResponse = blockingStub.deleteRecord(getDeleteRecordRequest(event));
        return deleteRecordResponse.getHeader().getErrorCode() == 0;
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
                .setRow(builder.getRowBuilder().setRgRowId(rowLocation.getRgRowId()).setFileId(rowLocation.getFileId()).setRgId(rowLocation.getRgId()))
                .setTimestamp(event.getTimeStamp())
                .setTransInfo(getTransinfo(event))
                .build();
    }

    private boolean sendUpdateRequest(RowChangeEvent event) {
        // Delete & Insert
        RetinaProto.DeleteRecordResponse deleteRecordResponse = blockingStub.deleteRecord(getDeleteRecordRequest(event));
        if (deleteRecordResponse.getHeader().getErrorCode() != 0) {
            return false;
        }

        RetinaProto.InsertRecordResponse insertRecordResponse = blockingStub.insertRecord(getInsertRecordRequest(event));
        return insertRecordResponse.getHeader().getErrorCode() == 0;
    }

    private RetinaProto.TransInfo getTransinfo(RowChangeEvent event) {
        return RetinaProto.TransInfo.newBuilder()
                .setOrder(event.getTransaction().getTotalOrder())
                .setTransId(event.getTransaction().getId().hashCode()).build();
    }
}
