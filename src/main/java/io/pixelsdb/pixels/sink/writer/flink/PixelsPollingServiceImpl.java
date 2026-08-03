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
 */
package io.pixelsdb.pixels.sink.writer.flink;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PixelsPollingServiceImpl extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsPollingServiceImpl.class);
    private final FlinkPollingWriter writer;
    private final int pollBatchSize;
    private final long pollTimeoutMs;
    private final FlushRateLimiter flushRateLimiter;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final String freshnessLevel;

    public PixelsPollingServiceImpl(FlinkPollingWriter writer)
    {
        if (writer == null)
        {
            throw new IllegalArgumentException("FlinkPollingWriter cannot be null.");
        }
        this.writer = writer;
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.pollBatchSize = config.getCommitBatchSize();
        this.pollTimeoutMs = config.getTimeoutMs();
        this.flushRateLimiter = FlushRateLimiterFactory.getNewInstance();
        this.freshnessLevel = config.getSinkMonitorFreshnessLevel();
        LOGGER.info("PixelsPollingServiceImpl initialized. Using 'sink.commit.batch.size' for pollBatchSize ({}) " +
                        "and 'sink.timeout.ms' for pollTimeoutMs ({}).",
                this.pollBatchSize, this.pollTimeoutMs);
    }

    @Override
    public void pollEvents(SinkProto.PollRequest request, StreamObserver<SinkProto.PollResponse> responseObserver)
    {
        SchemaTableName schemaTableName = new SchemaTableName(request.getSchemaName(), request.getTableName());
        LOGGER.debug("Received poll request for table '{}'", schemaTableName);
        List<SinkProto.RowRecord> records = new ArrayList<>(pollBatchSize);

        try
        {
            for (int bucketId : request.getBucketsList())
            {
                if (records.size() >= pollBatchSize)
                {
                    break;
                }

                List<SinkProto.RowRecord> polled =
                        writer.pollRecords(
                                schemaTableName,
                                bucketId,
                                pollBatchSize - records.size(),
                                0,
                                TimeUnit.MILLISECONDS
                        );

                if (polled != null && !polled.isEmpty())
                {
                    records.addAll(polled);
                }
            }

            SinkProto.PollResponse.Builder responseBuilder = SinkProto.PollResponse.newBuilder();
            if (records != null && !records.isEmpty())
            {
                responseBuilder.addAllRecords(records);
                metricsFacade.recordRowEvent(records.size());
                metricsFacade.recordTransaction();
//                this.flushRateLimiter.acquire(records.size());

                if (freshnessLevel.equals("embed"))
                {
                    FreshnessClient.getInstance().addMonitoredTable(request.getTableName());
                }
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            LOGGER.error("Polling thread was interrupted for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Server polling was interrupted")
                    .asRuntimeException());
        } catch (Exception e)
        {
            LOGGER.error("An unexpected error occurred while polling for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.UNKNOWN
                    .withDescription("An unexpected error occurred: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}