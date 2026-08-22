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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.prometheus.client.Summary;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TableSingleRecordWriter extends TableCrossTxWriter
{
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableSingleRecordWriter.class);
    private final TransactionProxy transactionProxy;

    public TableSingleRecordWriter(String t, int bucketId)
    {
        super(t, bucketId);
        this.transactionProxy = TransactionProxy.Instance();
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batch)
    {
        TransContext pixelsTransContext = transactionProxy.getNewTransContext(tableName);
        writeLock.lock();
        try
        {
            List<RetinaProto.TableUpdateData> tableUpdateData = new LinkedList<>();
            // Timestamp + IndexKey binding happen inside RetinaPayloadBuilder.
            RetinaProto.TableUpdateData update =
                    buildTableUpdateDataFromBatch(pixelsTransContext, batch);
            if (update != null)
            {
                tableUpdateData.add(update);
            }

            // flushRateLimiter.acquire(batch.size());
            long txStartTime = System.currentTimeMillis();

            final Summary.Timer startWriteLatencyTimer = metricsFacade.startWriteLatencyTimer(tableName);
            CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecordResponseCompletableFuture = delegate.writeBatchAsync(batch.get(0).getSchemaName(), tableUpdateData);

            updateRecordResponseCompletableFuture.thenAccept(
                    resp ->
                    {
                        if (freshness_embed)
                        {
                            FreshnessClient.getInstance().addMonitoredTable(tableName);
                        }

                        if (resp.getHeader().getErrorCode() != 0)
                        {
                            transactionProxy.rollbackTrans(pixelsTransContext);
                        } else
                        {
                            metricsFacade.recordRowEvent(batch.size());
                            long txEndTime = System.currentTimeMillis();
                            if (freshnessLevel.equals("row"))
                            {
                                metricsFacade.recordFreshness(txEndTime - txStartTime);
                            }
                            transactionProxy.commitTrans(pixelsTransContext);
                            if (startWriteLatencyTimer != null)
                            {
                                startWriteLatencyTimer.observeDuration();
                            }
                        }
                    }
            );
        } finally
        {
            writeLock.unlock();
        }
    }

    protected RetinaProto.TableUpdateData buildTableUpdateDataFromBatch(
            TransContext transContext, List<RowChangeEvent> smallBatch)
    {
        try
        {
            return RetinaPayloadBuilder.buildTableUpdateData(
                    tableName, transContext.getTimestamp(), smallBatch);
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
    }
}
