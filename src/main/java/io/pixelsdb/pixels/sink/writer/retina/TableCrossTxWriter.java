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


import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer.retina
 * @className: TableCrossTxWriter
 * @author: AntiO2
 * @date: 2025/9/27 09:36
 */
public class TableCrossTxWriter extends TableWriter
{
    protected final ReentrantLock writeLock = new ReentrantLock();
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableCrossTxWriter.class);
    private final int flushBatchSize;
    private final InFlightControlManager inFlightControlManager;

    public TableCrossTxWriter(String t, int bucketId)
    {
        super(t, bucketId);
        flushBatchSize = config.getFlushBatchSize();
        inFlightControlManager = InFlightControlManager.getInstance();
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batch)
    {
        writeLock.lock();
        try
        {
            String txId = null;
            List<RowChangeEvent> smallBatch = null;
            List<String> txIds = new ArrayList<>();
            List<String> fullTableName = new ArrayList<>();
            List<RetinaProto.TableUpdateData> tableUpdateData = new LinkedList<>();
            List<Integer> tableUpdateCount = new ArrayList<>();
            for (RowChangeEvent event : batch)
            {
                String currTxId = event.getTransaction().getId();
                if (!currTxId.equals(txId))
                {
                    if (smallBatch != null && !smallBatch.isEmpty())
                    {
                        RetinaProto.TableUpdateData update =
                                buildTableUpdateDataFromBatch(txId, smallBatch);
                        if (update == null)
                        {
                            continue;
                        }
                        tableUpdateData.add(update);
                        tableUpdateCount.add(smallBatch.size());
                    }
                    txIds.add(currTxId);
                    fullTableName.add(event.getFullTableName());
                    txId = currTxId;
                    smallBatch = new LinkedList<>();
                }
                smallBatch.add(event);
            }

            if (smallBatch != null)
            {
                RetinaProto.TableUpdateData update =
                        buildTableUpdateDataFromBatch(txId, smallBatch);
                if (update != null)
                {
                    tableUpdateData.add(update);
                    tableUpdateCount.add(smallBatch.size());
                }
            }

            // flushRateLimiter.acquire(batch.size());
            long txStartTime = System.currentTimeMillis();

            int rowCount = batch.size();
            inFlightControlManager.acquire(1);
            LOGGER.debug("Sending {} rows of table {} to retina, txIds={}", rowCount, tableName, txIds);
            CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecordResponseCompletableFuture =
                    delegate.writeBatchAsync(batch.get(0).getSchemaName(), tableUpdateData);
            if (updateRecordResponseCompletableFuture == null)
            {
                inFlightControlManager.release(1);
                LOGGER.error("Failed to submit {} rows of table {} to retina, txIds={}", rowCount, tableName, txIds);
                failCtxs(txIds);
                return;
            }

            updateRecordResponseCompletableFuture.whenComplete(
                    (resp, err) ->
                    {
                        inFlightControlManager.release(1);
                        if (err != null)
                        {
                            LOGGER.error("Retina write failed for {} rows of table {}, txIds={}",
                                    rowCount, tableName, txIds, err);
                            failCtxs(txIds);
                        } else if (resp.getHeader().getErrorCode() != 0)
                        {
                            LOGGER.error("Retina rejected {} rows of table {}, txIds={}, errorCode={}, errorMsg={}",
                                    rowCount, tableName, txIds, resp.getHeader().getErrorCode(),
                                    resp.getHeader().getErrorMsg());
                            failCtxs(txIds);
                        } else
                        {
                            long txEndTime = System.currentTimeMillis();
                            if (freshnessLevel.equals("row"))
                            {
                                metricsFacade.recordFreshness(txEndTime - txStartTime);
                            }
                            updateCtxCounters(txIds, fullTableName, tableUpdateCount);
                            LOGGER.debug("Retina acked {} rows of table {}, txIds={}", rowCount, tableName, txIds);
                        }
                    }
            );
        } finally
        {
            writeLock.unlock();
        }
    }

    private void failCtxs(List<String> txIds)
    {
        for (String writeTxId : txIds)
        {
            SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(writeTxId);
            if (sinkContext != null)
            {
                sinkContext.setFailed(true);
            }
        }
    }

    private void updateCtxCounters(List<String> txIds, List<String> fullTableName, List<Integer> tableUpdateCount)
    {
        writeLock.lock();
        for (int i = 0; i < txIds.size(); i++)
        {
            metricsFacade.recordRowEvent(tableUpdateCount.get(i));
            String writeTxId = txIds.get(i);
            SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(writeTxId);

            try
            {
                sinkContext.tableCounterLock.lock();
                sinkContext.recordTimestamp(fullTableName.get(i), LocalDateTime.now());
                sinkContext.updateCounter(fullTableName.get(i), tableUpdateCount.get(i));
                if (sinkContext.isCompleted())
                {
                    SinkContextManager.getInstance().endTransaction(sinkContext);
                }
            } finally
            {
                sinkContext.tableCounterLock.unlock();
            }
        }
        writeLock.unlock();
    }

    protected RetinaProto.TableUpdateData buildTableUpdateDataFromBatch(
            String txId, List<RowChangeEvent> smallBatch)
    {
        SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(txId);
        if (sinkContext == null)
        {
            return null;
        }
        try
        {
            sinkContext.getLock().lock();
            while (sinkContext.getPixelsTransCtx() == null)
            {
                LOGGER.warn("Wait for tx to begin trans: {}", txId); // CODE SHOULD NOT REACH HERE
                sinkContext.getCond().await();
            }
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            sinkContext.getLock().unlock();
        }
        try
        {
            return RetinaPayloadBuilder.buildTableUpdateData(
                    tableName, sinkContext.getTimestamp(), smallBatch);
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
    }

    @Override
    protected boolean needFlush()
    {
        return buffer.size() >= flushBatchSize;
    }
}
