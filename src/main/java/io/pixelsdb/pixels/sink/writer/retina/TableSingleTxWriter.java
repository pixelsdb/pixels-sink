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

import java.util.ArrayList;
import java.util.List;

public class TableSingleTxWriter extends TableWriter
{
    private static final long TX_TIMEOUT_MS = 3000;
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableSingleTxWriter.class);

    public TableSingleTxWriter(String tableName, int bucketId)
    {
        super(tableName, bucketId);
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batchToFlush)
    {
        List<RowChangeEvent> batch;
        String txId;
        SinkContext sinkContext = null;
        bufferLock.lock();
        try
        {
            if (buffer.isEmpty() || currentTxId == null)
            {
                return;
            }
            txId = currentTxId;
            currentTxId = null;

            sinkContext = SinkContextManager.getInstance().getSinkContext(txId);
            sinkContext.getLock().lock();
            try
            {
                while (sinkContext.getPixelsTransCtx() == null)
                {
                    LOGGER.warn("Wait for prev tx to begin trans: {}", txId);
                    sinkContext.getCond().await();
                }
            } finally
            {
                sinkContext.getLock().unlock();
            }

            // Swap buffers quickly under lock
            batch = buffer;
            buffer = new ArrayList<>();
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            bufferLock.unlock();
        }

        RowChangeEvent event1 = batch.get(0);
        try
        {
            List<RetinaProto.TableUpdateData> tableUpdateData = List.of(
                    RetinaPayloadBuilder.buildTableUpdateData(tableName, 0L, batch));
            delegate.writeTrans(event1.getSchemaName(), tableUpdateData);
            sinkContext.updateCounter(fullTableName, batch.size());
            // ---- Outside lock: build proto and write ----
            LOGGER.info("Flushing {} events for table {} txId={}", batch.size(), fullTableName, txId);
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
    }

    @Override
    protected boolean needFlush()
    {
        if (currentTxId == null || !currentTxId.equals(txId))
        {
            return !buffer.isEmpty();
        }
        return false;
    }
}
