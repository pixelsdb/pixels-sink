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

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaWriter implements PixelsSinkWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final SinkContextManager sinkContextManager;
    private final TransactionMode transactionMode;

    public RetinaWriter()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.sinkContextManager = SinkContextManager.getInstance();
        this.transactionMode = config.getTransactionMode();
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata txMeta)
    {
        if (transactionMode.equals(TransactionMode.RECORD))
        {
            return true;
        }

        try
        {
            if (txMeta.getStatus() == SinkProto.TransactionStatus.BEGIN)
            {
                handleTxBegin(txMeta);
            } else if (txMeta.getStatus() == SinkProto.TransactionStatus.END)
            {
                handleTxEnd(txMeta);
            }
        } catch (SinkException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean writeRow(RowChangeEvent event)
    {
        try
        {
            if (event == null)
            {
                return false;
            }

            metricsFacade.recordRowChange(event.getTable(), event.getOp());
            if (event.getTransaction() == null || event.getTransaction().getId().isEmpty())
            {
                handleNonTxEvent(event);
                return true;
            }


            String table = event.getFullTableName();

            long collectionOrder = event.getTransaction().getDataCollectionOrder();
            long totalOrder = event.getTransaction().getTotalOrder();
            if (transactionMode.equals(TransactionMode.RECORD))
            {
                sinkContextManager.writeRowChangeEvent(null, event);
            } else
            {
                AtomicBoolean canWrite = new AtomicBoolean(false);
                SinkContext ctx = sinkContextManager.getActiveTxContext(event, canWrite);

                if (canWrite.get())
                {
                    sinkContextManager.writeRowChangeEvent(ctx, event);
                }
            }
        } catch (SinkException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        }

        return true;
    }

    private void handleTxBegin(SinkProto.TransactionMetadata txBegin) throws SinkException
    {
        // startTrans(txBegin.getId()).get();
        try
        {
            // flushRateLimiter.acquire(1);
            startTransSync(txBegin.getId());
        } catch (SinkException e)
        {
            throw new SinkException("Failed to start trans", e);
        }

    }

    private void startTransSync(String sourceTxId) throws SinkException
    {
        sinkContextManager.startTransSync(sourceTxId);
    }

    private void handleTxEnd(SinkProto.TransactionMetadata txEnd)
    {
        sinkContextManager.processTxCommit(txEnd);
    }

    private void handleNonTxEvent(RowChangeEvent event) throws SinkException
    {
        // virtual tx
        String randomId = Long.toString(System.currentTimeMillis()) + RandomUtils.nextLong();
        writeTrans(buildBeginTransactionMetadata(randomId));
        sinkContextManager.writeRandomRowChangeEvent(randomId, event);
        writeTrans(buildEndTransactionMetadata(event.getFullTableName(), randomId));
    }

    public void shutdown()
    {
        dispatchExecutor.shutdown();
        timeoutScheduler.shutdown();
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public void flush()
    {

    }

    private SinkProto.TransactionMetadata buildBeginTransactionMetadata(String id)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setStatus(SinkProto.TransactionStatus.BEGIN)
                .setId(id);
        return builder.build();
    }

    private SinkProto.TransactionMetadata buildEndTransactionMetadata(String fullTableName, String id)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setStatus(SinkProto.TransactionStatus.END)
                .setId(id)
                .setEventCount(1L);

        SinkProto.DataCollection.Builder dataCollectionBuilder = SinkProto.DataCollection.newBuilder();
        dataCollectionBuilder.setDataCollection(fullTableName)
                .setEventCount(1L);
        builder.addDataCollections(dataCollectionBuilder);
        return builder.build();
    }
}
