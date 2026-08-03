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
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.util.BlockingBoundedMap;
import io.pixelsdb.pixels.sink.util.DataTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkContextManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkContextManager.class);
    private static volatile SinkContextManager instance;
    private final BlockingBoundedMap<String, SinkContext> activeTxContexts;
    // private final ConcurrentMap<String, SinkContext> activeTxContexts = new ConcurrentHashMap<>(10000);
    private final TransactionProxy transactionProxy = TransactionProxy.Instance();
    private final CommitMethod commitMethod;
    private final String freshnessLevel;
    private final RetinaBucketDispatcher retinaBucketDispatcher;

    private SinkContextManager()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        if (config.getCommitMethod().equals("sync"))
        {
            this.commitMethod = CommitMethod.Sync;
        } else
        {
            this.commitMethod = CommitMethod.Async;
        }
        this.freshnessLevel = config.getSinkMonitorFreshnessLevel();
        this.retinaBucketDispatcher = new RetinaBucketDispatcher();
        this.activeTxContexts = new BlockingBoundedMap<>(config.getRetinaTransLimit());
    }

    public static SinkContextManager getInstance()
    {
        if (instance == null)
        {
            synchronized (SinkContextManager.class)
            {
                if (instance == null)
                {
                    instance = new SinkContextManager();
                }
            }
        }
        return instance;
    }

    protected SinkContext getActiveTxContext(RowChangeEvent event, AtomicBoolean canWrite)
    {
        String txId = event.getTransaction().getId();
        return activeTxContexts.compute(txId, (sourceTxId, sinkContext) ->
        {
            if (sinkContext == null)
            {
                LOGGER.trace("Allocate new tx {}\torder:{}", sourceTxId, event.getTransaction().getTotalOrder());
                SinkContext newSinkContext = new SinkContext(sourceTxId);
                newSinkContext.bufferOrphanedEvent(event);
                return newSinkContext;
            } else
            {
                try
                {
                    sinkContext.getLock().lock();
                    if (sinkContext.getPixelsTransCtx() == null)
                    {
                        LOGGER.trace("Buffer in tx {}\torder:{}", sourceTxId, event.getTransaction().getTotalOrder());
                        canWrite.set(false);
                        sinkContext.bufferOrphanedEvent(event);
                        return sinkContext;
                    }
                    LOGGER.trace("Ready to write in tx {}\torder:{}", sourceTxId, event.getTransaction().getTotalOrder());
                    canWrite.set(true);
                    return sinkContext;
                } finally
                {
                    sinkContext.getCond().signalAll();
                    sinkContext.getLock().unlock();
                }

            }
        });
    }

    protected void startTransSync(String sourceTxId)
    {
        LOGGER.trace("Start trans {}", sourceTxId);
        TransContext pixelsTransContext = transactionProxy.getNewTransContext(sourceTxId);
        activeTxContexts.compute(
                sourceTxId,
                (k, oldCtx) ->
                {
                    if (oldCtx == null)
                    {
                        LOGGER.trace("Start trans {} without buffered events", sourceTxId);
                        return new SinkContext(sourceTxId, pixelsTransContext);
                    } else
                    {
                        oldCtx.getLock().lock();
                        try
                        {
                            if (oldCtx.getPixelsTransCtx() != null)
                            {
                                LOGGER.warn("Previous tx {} has been released, maybe due to loop process", sourceTxId);
                                oldCtx.tableCounters = new ConcurrentHashMap<>();
                            }
                            LOGGER.trace("Start trans with buffered events {}", sourceTxId);
                            oldCtx.setPixelsTransCtx(pixelsTransContext);
                            handleOrphanEvents(oldCtx);
                            oldCtx.getCond().signalAll();
                        } catch (SinkException e)
                        {
                            throw new RuntimeException(e);
                        } finally
                        {
                            oldCtx.getLock().unlock();
                        }
                        return oldCtx;
                    }
                }
        );
        LOGGER.trace("Begin Tx Sync: {}", sourceTxId);
    }

    void processTxCommit(SinkProto.TransactionMetadata txEnd)
    {
        String txId = txEnd.getId();
        SinkContext ctx = getSinkContext(txId);
        if (ctx == null)
        {
            throw new RuntimeException("Sink Context is null");
        }

        try
        {
            ctx.tableCounterLock.lock();
            ctx.setEndTx(txEnd);
            long startTs = System.currentTimeMillis();
            if (ctx.isCompleted())
            {
                endTransaction(ctx);
            }
        } finally
        {
            ctx.tableCounterLock.unlock();
        }
    }

    void endTransaction(SinkContext ctx)
    {
        String txId = ctx.getSourceTxId();
        removeSinkContext(txId);
        boolean failed = ctx.isFailed();
        if (!failed)
        {
            LOGGER.trace("Committed transaction: {}, Pixels Trans is {}", txId, ctx.getPixelsTransCtx().getTransId());
            switch (commitMethod)
            {
                case Sync ->
                {
                    transactionProxy.commitTransSync(ctx);
                }
                case Async ->
                {
                    transactionProxy.commitTransAsync(ctx);
                }
            }
            if (freshnessLevel.equals("embed"))
            {
                for (String table : ctx.getTableCounters().keySet())
                {
                    String tableName = DataTransform.extractTableName(table);
                    FreshnessClient.getInstance().addMonitoredTable(tableName);
                }
            }
        } else
        {
            LOGGER.info("Abort transaction: {}", txId);
            CompletableFuture.runAsync(() ->
            {
                transactionProxy.rollbackTrans(ctx.getPixelsTransCtx());
            }).whenComplete((v, ex) ->
            {
                if (ex != null)
                {
                    LOGGER.error("Rollback failed", ex);
                }
            });
        }
    }

    private void handleOrphanEvents(SinkContext ctx) throws SinkException
    {
        Queue<RowChangeEvent> buffered = ctx.getOrphanEvent();
        ctx.setOrphanEvent(null);
        if (buffered != null)
        {
            LOGGER.trace("Handle Orphan Events in {}", ctx.sourceTxId);
            for (RowChangeEvent event : buffered)
            {
                writeRowChangeEvent(ctx, event);
            }
        }
    }

    protected void writeRowChangeEvent(SinkContext ctx, RowChangeEvent event) throws SinkException
    {
        if (ctx != null)
        {
            event.setTimeStamp(ctx.getTimestamp());
        }
        retinaBucketDispatcher.writeRowChangeEvent(event, ctx);
    }

    protected SinkContext getSinkContext(String txId)
    {
        return activeTxContexts.get(txId);
    }

    protected void removeSinkContext(String txId)
    {
        activeTxContexts.remove(txId);
    }

    protected void writeRandomRowChangeEvent(String randomId, RowChangeEvent event) throws SinkException
    {
        writeRowChangeEvent(getSinkContext(randomId), event);
    }

    public int getActiveTxnsNum()
    {
        return activeTxContexts.size();
    }

    public String findMinActiveTx()
    {
        Comparator<String> customComparator = (key1, key2) ->
        {
            try
            {
                String[] parts1 = key1.split("_");
                int int1 = Integer.parseInt(parts1[0]);
                int loopId1 = Integer.parseInt(parts1[1]);

                String[] parts2 = key2.split("_");
                int int2 = Integer.parseInt(parts2[0]);
                int loopId2 = Integer.parseInt(parts2[1]);

                int loopIdComparison = Integer.compare(loopId1, loopId2);
                if (loopIdComparison != 0)
                {
                    return loopIdComparison;
                }
                return Integer.compare(int1, int2);
            } catch (Exception e)
            {
                System.err.println("Key format error for comparison: " + e.getMessage());
                return 0;
            }
        };

        Optional<String> min = activeTxContexts.keySet().stream().min(customComparator);
        return min.orElse("None");
    }

    private enum CommitMethod
    {
        Sync,
        Async
    }
}
