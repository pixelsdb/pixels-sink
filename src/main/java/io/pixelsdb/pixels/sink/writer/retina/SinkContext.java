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
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SinkContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkContext.class);
    @Getter
    final ReentrantLock lock = new ReentrantLock();
    @Getter
    final Condition cond = lock.newCondition(); // this cond is wait for pixels tx

    @Getter
    final ReentrantLock tableCounterLock = new ReentrantLock();
    @Getter
    final Condition tableCounterCond = tableCounterLock.newCondition();


    @Getter
    final String sourceTxId;
    @Getter
    final AtomicInteger pendingEvents = new AtomicInteger(0);
    @Getter
    final CompletableFuture<Void> completionFuture = new CompletableFuture<>();
    @Getter
    final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final Queue<Pair<String, LocalDateTime>> recordTimes = new ConcurrentLinkedQueue<>();
    @Getter
    Map<String, Long> tableCounters = new ConcurrentHashMap<>();
    @Getter
    @Setter
    Queue<RowChangeEvent> orphanEvent = new ConcurrentLinkedQueue<>();
    @Getter
    @Setter
    SinkProto.TransactionMetadata endTx;
    @Getter
    private TransContext pixelsTransCtx;
    @Setter
    @Getter
    private boolean failed = false;
    @Getter
    @Setter
    private volatile Long startTime = null;

    public SinkContext(String sourceTxId)
    {
        this.sourceTxId = sourceTxId;
        this.pixelsTransCtx = null;
        setCurrStartTime();
    }

    public SinkContext(String sourceTxId, TransContext pixelsTransCtx)
    {
        this.sourceTxId = sourceTxId;
        this.pixelsTransCtx = pixelsTransCtx;
        setCurrStartTime();
    }


    void updateCounter(String table)
    {
        updateCounter(table, 1L);
    }

    public void setPixelsTransCtx(TransContext pixelsTransCtx)
    {
        if (this.pixelsTransCtx != null)
        {
            throw new IllegalStateException("Pixels Trans Context Already Set");
        }
        this.pixelsTransCtx = pixelsTransCtx;
    }

    public void recordTimestamp(String table, LocalDateTime timestamp)
    {
        recordTimes.offer(new Pair<>(table, timestamp));
    }

    public void updateCounter(String table, long count)
    {
        tableCounterLock.lock();
        tableCounters.compute(table, (k, v) ->
                (v == null) ? count : v + count);
        tableCounterCond.signalAll();
        tableCounterLock.unlock();
    }

    public boolean isCompleted()
    {
        try
        {
            tableCounterLock.lock();
            if (endTx == null)
            {
                return false;
            }
            for (SinkProto.DataCollection dataCollection : endTx.getDataCollectionsList())
            {
                Long targetEventCount = tableCounters.get(dataCollection.getDataCollection());
                long target = targetEventCount == null ? 0 : targetEventCount;
                LOGGER.debug("TX {}, Table {}, event count {}, tableCursors {}", endTx.getId(), dataCollection.getDataCollection(), dataCollection.getEventCount(), target);
                if (dataCollection.getEventCount() > target)
                {
                    return false;
                }
            }
            return true;
        } finally
        {
            tableCounterLock.unlock();
        }

    }

    public int getProcessedRowsNum()
    {
        long num = 0;
        try
        {
            tableCounterLock.lock();
            for (Long counter : tableCounters.values())
            {
                num += counter;
            }
        } finally
        {
            tableCounterLock.unlock();
        }
        return (int) num;
    }

    public long getTimestamp()
    {
        if (pixelsTransCtx == null)
        {
            throw new RuntimeException("PixelsTransCtx is NULL");
        }
        return pixelsTransCtx.getTimestamp();
    }

    public void bufferOrphanedEvent(RowChangeEvent event)
    {
        orphanEvent.add(event);
    }

    public void setCurrStartTime()
    {
        if (startTime != null)
        {
            return;
        }

        synchronized (this)
        {
            if (startTime == null)
            {
                startTime = System.currentTimeMillis();
            }
        }
    }
}
