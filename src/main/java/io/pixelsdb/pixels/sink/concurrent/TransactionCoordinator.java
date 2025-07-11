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

package io.pixelsdb.pixels.sink.concurrent;

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.pixelsdb.pixels.sink.sink.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.sink.PixelsSinkWriterFactory;
import io.pixelsdb.pixels.sink.util.LatencySimulator;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TransactionCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinator.class);
    public static final int INITIAL_CAPACITY = 11;
    private final PixelsSinkWriter writer;

    final ConcurrentMap<String, SinkContext> activeTxContexts = new ConcurrentHashMap<>();
    final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();
    private final ExecutorService transactionExecutor = Executors.newCachedThreadPool();
    private final ConcurrentMap<String, List<RowChangeEvent>> orphanedEvents = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PriorityBlockingQueue<OrderedEvent>> orderedBuffers = new ConcurrentHashMap<>();
    // private final BlockingQueue<RowChangeEvent> nonTxQueue = new LinkedBlockingQueue<>();
    private long TX_TIMEOUT_MS = PixelsSinkConfigFactory.getInstance().getTransactionTimeout();
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final TransactionManager transactionManager = TransactionManager.Instance();
    private final TransService transService;

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();


    TransactionCoordinator() {
        try {
            this.writer = PixelsSinkWriterFactory.getWriter();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        transService = TransService.Instance();
        // startDispatchWorker();
        startTimeoutChecker();
    }

    public void processTransactionEvent(SinkProto.TransactionMetadata txMeta) {
        if (txMeta.getStatus() == SinkProto.TransactionStatus.BEGIN) {
            handleTxBegin(txMeta);
        } else if (txMeta.getStatus() == SinkProto.TransactionStatus.END) {
            handleTxEnd(txMeta);
            metricsFacade.recordTransaction();
        }
    }

    public void processRowEvent(RowChangeEvent event) {
        if (event == null) {
            return;
        }

        metricsFacade.recordRowChange(event.getTable(), event.getOp());
        event.startLatencyTimer();
        if (event.getTransaction() == null || event.getTransaction().getId().isEmpty()) {
            handleNonTxEvent(event);
            return;
        }

        String txId = event.getTransaction().getId();
        String table = event.getFullTableName();

        long collectionOrder = event.getTransaction().getDataCollectionOrder();
        long totalOrder = event.getTransaction().getTotalOrder();

        LOGGER.debug("Receive event {} {}/{} {}/{} ", event.getOp().toString(), txId, totalOrder, table, collectionOrder);
        SinkContext ctx = activeTxContexts.get(txId);
        if (ctx == null) {
            // async method
            //            try {
            //                ctx = startTrans(txId).get();
            //            } catch (InterruptedException | ExecutionException e) {
            //                throw new RuntimeException(e);
            //            }

            // sync mode: we should wait for transaction message
            bufferOrphanedEvent(event);
            return;
        }
        ctx.lock.lock();
        try {
            ctx.cond.signalAll();
        } finally {
            ctx.lock.unlock();
        }

        OrderedEvent orderedEvent = new OrderedEvent(event, collectionOrder, totalOrder);
//        if (ctx.isReadyForDispatch(table, collectionOrder)) {
        if(true) {
            LOGGER.debug("Immediately dispatch {} {}/{}", event.getTransaction().getId(), collectionOrder, totalOrder);
            ctx.pendingEvents.incrementAndGet();
            dispatchImmediately(event, ctx);
            // ctx.updateCursor(table, collectionOrder);
            ctx.updateCounter(table);
            checkPendingEvents(ctx, table);
        } else {
            bufferOrderedEvent(ctx, orderedEvent);
        }
    }

    private void handleTxBegin(SinkProto.TransactionMetadata txBegin) {
        // startTrans(txBegin.getId()).get();
        startTransSync(txBegin.getId());
    }

    private void startTransSync(String sourceTxId) {
        SinkContext ctx = activeTxContexts.computeIfAbsent(sourceTxId, k -> new SinkContext(sourceTxId));
        TransContext pixelsTransContext;
        Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
        if (pixelsSinkConfig.isRpcEnable()) {
            pixelsTransContext = transactionManager.getTransContext();
        } else {
            LatencySimulator.smartDelay();
            pixelsTransContext = new TransContext(sourceTxId.hashCode(), System.currentTimeMillis(), false);
        }
        transLatencyTimer.close();
        ctx.pixelsTransCtx = pixelsTransContext;
        List<RowChangeEvent> buffered = getBufferedEvents(sourceTxId);
        if (buffered != null) {
            buffered.forEach(be -> processBufferedEvent(ctx, be));
        }
        LOGGER.info("Begin Tx Sync: {}", sourceTxId);
    }

    @Deprecated
    private Future<SinkContext> startTrans(String sourceTxId) {
        SinkContext ctx = activeTxContexts.computeIfAbsent(sourceTxId, k -> new SinkContext(sourceTxId));
        return transactionExecutor.submit(() -> {
            try {
                ctx.lock.lock();
                TransContext pixelsTransContext;
                Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                if (pixelsSinkConfig.isRpcEnable()) {
                    pixelsTransContext = transService.beginTrans(false);
                } else {
                    LatencySimulator.smartDelay();
                    pixelsTransContext = new TransContext(sourceTxId.hashCode(), System.currentTimeMillis(), false);
                }
                transLatencyTimer.close();
                activeTxContexts.get(sourceTxId).pixelsTransCtx = pixelsTransContext;
                ctx.lock.unlock();
                List<RowChangeEvent> buffered = getBufferedEvents(sourceTxId);
                if (buffered != null) {
                    buffered
                            // .stream()
                            // .sorted(Comparator.comparingLong(BufferedEvent::getTotalOrder))
                            .forEach(be -> processBufferedEvent(ctx, be));
                }
            } catch (TransException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Begin Tx: {}", sourceTxId);
            return ctx;
        });
    }

    private void handleTxEnd(SinkProto.TransactionMetadata txEnd) {
        String txId = txEnd.getId();
        SinkContext ctx = activeTxContexts.get(txId);
        transactionExecutor.submit(() -> {
                    LOGGER.info("Begin to Commit transaction: {}, total event {}; Data Collection {}", txId, txEnd.getEventCount(),
                            txEnd.getDataCollectionsList().stream()
                                    .map(dc -> dc.getDataCollection() + "=" +
                                            ctx.tableCursors.getOrDefault(dc.getDataCollection(), 0L) +
                                            "/" + dc.getEventCount())
                                    .collect(Collectors.joining(", ")));
                    if (ctx != null) {
                        try {
                            ctx.lock.lock();
                            ctx.markCompleted();
                            try {
                                while (!ctx.isCompleted(txEnd)) {
                                    ctx.lock.lock();
                                    LOGGER.debug("TX End Get Lock {}", txId);
                                    LOGGER.debug("Waiting for events in TX {}: {}", txId,
                                            txEnd.getDataCollectionsList().stream()
                                                    .map(dc -> dc.getDataCollection() + "=" +
                                                            ctx.tableCursors.getOrDefault(dc.getDataCollection(), 0L) +
                                                            "/" + dc.getEventCount())
                                                    .collect(Collectors.joining(", ")));

                                    ctx.cond.await(100, TimeUnit.MILLISECONDS);
                                }
                            } finally {
                                ctx.lock.unlock();
                            }

                            if (ctx.pendingEvents.get() > 0) {
                                LOGGER.info("Waiting for {} pending events in TX {}",
                                        ctx.pendingEvents.get(), txId);
                                ctx.awaitCompletion();
                            }

                            flushRemainingEvents(ctx);
                            activeTxContexts.remove(txId);
                            LOGGER.info("Committed transaction: {}", txId);
                            Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                            if (pixelsSinkConfig.isRpcEnable()) {
                                transService.commitTrans(ctx.pixelsTransCtx.getTransId(), ctx.pixelsTransCtx.getTimestamp());
                            } else {
                                LatencySimulator.smartDelay();
                            }
                            transLatencyTimer.close();
                        } catch (InterruptedException | ExecutionException | TransException e) {
                            // TODO(AntiO2) abort?
                            LOGGER.error("Failed to commit transaction {}", txId, e);
                        }
                    }
                }
        );
    }


    private void bufferOrphanedEvent(RowChangeEvent event) {
        orphanedEvents.computeIfAbsent(event.getTransaction().getId(), k -> new CopyOnWriteArrayList<>()).add(event);
        // LOGGER.debug("Buffered orphan event for TX {}: {}/{}", txId, event.collectionOrder, event.totalOrder);
    }

    private List<RowChangeEvent> getBufferedEvents(String txId) {
        return orphanedEvents.remove(txId);
    }

    private void processBufferedEvent(SinkContext ctx, RowChangeEvent bufferedEvent) {
        String table = bufferedEvent.getTable();
        dispatchImmediately(bufferedEvent, ctx);

//        long collectionOrder = bufferedEvent.collectionOrder;
//        if (ctx.isReadyForDispatch(table, collectionOrder)) {
//            dispatchImmediately(bufferedEvent.event, ctx);
//            ctx.lock.lock();
//            ctx.updateCursor(table, collectionOrder);
//            ctx.lock.unlock();
//            checkPendingEvents(ctx, table);
//        } else {
//            bufferOrderedEvent(ctx, new OrderedEvent(
//                    bufferedEvent.event,
//                    collectionOrder,
//                    bufferedEvent.totalOrder
//            ));
//            ctx.pendingEvents.incrementAndGet(); // track pending events
//        }
    }

    private void bufferOrderedEvent(SinkContext ctx, OrderedEvent event) {
        String bufferKey = ctx.sourceTxId + "|" + event.getTable();
        LOGGER.info("Buffered out-of-order event: {} {}/{}. Pending Events: {}",
                bufferKey, event.collectionOrder, event.totalOrder, ctx.pendingEvents.incrementAndGet());
        orderedBuffers.computeIfAbsent(bufferKey, k ->
                new PriorityBlockingQueue<>(INITIAL_CAPACITY, Comparator.comparingLong(OrderedEvent::getCollectionOrder))
        ).offer(event);
    }

    private void checkPendingEvents(SinkContext ctx, String table) {
        String bufferKey = ctx.sourceTxId + "|" + table;
        PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.get(bufferKey);
        if (buffer == null) return;

        while (!buffer.isEmpty()) {
            OrderedEvent nextEvent = buffer.peek();
            if (ctx.isReadyForDispatch(table, nextEvent.collectionOrder)) {
                LOGGER.debug("Ordered buffer dispatch {} {}/{}", bufferKey, nextEvent.collectionOrder, nextEvent.totalOrder);
                dispatchImmediately(nextEvent.event, ctx);
                buffer.poll();
            } else {
                break;
            }
        }
    }

    private void startDispatchWorker() {
//        dispatchExecutor.execute(() -> {
//            while (!Thread.currentThread().isInterrupted()) {
//                try {
//                    RowChangeEvent event = nonTxQueue.poll(10, TimeUnit.MILLISECONDS);
//                    if (event != null) {
//                        dispatchImmediately(event, null);
//                        metricsFacade.recordTransaction();
//                        continue;
//                    }
//
//                    activeTxContexts.values().forEach(ctx ->
//                            ctx.getTrackedTables().forEach(table ->
//                                    checkPendingEvents(ctx, table)
//                            )
//                    );
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        });
    }

    protected void dispatchImmediately(RowChangeEvent event, SinkContext ctx) {
        dispatchExecutor.execute(() -> {
            try {
                LOGGER.debug("Dispatching [{}] {}.{} (Order: {}/{}) TX: {}",
                        event.getOp().name(),
                        event.getDb(),
                        event.getTable(),
                        event.getTransaction() != null ?
                                event.getTransaction().getDataCollectionOrder() : "N/A",
                        event.getTransaction() != null ?
                                event.getTransaction().getTotalOrder() : "N/A",
                        event.getTransaction().getId());
                Summary.Timer writeLatencyTimer = metricsFacade.startWriteLatencyTimer();
                boolean success = writer.write(event);
                writeLatencyTimer.close();
                if (success) {
                    metricsFacade.recordTotalLatency(event);
                    metricsFacade.recordRowChange(event.getTable(), event.getOp());
                    event.endLatencyTimer();
                } else {
                    // TODO retry?
                }

            } finally {
                if (ctx != null) {
                    ctx.updateCounter(event.getFullTableName());
                    if (ctx.pendingEvents.decrementAndGet() == 0 && ctx.completed) {
                        ctx.completionFuture.complete(null);
                    }
                }
            }
        });
    }

    private void startTimeoutChecker() {
        timeoutScheduler.scheduleAtFixedRate(() -> {
            activeTxContexts.entrySet().removeIf(entry -> {
                SinkContext ctx = entry.getValue();
                if (ctx.isExpired()) {
                    LOGGER.warn("Transaction timeout: {}", entry.getKey());
                    flushRemainingEvents(ctx);
                    return true;
                }
                return false;
            });
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void flushRemainingEvents(SinkContext ctx) {
        LOGGER.debug("Try Flush remaining events of {}", ctx.sourceTxId);
        ctx.getTrackedTables().forEach(table -> {
            String bufferKey = ctx.sourceTxId + "|" + table;
            PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.remove(bufferKey);
            if (buffer != null) {
                LOGGER.warn("Flushing {} events for {}.{}",
                        buffer.size(), ctx.sourceTxId, table);
                buffer.forEach(event -> {
                    LOGGER.debug("Processing event for {}:{}/{}",
                            ctx.sourceTxId, event.collectionOrder, event.totalOrder);
                    dispatchImmediately(event.event, ctx);
                    LOGGER.debug("End Event for {}:{}/{}",
                            ctx.sourceTxId, event.collectionOrder, event.totalOrder);
                });
            }
        });
    }

    private void handleNonTxEvent(RowChangeEvent event) {
        // nonTxQueue.offer(event);
        dispatchImmediately(event, null);
        // event.endLatencyTimer();
    }

    public void shutdown() {
        dispatchExecutor.shutdown();
        timeoutScheduler.shutdown();
    }

    public void setTxTimeoutMs(long txTimeoutMs) {
        TX_TIMEOUT_MS = txTimeoutMs;
    }

    private static class OrderedEvent {
        final RowChangeEvent event;
        final String table;
        final long collectionOrder;
        final long totalOrder;

        OrderedEvent(RowChangeEvent event, long collectionOrder, long totalOrder) {
            this.event = event;
            this.table = event.getFullTableName();
            this.collectionOrder = collectionOrder;
            this.totalOrder = totalOrder;
        }

        String getTable() {
            return table;
        }

        long getCollectionOrder() {
            return collectionOrder;
        }
    }

    @Deprecated
    private static class BufferedEvent { // useless
        final RowChangeEvent event;
        final long collectionOrder;
        final long totalOrder;

        BufferedEvent(RowChangeEvent event, long collectionOrder, long totalOrder) {
            this.event = event;
            this.collectionOrder = collectionOrder;
            this.totalOrder = totalOrder;
        }

        long getTotalOrder() {
            return totalOrder;
        }
    }

}
