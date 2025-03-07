package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.config.PixelsSinkDefaultConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinator.class);
    public static final int INITIAL_CAPACITY = 11;

    final ConcurrentMap<String, TransactionContext> activeTxContexts = new ConcurrentHashMap<>();
    final ExecutorService dispatchExecutor = Executors.newFixedThreadPool(PixelsSinkDefaultConfig.SINK_THREAD);
    private final ConcurrentMap<String, List<BufferedEvent>> orphanedEvents = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PriorityBlockingQueue<OrderedEvent>> orderedBuffers = new ConcurrentHashMap<>();
    private final BlockingQueue<RowChangeEvent> nonTxQueue = new LinkedBlockingQueue<>();
    private long TX_TIMEOUT_MS = PixelsSinkConfigFactory.getInstance().getTransactionTimeout();
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor();

    public TransactionCoordinator() {
        startDispatchWorker();
        startTimeoutChecker();
    }

    public void processTransactionEvent(TransactionMetadataValue.TransactionMetadata txMeta) {
        if ("BEGIN".equals(txMeta.getStatus())) {
            handleTxBegin(txMeta);
        } else if ("END".equals(txMeta.getStatus())) {
            handleTxEnd(txMeta);
        }
    }

    public void processRowEvent(RowChangeEvent event) {
        if (event.getTransaction() == null || event.getTransaction().getId().isEmpty()) {
            handleNonTxEvent(event);
            return;
        }


        String txId = event.getTransaction().getId();
        String table = event.getTable();


        long collectionOrder = event.getTransaction().getDataCollectionOrder();
        long totalOrder = event.getTransaction().getTotalOrder();

        LOGGER.debug("Receive event  {} {}/{}", txId, collectionOrder, totalOrder);

        TransactionContext ctx = activeTxContexts.get(txId);
        if (ctx == null) {
            bufferOrphanedEvent(txId, new BufferedEvent(event, collectionOrder, totalOrder));
            return;
        }

        OrderedEvent orderedEvent = new OrderedEvent(event, collectionOrder, totalOrder);
        if (ctx.isReadyForDispatch(table, collectionOrder)) {
            LOGGER.debug("Immediately dispatch {} {}/{}", event.getTransaction().getId(), collectionOrder, totalOrder);
            ctx.pendingEvents.incrementAndGet();
            dispatchImmediately(event, ctx);
            ctx.updateCursor(table, collectionOrder);
            checkPendingEvents(ctx, table);
        } else {
            bufferOrderedEvent(ctx, orderedEvent);
        }
    }

    private void handleTxBegin(TransactionMetadataValue.TransactionMetadata txBegin) {
        String txId = txBegin.getId();
        TransactionContext ctx = new TransactionContext(txId);
        activeTxContexts.put(txId, ctx);
        List<BufferedEvent> buffered = getBufferedEvents(txId);
        if (buffered != null) {
            buffered.stream()
                    .sorted(Comparator.comparingLong(BufferedEvent::getTotalOrder))
                    .forEach(be -> processBufferedEvent(ctx, be));
        }
    }

    private void handleTxEnd(TransactionMetadataValue.TransactionMetadata txEnd) {
        String txId = txEnd.getId();
        TransactionContext ctx = activeTxContexts.get(txId);
        if (ctx != null) {
            ctx.markCompleted();

            try {
                if (ctx.pendingEvents.get() > 0) {
                    LOGGER.info("Waiting for {} pending events in TX {}",
                            ctx.pendingEvents.get(), txId);
                    ctx.awaitCompletion();
                }
                flushRemainingEvents(ctx);
                activeTxContexts.remove(txId);
                LOGGER.info("Committed transaction: {}", txId);
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Failed to commit transaction {}", txId, e);
            }
        }
    }


    private void bufferOrphanedEvent(String txId, BufferedEvent event) {
        orphanedEvents.computeIfAbsent(txId, k -> new CopyOnWriteArrayList<>()).add(event);
        LOGGER.debug("Buffered orphan event for TX {}: {}", txId, event);
    }

    private List<BufferedEvent> getBufferedEvents(String txId) {
        return orphanedEvents.remove(txId);
    }

    private void processBufferedEvent(TransactionContext ctx, BufferedEvent bufferedEvent) {
        String table = bufferedEvent.event.getTable();
        long collectionOrder = bufferedEvent.collectionOrder;

        if (ctx.isReadyForDispatch(table, collectionOrder)) {
            dispatchImmediately(bufferedEvent.event, ctx);
            ctx.updateCursor(table, collectionOrder);
            checkPendingEvents(ctx, table);
        } else {
            bufferOrderedEvent(ctx, new OrderedEvent(
                    bufferedEvent.event,
                    collectionOrder,
                    bufferedEvent.totalOrder
            ));
            ctx.pendingEvents.incrementAndGet(); // track pending events
        }
    }

    private void bufferOrderedEvent(TransactionContext ctx, OrderedEvent event) {
        String bufferKey = ctx.txId + "|" + event.getTable();
        LOGGER.info("Buffered out-of-order event: {} {}/{}. Pending Events: {}",
                bufferKey, event.collectionOrder, event.totalOrder, ctx.pendingEvents.incrementAndGet());
        orderedBuffers.computeIfAbsent(bufferKey, k ->
                new PriorityBlockingQueue<>(INITIAL_CAPACITY, Comparator.comparingLong(OrderedEvent::getCollectionOrder))
        ).offer(event);
    }

    private void checkPendingEvents(TransactionContext ctx, String table) {
        String bufferKey = ctx.txId + "|" + table;
        PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.get(bufferKey);
        if (buffer == null) return;

        while (!buffer.isEmpty()) {
            OrderedEvent nextEvent = buffer.peek();
            if (ctx.isReadyForDispatch(table, nextEvent.collectionOrder)) {
                LOGGER.debug("Ordered buffer dispatch {} {}/{}", bufferKey, nextEvent.collectionOrder, nextEvent.totalOrder);
                dispatchImmediately(nextEvent.event, ctx);
                ctx.updateCursor(table, nextEvent.collectionOrder);
                buffer.poll();
            } else {
                break;
            }
        }
    }

    private void startDispatchWorker() {
        dispatchExecutor.execute(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    RowChangeEvent event = nonTxQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        dispatchImmediately(event, null);
                        continue;
                    }

                    activeTxContexts.values().forEach(ctx ->
                            ctx.getTrackedTables().forEach(table ->
                                    checkPendingEvents(ctx, table)
                            )
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    protected void dispatchImmediately(RowChangeEvent event, TransactionContext ctx) {
        dispatchExecutor.execute(() -> {
            try {
                LOGGER.info("Dispatching [{}] {}.{} (Order: {}/{})",
                        event.getOp().name(),
                        event.getDb(),
                        event.getTable(),
                        event.getTransaction() != null ?
                                event.getTransaction().getDataCollectionOrder() : "N/A",
                        event.getTransaction() != null ?
                                event.getTransaction().getTotalOrder() : "N/A");
            } finally {
                if (ctx != null) {
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
                TransactionContext ctx = entry.getValue();
                if (ctx.isExpired()) {
                    LOGGER.warn("Transaction timeout: {}", entry.getKey());
                    flushRemainingEvents(ctx);
                    return true;
                }
                return false;
            });
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void flushRemainingEvents(TransactionContext ctx) {
        LOGGER.debug("Try Flush remaining events of {}", ctx.txId);
        ctx.getTrackedTables().forEach(table -> {
            String bufferKey = ctx.txId + "|" + table;
            PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.remove(bufferKey);
            if (buffer != null) {
                LOGGER.warn("Flushing {} events for {}.{}",
                        buffer.size(), ctx.txId, table);
                buffer.forEach(event -> {
                    LOGGER.debug("Processing event for {}:{}/{}",
                            ctx.txId, event.collectionOrder, event.totalOrder);
                    dispatchImmediately(event.event, ctx);
                    LOGGER.debug("End Event for {}:{}/{}",
                            ctx.txId, event.collectionOrder, event.totalOrder);
                });
            }
        });
    }

    private void handleNonTxEvent(RowChangeEvent event) {
        nonTxQueue.offer(event);
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
            this.table = event.getTable();
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

    private static class BufferedEvent {
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

    protected class TransactionContext {
        final String txId;
        final Map<String, AtomicLong> tableCursors = new ConcurrentHashMap<>();
        final long createTime = System.currentTimeMillis();
        volatile boolean completed = false;

        final AtomicInteger pendingEvents = new AtomicInteger(0);
        final CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        TransactionContext(String txId) {
            this.txId = txId;
        }

        boolean isReadyForDispatch(String table, long collectionOrder) {
            return tableCursors
                    .computeIfAbsent(table, k -> new AtomicLong(1))
                    .get() == collectionOrder;
        }

        void updateCursor(String table, long currentOrder) {
            tableCursors.compute(table, (k, v) ->
                    (v == null) ? new AtomicLong(currentOrder + 1) :
                            new AtomicLong(Math.max(v.get(), currentOrder + 1))
            );
        }

        Set<String> getTrackedTables() {
            return tableCursors.keySet();
        }

        void markCompleted() {
            this.completed = true;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - createTime > TX_TIMEOUT_MS;
        }

        void awaitCompletion() throws InterruptedException, ExecutionException {
            completionFuture.get();
        }
    }
}
