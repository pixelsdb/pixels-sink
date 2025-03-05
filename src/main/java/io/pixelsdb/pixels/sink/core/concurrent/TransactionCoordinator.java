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
import java.util.concurrent.atomic.AtomicLong;

public class TransactionCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinator.class);
    public static final int INITIAL_CAPACITY = 11;
    private static final long DEFAULT_TX_TIMEOUT_MS = PixelsSinkConfigFactory.getInstance().getTransactionTimeout();
    private final ConcurrentMap<String, TransactionContext> activeTxContexts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<BufferedEvent>> orphanedEvents = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PriorityBlockingQueue<OrderedEvent>> orderedBuffers = new ConcurrentHashMap<>();
    private final BlockingQueue<RowChangeEvent> nonTxQueue = new LinkedBlockingQueue<>();
    private final ExecutorService dispatchExecutor = Executors.newFixedThreadPool(PixelsSinkDefaultConfig.SINK_THREAD);
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
        if (event.getTransaction() == null) {
            handleNonTxEvent(event);
            return;
        }

        String txId = event.getTransaction().getId();
        String table = event.getTable();
        long collectionOrder = event.getTransaction().getDataCollectionOrder();
        long totalOrder = event.getTransaction().getTotalOrder();

        TransactionContext ctx = activeTxContexts.get(txId);
        if (ctx == null) {
            bufferOrphanedEvent(txId, new BufferedEvent(event, collectionOrder, totalOrder));
            return;
        }

        OrderedEvent orderedEvent = new OrderedEvent(event, collectionOrder, totalOrder);
        if (ctx.isReadyForDispatch(table, collectionOrder)) {
            dispatchImmediately(event);
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
        buffered.stream()
                .sorted(Comparator.comparingLong(BufferedEvent::getTotalOrder))
                .forEach(be -> processBufferedEvent(ctx, be));
    }

    private void handleTxEnd(TransactionMetadataValue.TransactionMetadata txEnd) {
        String txId = txEnd.getId();
        TransactionContext ctx = activeTxContexts.get(txId);
        if (ctx != null) {
            ctx.markCompleted();
            flushRemainingEvents(ctx);
            activeTxContexts.remove(txId);
            LOGGER.info("Committed transaction: {}", txId);
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
            dispatchImmediately(bufferedEvent.event);
            ctx.updateCursor(table, collectionOrder);
            checkPendingEvents(ctx, table);
        } else {
            bufferOrderedEvent(ctx, new OrderedEvent(
                    bufferedEvent.event,
                    collectionOrder,
                    bufferedEvent.totalOrder
            ));
        }
    }

    private void bufferOrderedEvent(TransactionContext ctx, OrderedEvent event) {
        String bufferKey = ctx.txId + "|" + event.getTable();
        orderedBuffers.computeIfAbsent(bufferKey, k ->
                new PriorityBlockingQueue<>(INITIAL_CAPACITY, Comparator.comparingLong(OrderedEvent::getCollectionOrder))
        ).offer(event);
        LOGGER.debug("Buffered out-of-order event: {}", event);
    }

    private void checkPendingEvents(TransactionContext ctx, String table) {
        String bufferKey = ctx.txId + "|" + table;
        PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.get(bufferKey);
        if (buffer == null) return;

        while (!buffer.isEmpty()) {
            OrderedEvent nextEvent = buffer.peek();
            if (ctx.isReadyForDispatch(table, nextEvent.collectionOrder)) {
                dispatchImmediately(nextEvent.event);
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
                    RowChangeEvent event = nonTxQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        dispatchImmediately(event);
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

    private void dispatchImmediately(RowChangeEvent event) {
        dispatchExecutor.execute(() -> {
            LOGGER.info("Dispatching [{}] {}.{} (Order: {}/{})",
                    event.getOp().name(),
                    event.getDb(),
                    event.getTable(),
                    event.getTransaction() != null ?
                            event.getTransaction().getDataCollectionOrder() : "N/A",
                    event.getTransaction() != null ?
                            event.getTransaction().getTotalOrder() : "N/A");
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
        ctx.getTrackedTables().forEach(table -> {
            String bufferKey = ctx.txId + "|" + table;
            PriorityBlockingQueue<OrderedEvent> buffer = orderedBuffers.remove(bufferKey);
            if (buffer != null) {
                LOGGER.warn("Flushing {} events for {}.{}",
                        buffer.size(), ctx.txId, table);
                buffer.forEach(event -> dispatchImmediately(event.event));
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

    private static class TransactionContext {
        final String txId;
        final Map<String, AtomicLong> tableCursors = new ConcurrentHashMap<>();
        final long createTime = System.currentTimeMillis();
        volatile boolean completed = false;

        TransactionContext(String txId) {
            this.txId = txId;
        }

        boolean isReadyForDispatch(String table, long collectionOrder) {
            return tableCursors
                    .computeIfAbsent(table, k -> new AtomicLong(0))
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
            return System.currentTimeMillis() - createTime > DEFAULT_TX_TIMEOUT_MS;
        }
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
}
