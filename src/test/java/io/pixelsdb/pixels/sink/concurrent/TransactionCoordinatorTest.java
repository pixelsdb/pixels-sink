package io.pixelsdb.pixels.sink.concurrent;

import io.pixelsdb.pixels.sink.TestUtils;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionCoordinatorTest {
    private TransactionCoordinator coordinator;
    private List<String> dispatchedEvents;
    private ExecutorService testExecutor;
    private CountDownLatch latch;
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinatorTest.class);
    @BeforeEach
    void setUp() throws IOException {
        PixelsSinkConfigFactory.initialize("");

        testExecutor = TestUtils.synchronousExecutor();
        dispatchedEvents = Collections.synchronizedList(new ArrayList<>());
        coordinator = new TestableCoordinator(dispatchedEvents);

        try {
            Field executorField = TransactionCoordinator.class
                    .getDeclaredField("dispatchExecutor");
            executorField.setAccessible(true);
            executorField.set(coordinator, testExecutor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject executor", e);
        }
    }

    private TransactionMetadataValue.TransactionMetadata buildBeginTx(String txId) {
        return TransactionMetadataValue.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus("BEGIN")
                .build();
    }

    private TransactionMetadataValue.TransactionMetadata buildEndTx(String txId) {
        return TransactionMetadataValue.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus("END")
                .build();
    }

    private RowChangeEvent buildEvent(String txId, String table, long collectionOrder, long totalOrder) {
        return new RowChangeEvent(
                RowRecordMessage.RowRecord.newBuilder().setTransaction(
                                RowRecordMessage.TransactionInfo.newBuilder()
                                        .setId(txId)
                                        .setTotalOrder(totalOrder)
                                        .setDataCollectionOrder(collectionOrder)
                                        .build()
                        ).setSource(
                        RowRecordMessage.SourceInfo.newBuilder()
                                .setTable(table)
                                .setDb("test_db")
                                .build()
                        ).setOp("c")
                        .build()
        );
    }

    @Test
    void shouldProcessOrderedEvents() throws Exception {
        coordinator.processTransactionEvent(buildBeginTx("tx1"));

        coordinator.processRowEvent(buildEvent("tx1", "orders", 1, 1));
        coordinator.processRowEvent(buildEvent("tx1", "orders", 2, 2));
        coordinator.processTransactionEvent(buildEndTx("tx1"));

        assertEquals(2, dispatchedEvents.size());
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
        assertTrue(dispatchedEvents.get(1).contains("Order: 2/2"));
    }

    @Test
    void shouldHandleOutOfOrderEvents() {
        coordinator.processTransactionEvent(buildBeginTx("tx2"));
        coordinator.processRowEvent(buildEvent("tx2", "users", 3, 3));
        coordinator.processRowEvent(buildEvent("tx2", "users", 2, 2));
        coordinator.processRowEvent(buildEvent("tx2", "users", 1, 1));
        coordinator.processTransactionEvent(buildEndTx("tx2"));
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
        assertTrue(dispatchedEvents.get(1).contains("Order: 2/2"));
        assertTrue(dispatchedEvents.get(2).contains("Order: 3/3"));
    }

    @Test
    void shouldRecoverOrphanedEvents() {
        coordinator.processRowEvent(buildEvent("tx3", "logs", 1, 1)); // orphan event
        coordinator.processTransactionEvent(buildBeginTx("tx3"));     // recover
        coordinator.processTransactionEvent(buildEndTx("tx3"));
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
    }

    @ParameterizedTest
    @EnumSource(value = SinkProto.OperationType.class, names = {"INSERT", "UPDATE", "DELETE", "SNAPSHOT"})
    void shouldProcessNonTransactionalEvents(SinkProto.OperationType opType) throws InterruptedException {
        RowChangeEvent event = new RowChangeEvent(
                RowRecordMessage.RowRecord.newBuilder().build(),
                opType,
                null,
                null
        );
        coordinator.processRowEvent(event);
        TimeUnit.MILLISECONDS.sleep(10);
        assertEquals(1, dispatchedEvents.size());
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldHandleTransactionTimeout() throws Exception {
        TransactionCoordinator fastTimeoutCoordinator = new TransactionCoordinator();
        fastTimeoutCoordinator.setTxTimeoutMs(100);

        fastTimeoutCoordinator.processTransactionEvent(buildBeginTx("tx4"));
        fastTimeoutCoordinator.processRowEvent(buildEvent("tx4", "temp", 1, 1));

        TimeUnit.MILLISECONDS.sleep(150); // wait for timeout
        assertEquals(1, fastTimeoutCoordinator.activeTxContexts.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 9, 16})
    void shouldHandleConcurrentEvents(int threadCount) throws Exception {
        PixelsSinkConfigFactory.reset();
        PixelsSinkConfigFactory.initialize("");

        latch = new CountDownLatch(threadCount);
        coordinator.processTransactionEvent(buildBeginTx("tx5"));
        // concurrently send event
        for (int i = 0; i < threadCount; i++) {
            int order = i + 1;
            new Thread(() -> {
                coordinator.processRowEvent(buildEvent("tx5", "concurrent", order, order));
                latch.countDown();
            }).start();
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        coordinator.processTransactionEvent(buildEndTx("tx5"));

        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        assertEquals(threadCount, dispatchedEvents.size());
        PixelsSinkConfigFactory.reset();
    }


    private static class TestableCoordinator extends TransactionCoordinator {
        private final List<String> eventLog;
        private static final Logger LOGGER = LoggerFactory.getLogger(TestableCoordinator.class);
        TestableCoordinator(List<String> eventLog) {
            this.eventLog = eventLog;
        }

        @Override
        protected void dispatchImmediately(RowChangeEvent event, TransactionContext ctx) {
            dispatchExecutor.execute(() -> {
                try {
                    String log = String.format("Dispatching [%s] %s.%s (Order: %s/%s)",
                            event.getOp().name(),
                            event.getDb(),
                            event.getTable(),
                            event.getTransaction() != null ?
                                    event.getTransaction().getDataCollectionOrder() : "N/A",
                            event.getTransaction() != null ?
                                    event.getTransaction().getTotalOrder() : "N/A");
                    LOGGER.info(log);
                    eventLog.add(log);
                    LOGGER.debug("Event log size : {}", eventLog.size());
                } finally {
                    if (ctx != null) {
                        if (ctx.pendingEvents.decrementAndGet() == 0 && ctx.completed) {
                            ctx.completionFuture.complete(null);
                        }
                    }
                }
            });
        }
    }
}