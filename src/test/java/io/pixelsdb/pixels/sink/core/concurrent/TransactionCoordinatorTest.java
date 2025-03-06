package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.TestUtils;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
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

    @BeforeEach
    void setUp() throws IOException {
        PixelsSinkConfigFactory.initialize("");

        testExecutor = TestUtils.synchronousExecutor();
        dispatchedEvents = new ArrayList<>();
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
        coordinator.processRowEvent(buildEvent("tx2", "users", 1, 1));

        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
        assertTrue(dispatchedEvents.get(1).contains("Order: 3/3"));
    }

    @Test
    void shouldRecoverOrphanedEvents() {
        coordinator.processRowEvent(buildEvent("tx3", "logs", 1, 1)); // 成为孤儿
        coordinator.processTransactionEvent(buildBeginTx("tx3"));     // 恢复孤儿事件

        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
    }

    @ParameterizedTest
    @EnumSource(OperationType.class)
    void shouldProcessNonTransactionalEvents(OperationType opType) {
        RowChangeEvent event = new RowChangeEvent(
                RowRecordMessage.RowRecord.newBuilder().build()
        );
        coordinator.processRowEvent(event);
        assertEquals(1, dispatchedEvents.size());
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
    @ValueSource(ints = {1, 3, 5})
    void shouldHandleConcurrentEvents(int threadCount) throws Exception {
        latch = new CountDownLatch(threadCount);

        // 并发发送事件
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                coordinator.processRowEvent(buildEvent("tx5", "concurrent", 1, 1));
                latch.countDown();
            }).start();
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(threadCount, dispatchedEvents.size());
    }

    private static class TestableCoordinator extends TransactionCoordinator {
        private final List<String> eventLog;

        TestableCoordinator(List<String> eventLog) {
            this.eventLog = eventLog;
        }

        @Override
        protected void dispatchImmediately(RowChangeEvent event) {
            dispatchExecutor.execute(() -> {
                String log = String.format("Dispatching [%s] %s.%s (Order: %s/%s)",
                        event.getOp().name(),
                        event.getDb(),
                        event.getTable(),
                        event.getTransaction() != null ?
                                event.getTransaction().getDataCollectionOrder() : "N/A",
                        event.getTransaction() != null ?
                                event.getTransaction().getTotalOrder() : "N/A");
                eventLog.add(log);
            });
        }

    }
}