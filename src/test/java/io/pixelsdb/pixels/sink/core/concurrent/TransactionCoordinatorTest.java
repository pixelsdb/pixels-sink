package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionCoordinatorTest {

    private static final String TX_ID = "1001:12345";
    private static final String TABLE_NATION = "public.nation";
    private static final String TABLE_REGION = "public.region";
    private TransactionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new TransactionCoordinator();
    }

    @Test
    void testNormalTransactionFlow() {
        TransactionMetadataValue.TransactionMetadata begin = createBeginMetadata(TX_ID);
        coordinator.processBegin(begin);
        assertTrue(coordinator.getActiveTransactions().containsKey(TX_ID));

        RowChangeEvent event = createRowEvent(TX_ID, TABLE_NATION, 1);
        coordinator.processRowEvent(event);

        TransactionMetadataValue.TransactionMetadata end = createEndMetadata(TX_ID, Collections.singletonMap(TABLE_NATION, 1));
        coordinator.processEnd(end);

        assertFalse(coordinator.getActiveTransactions().containsKey(TX_ID));
        assertFalse(coordinator.getOrphanEvents().containsKey(TX_ID));
    }

    @Test
    void testOrphanEventRecovery() {
        RowChangeEvent orphanEvent = createRowEvent(TX_ID, TABLE_NATION, 1);
        coordinator.processRowEvent(orphanEvent);
        assertTrue(coordinator.getOrphanEvents().containsKey(TX_ID));

        TransactionMetadataValue.TransactionMetadata begin = createBeginMetadata(TX_ID);
        coordinator.processBegin(begin);

        TransactionMetadataValue.TransactionMetadata end = createEndMetadata(TX_ID, Collections.singletonMap(TABLE_NATION, 1));
        coordinator.processEnd(end);

        assertFalse(coordinator.getActiveTransactions().containsKey(TX_ID));
        assertFalse(coordinator.getOrphanEvents().containsKey(TX_ID));
    }

    @Test
    void testSnapshotDataHandling() {
        RowRecordMessage.RowRecord rowRecord = RowRecordMessage.RowRecord.newBuilder()
                .setSource(
                        RowRecordMessage.SourceInfo.newBuilder()
                                .setTable("public.nation") // 设置必要字段
                                .setDb("test_db")
                                .build()
                )
                .setOp("INSERT")
                .setTsMs(System.currentTimeMillis())
                .build();

        RowChangeEvent rowChangeEvent = new RowChangeEvent(rowRecord);
        coordinator.processRowEvent(rowChangeEvent);

        String virtualTxId = "SNAPSHOT-public.nation";
        assertFalse(coordinator.getActiveTransactions().containsKey(virtualTxId));
    }

    // ==== 测试场景 4：事务超时 ====
    @Test
    void testTransactionTimeout() throws InterruptedException {
        TransactionMetadataValue.TransactionMetadata begin = createBeginMetadata(TX_ID);
        coordinator.processBegin(begin);

        coordinator.setTransactionTimeoutMs(100L);
        TimeUnit.MILLISECONDS.sleep(150);
        coordinator.checkTimeouts();

        assertFalse(coordinator.getActiveTransactions().containsKey(TX_ID));
    }

    // ==== 测试场景 5：完整性检查失败 ====
    @Test
    void testIntegrityCheckFailure() {
        // 发送 BEGIN
        TransactionMetadataValue.TransactionMetadata begin = createBeginMetadata(TX_ID);
        coordinator.processBegin(begin);

        // 发送行事件（数量不足）
        RowChangeEvent event = createRowEvent(TX_ID, TABLE_NATION, 1);
        coordinator.processRowEvent(event);

        // 发送 END（要求 2 个事件）
        TransactionMetadataValue.TransactionMetadata end = createEndMetadata(TX_ID, Collections.singletonMap(TABLE_NATION, 2));
        coordinator.processEnd(end);

        // 验证事务未提交
        assertTrue(coordinator.getActiveTransactions().containsKey(TX_ID));
    }

    private TransactionMetadataValue.TransactionMetadata createBeginMetadata(String txId) {
        return TransactionMetadataValue.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus("BEGIN")
                .build();
    }

    private TransactionMetadataValue.TransactionMetadata createEndMetadata(String txId, Map<String, Integer> dataCollections) {
        List<TransactionMetadataValue.TransactionMetadata.DataCollection> protoDataCollections = dataCollections.entrySet().stream()
                .map(e -> TransactionMetadataValue.TransactionMetadata.DataCollection.newBuilder()
                        .setDataCollection(e.getKey())
                        .setEventCount(e.getValue())
                        .build())
                .collect(Collectors.toList());

        return TransactionMetadataValue.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus("END")
                .addAllDataCollections(protoDataCollections)
                .build();
    }

    private RowChangeEvent createRowEvent(String txId, String table, int totalOrder) {
        RowRecordMessage.TransactionInfo txInfo = RowRecordMessage.TransactionInfo.newBuilder()
                .setId(txId)
                .setTotalOrder(totalOrder)
                .build();

        return new RowChangeEvent(RowRecordMessage.RowRecord.newBuilder()
                .setSource(
                        RowRecordMessage.SourceInfo.newBuilder()
                                .setTable("public.nation")
                                .setDb("test_db")
                                .build()
                )
                .setOp("INSERT")
                .setTransaction(txInfo)
                .build());
    }
}