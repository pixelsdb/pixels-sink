package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TransactionCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinator.class);
    private final ConcurrentHashMap<String, TransactionState> txStateCache = new ConcurrentHashMap<>();

    @Getter
    private final ConcurrentMap<String, TransactionState> activeTransactions = new ConcurrentHashMap<>();
    @Getter
    private final ConcurrentMap<String, List<RowChangeEvent>> orphanEvents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Getter
    @Setter
    private Long transactionTimeoutMs = 30000L;

    public TransactionCoordinator() {
        // scheduler.scheduleAtFixedRate(this::checkTimeouts, 0, 10, TimeUnit.SECONDS);
    }

    public void processBegin(TransactionMetadataValue.TransactionMetadata txBegin) {
        String txId = txBegin.getId();
        TransactionState state = activeTransactions.compute(txId, (k, v) -> {
            if (v == null) {
                v = new TransactionState(txId);
                LOGGER.info("Created new transaction: {}", txId);
            }
            return v;
        });

        List<RowChangeEvent> orphans = orphanEvents.remove(txId);
        if (orphans != null) {
            orphans.forEach(state::addRowEvent);
            LOGGER.info("Loaded {} orphan events for TX {}", orphans.size(), txId);
            tryCommitIfReady(txId, state);
        }
    }

    public void processRowEvent(RowChangeEvent event) {
        if (event.getTransaction() == null) {
            handleSnapshotEvent(event);
            return;
        }

        String txId = event.getTransaction().getId();
        TransactionState state = activeTransactions.computeIfAbsent(txId, id -> {
            LOGGER.warn("Received event for unknown TX {}, caching as orphan", id);
            return null;
        });


        if (state != null) {
            state.addRowEvent(event);
            tryCommitIfReady(txId, state);
        } else {
            LOGGER.info("Register orphan event {}", txId);
            orphanEvents.computeIfAbsent(txId, k -> new CopyOnWriteArrayList<>()).add(event);
        }
    }

    public void processEnd(TransactionMetadataValue.TransactionMetadata txEnd) {
        String txId = txEnd.getId();
        TransactionState state = activeTransactions.get(txId);
        if (state != null) {
            state.markEndReceived();
            state.setExpectedCounts(parseDataCollections(txEnd.getDataCollectionsList()));
            tryCommitIfReady(txId, state);
        }
    }

    private void tryCommitIfReady(String txId, TransactionState state) {
        if (state.isComplete()) {
            commitTransaction(txId, state);
            activeTransactions.remove(txId);
        }
    }

    private Map<String, Long> parseDataCollections(List<TransactionMetadataValue.TransactionMetadata.DataCollection> dataCollections) {
        return dataCollections.stream()
                .collect(Collectors.toMap(
                        TransactionMetadataValue.TransactionMetadata.DataCollection::getDataCollection,
                        TransactionMetadataValue.TransactionMetadata.DataCollection::getEventCount
                ));
    }

    private void commitTransaction(String txId, TransactionState state) {
        // TODO RPC接口
        LOGGER.info("Committing TX {} with events: {}", txId, state.getRowEvents().size());
    }

    private void handleSnapshotEvent(RowChangeEvent event) {
        String virtualTxId = "SNAPSHOT-" + event.getTable();
        TransactionState state = activeTransactions.computeIfAbsent(virtualTxId,
                TransactionState::new
        );
        state.addRowEvent(event);
        state.markEndReceived();
        tryCommitIfReady(virtualTxId, state);
    }


    void checkTimeouts() {
        activeTransactions.forEach((txId, state) -> {
            if (state.isExpired(transactionTimeoutMs)) { // TODO
                LOGGER.warn("TX {} timed out, rolling back", txId);
                activeTransactions.remove(txId);
                orphanEvents.remove(txId);
            }
        });
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
