package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;

import java.util.concurrent.ConcurrentHashMap;


public class TransactionCoordinator {
    private final ConcurrentHashMap<String, TransactionState> txStateCache = new ConcurrentHashMap<>();

//    public void onTransactionBegin(TransactionMessageDTO txMeta) {
//        String txId = txMeta.getId();
//        txStateCache.put(txId, new TransactionState(txMeta));
//    }
public void onRowEvent(RowChangeEvent event) {
    if (event.getTransaction() == null) {
        String txId = PixelsSinkConstants.SNAPSHOT_TX_PREFIX + event.getSourceTable();
        TransactionState state = activeTransactions.computeIfAbsent(txId, id ->
                new TransactionState(id, Collections.emptyMap())
        );
        state.addRowEvent(event);
        state.markEndReceived();
        tryCommitIfReady(txId, state);
    } else {

    }
}
}
