package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.pojo.TransactionMessageDTO;
import io.pixelsdb.pixels.sink.pojo.enums.TransactionStatusEnum;

import java.util.concurrent.ConcurrentHashMap;
public class TransactionCoordinator {
    private final ConcurrentHashMap<String, TransactionState> txStateCache = new ConcurrentHashMap<>();

//    public void onTransactionBegin(TransactionMessageDTO txMeta) {
//        String txId = txMeta.getId();
//        txStateCache.put(txId, new TransactionState(txMeta));
//    }

}
