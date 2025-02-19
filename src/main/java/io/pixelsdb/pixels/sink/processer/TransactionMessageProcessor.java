package io.pixelsdb.pixels.sink.processer;

import java.util.Map;

public class TransactionMessageProcessor {
//    private final Map<String, TransactionTask> activeTransactions = new ConcurrentHashMap<>();
//
//    public void process(TransactionMessageDTO transaction) {
//        String txId = transaction.getId();
//        TransactionStatusEnum status = transaction.getStatus();
//
//        if (status == TransactionStatusEnum.BEGIN) {
//            // 事务开始，初始化任务
//            activeTransactions.put(txId, new TransactionTask(txId));
//        } else if (status == TransactionStatusEnum.END) {
//            // 事务结束，触发任务处理
//            TransactionTask task = activeTransactions.remove(txId);
//            if (task != null) {
//                task.commit();
//            }
//        }
//    }
}