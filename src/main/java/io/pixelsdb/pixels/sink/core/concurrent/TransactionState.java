package io.pixelsdb.pixels.sink.core.concurrent;

import io.pixelsdb.pixels.sink.pojo.DataCollectionInfo;
import io.pixelsdb.pixels.sink.pojo.TransactionInfoBO;
import io.pixelsdb.pixels.sink.pojo.TransactionMessageDTO;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TransactionState {
//    private final TransactionMessageDTO metadata;
//    private final List<RowRecord> records = new CopyOnWriteArrayList<>();
//    private final Map<String, AtomicLong> eventCounts = new ConcurrentHashMap<>();
//    private boolean committed = false;
//
//    public TransactionState(TransactionMessageDTO metadata) {
//        this.metadata = metadata;
//        // 初始化事件计数器
//        if (metadata.getDataCollections() != null) {
//            for (DataCollection dc : metadata.getDataCollections()) {
//                eventCounts.put(dc.getDataCollection(), new AtomicLong(0));
//            }
//        }
//    }
//
//    public void incrementEventCount(String table) {
//        AtomicLong counter = eventCounts.get(table);
//        if (counter != null) {
//            counter.incrementAndGet();
//        }
//    }
//
//    public long getReceivedEventCount(String table) {
//        AtomicLong counter = eventCounts.get(table);
//        return counter != null ? counter.get() : 0;
//    }
//
//    // Getters and Setters
}
