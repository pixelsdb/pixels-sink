/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.concurrent;

import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
public class TransactionState {
    private final String txId;
    private final long beginTs;
    private final Map<String, AtomicInteger> receivedCounts = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<RowChangeEvent> rowEvents = new CopyOnWriteArrayList<>();
    private Map<String, Long> expectedCounts; // update this when receive END message
    private volatile boolean endReceived = false;

    public TransactionState(String txId) {
        this.txId = txId;
        this.beginTs = System.currentTimeMillis();
        this.expectedCounts = new HashMap<>();
    }

    public void addRowEvent(RowChangeEvent event) {
        rowEvents.add(event);
        String table = event.getTable();
        receivedCounts.compute(table, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        });
    }

    public boolean isComplete() {
        return endReceived &&
                expectedCounts.entrySet().stream()
                        .allMatch(e -> receivedCounts.getOrDefault(e.getKey(), new AtomicInteger(0)).get() >= e.getValue());
    }

    public void markEndReceived() {
        this.endReceived = true;
    }

    public boolean isExpired(long timeoutMs) {
        return System.currentTimeMillis() - beginTs > timeoutMs;
    }

    public void setExpectedCounts(List<TransactionMetadataValue.TransactionMetadata.DataCollection> dataCollectionList) {
        this.expectedCounts = dataCollectionList.stream()
                .collect(Collectors.toMap(
                        TransactionMetadataValue.TransactionMetadata.DataCollection::getDataCollection,
                        TransactionMetadataValue.TransactionMetadata.DataCollection::getEventCount
                ));
    }

    public void setExpectedCounts(Map<String, Long> dataCollectionMap) {
        this.expectedCounts = dataCollectionMap;
    }

    public List<RowChangeEvent> getRowEvents() {
        return rowEvents;
    }
}
