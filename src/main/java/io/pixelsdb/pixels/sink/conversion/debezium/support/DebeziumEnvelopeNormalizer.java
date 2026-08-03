/*
 * Copyright 2026 PixelsDB.
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
 */
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceMetadata;

public final class DebeziumEnvelopeNormalizer
{
    private DebeziumEnvelopeNormalizer()
    {
    }

    public static DebeziumSourceMetadata extractSource(Object source)
    {
        return new DebeziumSourceMetadata(
                DebeziumRecordUtil.getStringSafely(source, "connector"),
                DebeziumRecordUtil.getStringSafely(source, "db"),
                DebeziumRecordUtil.getStringSafely(source, "schema"),
                DebeziumRecordUtil.getStringSafely(source, "table"));
    }

    public static DebeziumSourceAdapter adapterForSource(Object source)
    {
        String payloadConnector = DebeziumRecordUtil.getStringSafely(source, "connector");
        return DebeziumSourceAdapterRegistry.forSource(payloadConnector);
    }

    public static DebeziumSourceAdapter adapterForTransaction(Object transaction)
    {
        Object source = DebeziumRecordUtil.getFieldSafely(transaction, "source");
        if (source == null)
        {
            throw new IllegalArgumentException(
                    "Transaction metadata requires an explicit source adapter");
        }
        return adapterForSource(source);
    }

    public static SinkProto.SourceInfo normalizeSource(Object source)
    {
        return normalizeSource(source, adapterForSource(source));
    }

    public static SinkProto.SourceInfo normalizeSource(
            Object source, DebeziumSourceAdapter adapter)
    {
        DebeziumSourceMetadata metadata = extractSource(source);
        if (!metadata.connector().isBlank() &&
                !adapter.connector().equalsIgnoreCase(metadata.connector()))
        {
            throw new IllegalArgumentException(
                    "Configured Debezium connector " + adapter.connector() +
                            " does not match source.connector " + metadata.connector());
        }

        return adapter.normalizeSource(metadata);
    }

    public static SinkProto.TransactionInfo normalizeTransaction(
            Object transaction, DebeziumSourceAdapter adapter)
    {
        String rawTransactionId = DebeziumRecordUtil.getStringSafely(transaction, "id");
        return SinkProto.TransactionInfo.newBuilder()
                .setId(adapter.normalizeTransactionId(rawTransactionId))
                .setTotalOrder(DebeziumRecordUtil.getLongSafely(transaction, "total_order"))
                .setDataCollectionOrder(
                        DebeziumRecordUtil.getLongSafely(transaction, "data_collection_order"))
                .build();
    }

    public static SinkProto.TransactionMetadata normalizeTransactionMetadata(
            Object transaction, DebeziumSourceAdapter adapter)
    {
        String rawTransactionId = DebeziumRecordUtil.getStringSafely(transaction, "id");
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder()
                .setStatus(DebeziumRecordUtil.getStatusSafely(transaction, "status"))
                .setId(adapter.normalizeTransactionId(rawTransactionId))
                .setEventCount(DebeziumRecordUtil.getLongSafely(transaction, "event_count"))
                .setTimestamp(DebeziumRecordUtil.getLongSafely(transaction, "ts_ms"));

        Object collections = DebeziumRecordUtil.getFieldSafely(transaction, "data_collections");
        if (collections instanceof Iterable<?> iterable)
        {
            for (Object item : iterable)
            {
                String sourceDataCollection =
                        DebeziumRecordUtil.getStringSafely(item, "data_collection");
                builder.addDataCollections(SinkProto.DataCollection.newBuilder()
                        .setDataCollection(
                                adapter.normalizeDataCollection(sourceDataCollection))
                        .setEventCount(
                                DebeziumRecordUtil.getLongSafely(item, "event_count")));
            }
        }
        return builder.build();
    }

    public static SinkProto.TransactionMetadata normalizeTransactionMetadata(Object transaction)
    {
        return normalizeTransactionMetadata(transaction, adapterForTransaction(transaction));
    }
}
