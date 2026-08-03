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
package io.pixelsdb.pixels.sink.conversion.debezium.connect;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Converts Kafka Connect {@link SourceRecord} transaction envelopes.
 */
public final class DebeziumConnectTransactionConverter
        implements DebeziumTransactionConverter<SourceRecord>
{
    private final DebeziumSourceAdapter configuredAdapter;

    public DebeziumConnectTransactionConverter(DebeziumSourceAdapter configuredAdapter)
    {
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public SinkProto.TransactionMetadata convert(SourceRecord sourceRecord)
    {
        if (!(sourceRecord.value() instanceof Struct value))
        {
            throw new IllegalArgumentException("Debezium transaction value must be a Struct");
        }
        return configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(value)
                : DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(value, configuredAdapter);
    }
}
