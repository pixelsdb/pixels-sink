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
package io.pixelsdb.pixels.sink.conversion.debezium.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;

/**
 * Converts Debezium JSON transaction envelopes ({@code byte[]} payload).
 */
public final class DebeziumJsonTransactionConverter
        implements DebeziumTransactionConverter<byte[]>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DebeziumSourceAdapter configuredAdapter;

    public DebeziumJsonTransactionConverter(DebeziumSourceAdapter configuredAdapter)
    {
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public SinkProto.TransactionMetadata convert(byte[] data) throws Exception
    {
        if (configuredAdapter == null)
        {
            throw new IllegalStateException(
                    "No Debezium source adapter configured for JSON transaction conversion");
        }
        JsonNode payload = OBJECT_MAPPER.readTree(data).path("payload");
        return DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(
                payload, configuredAdapter);
    }
}
