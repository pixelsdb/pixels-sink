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
package io.pixelsdb.pixels.sink.source.engine;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRecordUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Locale;

/**
 * Classifies Kafka Connect {@link SourceRecord} Debezium envelopes.
 */
public final class ConnectEventClassifier implements DebeziumEventClassifier<SourceRecord>
{
    @Override
    public DebeziumRecordType classify(
            SourceRecord sourceRecord, String transactionTopic)
    {
        if (sourceRecord == null || sourceRecord.value() == null)
        {
            return DebeziumRecordType.TOMBSTONE;
        }
        if (!(sourceRecord.value() instanceof Struct value))
        {
            return DebeziumRecordType.UNKNOWN_CONTROL;
        }

        if (transactionTopic != null && transactionTopic.equals(sourceRecord.topic()))
        {
            String status = DebeziumRecordUtil.getStringSafely(value, "status");
            String id = DebeziumRecordUtil.getStringSafely(value, "id");
            return (status.equals("BEGIN") || status.equals("END")) && !id.isBlank()
                    ? DebeziumRecordType.TRANSACTION
                    : DebeziumRecordType.UNKNOWN_CONTROL;
        }

        return isRowChange(value)
                ? DebeziumRecordType.ROW
                : DebeziumRecordType.UNKNOWN_CONTROL;
    }

    @Override
    public SchemaTableName tableOf(SourceRecord record)
    {
        if (!(record.value() instanceof Struct value))
        {
            throw new IllegalArgumentException("Debezium row value must be a Struct");
        }
        Object sourceObject = DebeziumRecordUtil.getFieldSafely(value, "source");
        if (!(sourceObject instanceof Struct source))
        {
            throw new IllegalArgumentException("Debezium row source must be a Struct");
        }
        String database = DebeziumRecordUtil.getStringSafely(source, "db");
        String table = DebeziumRecordUtil.getStringSafely(source, "table");
        if (database.isBlank() || table.isBlank())
        {
            throw new IllegalArgumentException(
                    "Debezium row source is missing db or table");
        }
        return new SchemaTableName(database, table);
    }

    private static boolean isRowChange(Struct value)
    {
        String op = DebeziumRecordUtil.getStringSafely(value, "op").toLowerCase(Locale.ROOT);
        if (!(op.equals("c") || op.equals("u") || op.equals("d") || op.equals("r")))
        {
            return false;
        }

        Object sourceObject = DebeziumRecordUtil.getFieldSafely(value, "source");
        if (!(sourceObject instanceof Struct source) ||
                DebeziumRecordUtil.getStringSafely(source, "db").isBlank() ||
                DebeziumRecordUtil.getStringSafely(source, "table").isBlank())
        {
            return false;
        }

        Object before = DebeziumRecordUtil.getFieldSafely(value, "before");
        Object after = DebeziumRecordUtil.getFieldSafely(value, "after");
        return switch (op)
        {
            case "c", "r" -> after instanceof Struct;
            case "u" -> before instanceof Struct && after instanceof Struct;
            case "d" -> before instanceof Struct;
            default -> false;
        };
    }
}
