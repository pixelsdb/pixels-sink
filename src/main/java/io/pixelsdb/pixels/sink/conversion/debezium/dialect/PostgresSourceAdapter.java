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
package io.pixelsdb.pixels.sink.conversion.debezium.dialect;

import io.pixelsdb.pixels.sink.SinkProto;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PostgresSourceAdapter implements DebeziumSourceAdapter
{
    public static final PostgresSourceAdapter INSTANCE = new PostgresSourceAdapter();
    private static final Pattern TRANSACTION_WITH_LSN = Pattern.compile("^(\\d+):\\d+$");

    public PostgresSourceAdapter()
    {
    }

    @Override
    public String connector()
    {
        return "postgresql";
    }

    @Override
    public String normalizeTransactionId(String sourceId)
    {
        Matcher matcher = TRANSACTION_WITH_LSN.matcher(sourceId);
        return matcher.matches() ? matcher.group(1) : sourceId;
    }

    @Override
    public SinkProto.SourceInfo normalizeSource(DebeziumSourceMetadata source)
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(source.db())
                .setSchema(source.schema())
                .setTable(source.table())
                .build();
    }
}
