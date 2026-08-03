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
package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowChangeEventFactoryTest
{
    private static final SchemaTableName TABLE =
            new SchemaTableName("factory_test", "records");
    private Map<SchemaTableName, TableMetadata> registry;
    private TableMetadata previousMetadata;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception
    {
        Field field = TableMetadataRegistry.class.getDeclaredField("registry");
        field.setAccessible(true);
        registry = (Map<SchemaTableName, TableMetadata>)
                field.get(TableMetadataRegistry.Instance());
        previousMetadata = registry.put(TABLE, metadata());
    }

    @AfterEach
    void tearDown()
    {
        if (previousMetadata == null)
        {
            registry.remove(TABLE);
        } else
        {
            registry.put(TABLE, previousMetadata);
        }
    }

    @Test
    void shouldBuildCanonicalRowChangeEvent()
            throws Exception
    {
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("int"));
        SinkProto.SourceInfo source = SinkProto.SourceInfo.newBuilder()
                .setDb(TABLE.getSchemaName())
                .setTable(TABLE.getTableName())
                .build();
        SinkProto.RowValue after = SinkProto.RowValue.newBuilder()
                .addValues(SinkProto.ColumnValue.newBuilder()
                        .setValue(ByteString.copyFromUtf8("1")))
                .build();

        RowChangeEvent event = RowChangeEventFactory.create(
                SinkProto.OperationType.INSERT,
                source,
                SinkProto.TransactionInfo.newBuilder().setId("tx-1").build(),
                null,
                after,
                schema);

        assertEquals(SinkProto.OperationType.INSERT, event.getOp());
        assertEquals(TABLE.getSchemaName(), event.getDb());
        assertEquals(TABLE.getTableName(), event.getTable());
        assertEquals("tx-1", event.getTransaction().getId());
        assertEquals(after, event.getAfter());
    }

    private TableMetadata metadata() throws Exception
    {
        Table table = new Table();
        table.setId(1);
        table.setName(TABLE.getTableName());
        return new TableMetadata(table, null, List.of(column("id", "int")));
    }

    private Column column(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
    }
}
