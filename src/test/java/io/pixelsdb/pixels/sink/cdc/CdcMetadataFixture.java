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
package io.pixelsdb.pixels.sink.cdc;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class CdcMetadataFixture implements AutoCloseable
{
    static final long TABLE_ID = 4101L;
    static final long SCHEMA_ID = 4201L;
    static final long PRIMARY_INDEX_ID = 4301L;

    record ColumnSpec(String name, String pixelsType)
    {
    }

    private final SchemaTableName schemaTableName;
    private final Map<SchemaTableName, TableMetadata> registry;
    private final TableMetadata previous;
    private final TableMetadata metadata;
    private boolean closed;

    private CdcMetadataFixture(
            SchemaTableName schemaTableName,
            Map<SchemaTableName, TableMetadata> registry,
            TableMetadata previous,
            TableMetadata metadata)
    {
        this.schemaTableName = schemaTableName;
        this.registry = registry;
        this.previous = previous;
        this.metadata = metadata;
    }

    static CdcMetadataFixture install(
            String schemaName,
            String tableName,
            String primaryKeyColumn,
            List<ColumnSpec> columnSpecs) throws Exception
    {
        TableMetadata metadata = createMetadata(
                tableName, primaryKeyColumn, columnSpecs);
        SchemaTableName schemaTableName =
                new SchemaTableName(schemaName, tableName);
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        TableMetadata previous = registry.put(schemaTableName, metadata);
        return new CdcMetadataFixture(
                schemaTableName, registry, previous, metadata);
    }

    TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        if (previous == null)
        {
            registry.remove(schemaTableName);
        } else
        {
            registry.put(schemaTableName, previous);
        }
    }

    private static TableMetadata createMetadata(
            String tableName,
            String primaryKeyColumn,
            List<ColumnSpec> columnSpecs) throws Exception
    {
        Table table = new Table();
        table.setId(TABLE_ID);
        table.setSchemaId(SCHEMA_ID);
        table.setName(tableName);

        List<Column> columns = new ArrayList<>(columnSpecs.size());
        int primaryKeyColumnId = -1;
        for (int i = 0; i < columnSpecs.size(); ++i)
        {
            ColumnSpec spec = columnSpecs.get(i);
            int columnId = i + 1;
            Column column = new Column();
            column.setId(columnId);
            column.setName(spec.name());
            column.setType(spec.pixelsType());
            columns.add(column);
            if (spec.name().equals(primaryKeyColumn))
            {
                primaryKeyColumnId = columnId;
            }
        }
        if (primaryKeyColumnId < 0)
        {
            throw new IllegalArgumentException(
                    "Primary key column is missing: " + primaryKeyColumn);
        }

        KeyColumns keyColumns = new KeyColumns();
        keyColumns.addKeyColumnIds(primaryKeyColumnId);
        SinglePointIndex index = new SinglePointIndex();
        index.setId(PRIMARY_INDEX_ID);
        index.setTableId(TABLE_ID);
        index.setKeyColumns(keyColumns);
        return new TableMetadata(table, index, columns);
    }

    @SuppressWarnings("unchecked")
    private static Map<SchemaTableName, TableMetadata> metadataRegistry()
            throws ReflectiveOperationException
    {
        Field field = TableMetadataRegistry.class.getDeclaredField("registry");
        field.setAccessible(true);
        return (Map<SchemaTableName, TableMetadata>)
                field.get(TableMetadataRegistry.Instance());
    }
}
