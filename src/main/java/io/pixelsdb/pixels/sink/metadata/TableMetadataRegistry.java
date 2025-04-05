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

package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SecondaryIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TableMetadataRegistry {
    private static final MetadataService metadataService = MetadataService.Instance();
    private static volatile TableMetadataRegistry instance;
    private final ConcurrentMap<TableMetadataKey, TableMetadata> registry = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableMetadataKey, TypeDescription> typeDescriptionConcurrentMap = new ConcurrentHashMap<>();
    private final SchemaCache schemaCache = SchemaCache.getInstance();

    private TableMetadataRegistry() {
    }

    public static TableMetadataRegistry Instance() {
        if (instance == null) {
            synchronized (TableMetadataRegistry.class) {
                if (instance == null) {
                    instance = new TableMetadataRegistry();
                }
            }
        }
        return instance;
    }

    public TableMetadata getMetadata(String schema, String table) {
        TableMetadataKey key = new TableMetadataKey(schema, table);
        return registry.computeIfAbsent(key, k -> loadTableMetadata(schema, table));
    }

    public TableMetadata loadTableMetadata(String schemaName, String tableName) {
        try {
            Table table = metadataService.getTable(schemaName, tableName);
            SecondaryIndex index = metadataService.getSecondaryIndex(table.getId());
            /*
              TODO(Lizn): we only use unique index?
             */
            if (!index.isUnique()) {
                throw new MetadataException("Non Unique Index is not supported");
            }
            List<Column> tableColumns = metadataService.getColumns(schemaName, tableName, false);
            return new TableMetadata(table, index, tableColumns);
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
    }

    public TypeDescription getTypeDescription(String schema, String table) {
        return typeDescriptionConcurrentMap.get(new TableMetadataKey(schema, table));
    }
}
