package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SecondaryIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TableMetadataRegistry {
    private static final MetadataService metadataService = MetadataService.Instance();
    private static volatile TableMetadataRegistry instance;
    private final ConcurrentMap<TableMetadataKey, TableMetadata> registry = new ConcurrentHashMap<>();

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

}
