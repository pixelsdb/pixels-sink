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
 */
package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TableMetadataRegistry
{

    private static final Logger logger = LoggerFactory.getLogger(TableMetadataRegistry.class);
    private static final MetadataService metadataService = MetadataService.Instance();
    private static volatile TableMetadataRegistry instance;

    private final ConcurrentMap<SchemaTableName, TableMetadata> registry = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, SchemaTableName> tableId2SchemaTableName = new ConcurrentHashMap<>();
    private List<Schema> schemas;

    private TableMetadataRegistry()
    {
    }

    public static TableMetadataRegistry Instance()
    {
        if (instance == null)
        {
            synchronized (TableMetadataRegistry.class)
            {
                if (instance == null)
                {
                    instance = new TableMetadataRegistry();
                }
            }
        }
        return instance;
    }

    public TableMetadata getMetadata(String schema, String table) throws SinkException
    {
        SchemaTableName key = new SchemaTableName(schema, table);
        if (!registry.containsKey(key))
        {
            logger.debug("Registry doesn't contain {}", key);
            TableMetadata metadata = loadTableMetadata(schema, table);
            registry.put(key, metadata);
        }
        return registry.get(key);
    }


    public SchemaTableName getSchemaTableName(long tableId) throws SinkException
    {
        if (!tableId2SchemaTableName.containsKey(tableId))
        {
            logger.info("SchemaTableName doesn't contain {}", tableId);
            SchemaTableName metadata = loadSchemaTableName(tableId);
            tableId2SchemaTableName.put(tableId, metadata);
        }
        return tableId2SchemaTableName.get(tableId);
    }

    public TypeDescription getTypeDescription(String schemaName, String tableName) throws SinkException
    {
        return getMetadata(schemaName, tableName).getTypeDescription();
    }

    public List<String> getKeyColumnsName(String schemaName, String tableName) throws SinkException
    {
        return getMetadata(schemaName, tableName).getKeyColumnNames();
    }

    public long getPrimaryIndexKeyId(String schemaName, String tableName) throws SinkException
    {
        return getMetadata(schemaName, tableName).getPrimaryIndexKeyId();
    }


    public long getTableId(String schemaName, String tableName) throws SinkException
    {
        return getMetadata(schemaName, tableName).getTableId();
    }


    private TableMetadata loadTableMetadata(String schemaName, String tableName) throws SinkException
    {
        try
        {
            logger.info("Metadata Cache miss: {} {}", schemaName, tableName);
            Table table = metadataService.getTable(schemaName, tableName);
            SinglePointIndex index = null;
            try
            {
                index = metadataService.getPrimaryIndex(table.getId());
            } catch (MetadataException e)
            {
                logger.warn("Could not get primary index for table {}", tableName, e);
            }

            if (!index.isUnique())
            {
                throw new MetadataException("Non Unique Index is not supported, Schema:" + schemaName + " Table: " + tableName);
            }
            List<Column> tableColumns = metadataService.getColumns(schemaName, tableName, false);
            return new TableMetadata(table, index, tableColumns);
        } catch (MetadataException e)
        {
            throw new SinkException(e);
        }
    }

    private SchemaTableName loadSchemaTableName(long tableId) throws SinkException
    {
        // metadataService
        try
        {
            if (schemas == null)
            {
                schemas = metadataService.getSchemas();
            }
            Table table = metadataService.getTableById(tableId);

            long schemaId = table.getSchemaId();

            Schema schema = schemas.stream()
                    .filter(s -> s.getId() == schemaId)
                    .findFirst()
                    .orElseThrow(() -> new MetadataException("Schema not found for id: " + schemaId));

            return new SchemaTableName(table.getName(), schema.getName());

        } catch (MetadataException e)
        {
            throw new SinkException(e);
        }
    }

}
