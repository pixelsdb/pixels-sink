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
package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RowChangeEvent
{

    @Getter
    private final SinkProto.RowRecord rowRecord;
    @Getter
    private final TypeDescription schema;
    /**
     * timestamp from pixels transaction server
     */
    @Setter
    @Getter
    private long timeStamp;
    @Getter
    private String topic;
    @Getter
    private TableMetadata tableMetadata = null;
    private Map<String, SinkProto.ColumnValue> beforeValueMap;
    private Map<String, SinkProto.ColumnValue> afterValueMap;
    /**
     * Primary-key bytes used for bucket routing / PK-change detection.
     * Independent of TransService timestamp.
     */
    @Getter
    private ByteString beforeRoutingKey;
    @Getter
    private ByteString afterRoutingKey;
    @Getter
    private IndexProto.IndexKey beforeKey;
    @Getter
    private IndexProto.IndexKey afterKey;

    private boolean routingKeyInited = false;
    private boolean indexKeyBound = false;

    @Getter
    private long tableId;

    @Getter
    private SchemaTableName schemaTableName;

    public RowChangeEvent(SinkProto.RowRecord rowRecord) throws SinkException
    {
        TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
        this.rowRecord = rowRecord;
        this.schema = tableMetadataRegistry.getTypeDescription(
                rowRecord.getSource().getDb(), rowRecord.getSource().getTable());
        this.tableMetadata = tableMetadataRegistry.getMetadata(
                rowRecord.getSource().getDb(), rowRecord.getSource().getTable());
        init();
        initRoutingKey();
    }

    public RowChangeEvent(SinkProto.RowRecord rowRecord, TypeDescription schema) throws SinkException
    {
        this(rowRecord, schema, TableMetadataRegistry.Instance().getMetadata(
                rowRecord.getSource().getDb(), rowRecord.getSource().getTable()));
    }

    public RowChangeEvent(
            SinkProto.RowRecord rowRecord,
            TypeDescription schema,
            TableMetadata tableMetadata) throws SinkException
    {
        this.rowRecord = rowRecord;
        this.schema = schema;
        this.tableMetadata = tableMetadata;
        init();
    }

    protected static int getBucketFromIndexKey(IndexProto.IndexKey indexKey)
    {
        return getBucketIdFromByteBuffer(indexKey.getKey());
    }

    protected static int getBucketIdFromByteBuffer(ByteString byteString)
    {
        return RetinaUtils.getBucketIdFromByteBuffer(byteString);
    }

    private void init() throws SinkException
    {
        this.tableId = tableMetadata == null ? 0 : tableMetadata.getTableId();
        this.schemaTableName = new SchemaTableName(getSchemaName(), getTable());

        initColumnValueMap();
    }

    private void initColumnValueMap()
    {
        if (hasBeforeData())
        {
            this.beforeValueMap = new HashMap<>();
            initColumnValueMap(rowRecord.getBefore(), beforeValueMap);
        }

        if (hasAfterData())
        {
            this.afterValueMap = new HashMap<>();
            initColumnValueMap(rowRecord.getAfter(), afterValueMap);
        }
    }

    private void initColumnValueMap(SinkProto.RowValue rowValue, Map<String, SinkProto.ColumnValue> map)
    {
        IntStream.range(0, schema.getFieldNames().size())
                .forEach(i -> map.put(schema.getFieldNames().get(i), rowValue.getValuesList().get(i)));
    }

    /**
     * Builds primary-key bytes for routing. Safe before TransService timestamp is known.
     */
    public void initRoutingKey() throws SinkException
    {
        if (routingKeyInited)
        {
            return;
        }

        if (this.tableMetadata == null)
        {
            throw new SinkException("Row change table metadata is missing");
        }

        if (!this.tableMetadata.hasPrimaryIndex())
        {
            routingKeyInited = true;
            return;
        }
        if (hasBeforeData())
        {
            this.beforeRoutingKey = generateRoutingKey(tableMetadata, beforeValueMap);
        }

        if (hasAfterData())
        {
            this.afterRoutingKey = generateRoutingKey(tableMetadata, afterValueMap);
        }

        routingKeyInited = true;
    }

    /**
     * Builds Retina {@link IndexProto.IndexKey} from routing bytes and {@link #timeStamp}.
     * Call after the TransService timestamp is assigned.
     */
    public void bindIndexKey() throws SinkException
    {
        initRoutingKey();

        if (this.tableMetadata == null || !this.tableMetadata.hasPrimaryIndex())
        {
            return;
        }

        SinglePointIndex index = tableMetadata.getIndex();
        long tableIdForKey = tableMetadata.getTable().getId();
        if (hasBeforeData())
        {
            this.beforeKey = toIndexKey(beforeRoutingKey, index.getId(), tableIdForKey);
        }

        if (hasAfterData())
        {
            this.afterKey = toIndexKey(afterRoutingKey, index.getId(), tableIdForKey);
        }

        indexKeyBound = true;
    }

    /**
     * @deprecated Use {@link #initRoutingKey()} for bucketing and {@link #bindIndexKey()} after
     * timestamp assignment. Kept for callers that only need routing.
     */
    @Deprecated
    public void initIndexKey() throws SinkException
    {
        initRoutingKey();
    }

    /**
     * @deprecated Use {@link #bindIndexKey()}.
     */
    @Deprecated
    public void updateIndexKey() throws SinkException
    {
        bindIndexKey();
    }

    public boolean isIndexKeyBound()
    {
        return indexKeyBound;
    }

    public int getBeforeBucketFromIndex()
    {
        assert routingKeyInited;
        if (hasBeforeData())
        {
            return getBucketIdFromByteBuffer(beforeRoutingKey);
        }
        throw new IllegalCallerException("Event dosen't have before data");
    }

    public boolean isPkChanged() throws SinkException
    {
        if (!routingKeyInited)
        {
            initRoutingKey();
        }

        if (getOp() != SinkProto.OperationType.UPDATE)
        {
            return false;
        }

        return !beforeRoutingKey.equals(afterRoutingKey);
    }

    public int getAfterBucketFromIndex()
    {
        assert routingKeyInited;
        if (hasAfterData())
        {
            return getBucketIdFromByteBuffer(afterRoutingKey);
        }
        throw new IllegalCallerException("Event dosen't have after data");
    }

    private ByteString generateRoutingKey(
            TableMetadata tableMetadata, Map<String, SinkProto.ColumnValue> rowValue)
    {
        List<String> keyColumnNames = tableMetadata.getKeyColumnNames();
        int len = keyColumnNames.size();
        List<ByteString> keyColumnValues = new ArrayList<>(len);
        int keySize = 0;
        for (String keyColumnName : keyColumnNames)
        {
            ByteString value = rowValue.get(keyColumnName).getValue();
            keyColumnValues.add(value);
            keySize += value.size();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        for (ByteString value : keyColumnValues)
        {
            byteBuffer.put(value.toByteArray());
        }
        return ByteString.copyFrom(byteBuffer.rewind());
    }

    private IndexProto.IndexKey toIndexKey(ByteString routingKey, long indexId, long tableIdForKey)
    {
        return IndexProto.IndexKey.newBuilder()
                .setTimestamp(timeStamp)
                .setKey(routingKey)
                .setIndexId(indexId)
                .setTableId(tableIdForKey)
                .build();
    }

    public String getSourceTable()
    {
        return rowRecord.getSource().getTable();
    }

    public SinkProto.TransactionInfo getTransaction()
    {
        return rowRecord.getTransaction();
    }

    public String getTable()
    {
        return rowRecord.getSource().getTable();
    }

    public String getFullTableName()
    {
        SinkProto.SourceInfo source = rowRecord.getSource();
        String namespace = source.getSchema().isBlank() ? source.getDb() : source.getSchema();
        return namespace + "." + source.getTable();
    }

    public String getSchemaName()
    {
        return rowRecord.getSource().getDb();
    }

    public boolean hasError()
    {
        return false;
    }

    public String getDb()
    {
        return rowRecord.getSource().getDb();
    }

    public boolean isDelete()
    {
        return getOp() == SinkProto.OperationType.DELETE;
    }

    public boolean isInsert()
    {
        return getOp() == SinkProto.OperationType.INSERT;
    }

    public boolean isSnapshot()
    {
        return getOp() == SinkProto.OperationType.SNAPSHOT;
    }

    public boolean isUpdate()
    {
        return getOp() == SinkProto.OperationType.UPDATE;
    }

    public boolean hasBeforeData()
    {
        return isUpdate() || isDelete();
    }

    public boolean hasAfterData()
    {
        return isUpdate() || isInsert() || isSnapshot();
    }

    public SinkProto.OperationType getOp()
    {
        return rowRecord.getOp();
    }

    public SinkProto.RowValue getBefore()
    {
        return rowRecord.getBefore();
    }

    public SinkProto.RowValue getAfter()
    {
        return rowRecord.getAfter();
    }

    public List<ByteString> getAfterData()
    {
        List<SinkProto.ColumnValue> colValues = rowRecord.getAfter().getValuesList();
        List<ByteString> colValueList = new ArrayList<>(colValues.size());
        for (SinkProto.ColumnValue col : colValues)
        {
            colValueList.add(col.getValue());
        }
        return colValueList;
    }

    /**
     * Parallel to {@link #getAfterData()}: {@code true} means SQL NULL at that column.
     */
    public List<Boolean> getAfterIsNull()
    {
        List<SinkProto.ColumnValue> colValues = rowRecord.getAfter().getValuesList();
        List<Boolean> isNullList = new ArrayList<>(colValues.size());
        for (SinkProto.ColumnValue col : colValues)
        {
            isNullList.add(col.getIsNull());
        }
        return isNullList;
    }

    @Override
    public String toString()
    {
        String sb = "RowChangeEvent{" +
                rowRecord.getSource().getDb() +
                "." + rowRecord.getSource().getTable() +
                rowRecord.getTransaction().getId();
        return sb;
    }
}
