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

package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.domain.SecondaryIndex;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.prometheus.client.Summary;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class RowChangeEvent {

    @Getter
    private IndexProto.IndexKey indexKey;
    private final SinkProto.RowRecord rowRecord;
    @Setter
    private SecondaryIndex indexInfo;
    /**
     * timestamp from pixels transaction server
     */
    @Setter
    @Getter
    private long timeStamp;

    @Getter
    private final TypeDescription schema;

    @Getter
    private String topic;

    @Getter
    private TableMetadata tableMetadata = null;

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private Summary.Timer latencyTimer;


    public RowChangeEvent(SinkProto.RowRecord rowRecord) {
        this.rowRecord = rowRecord;
        this.schema = null;
    }

    @Deprecated
    public RowChangeEvent(SinkProto.RowRecord rowRecord, SinkProto.OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.schema = null;
    }

    public RowChangeEvent(SinkProto.RowRecord rowRecord, TypeDescription schema, SinkProto.OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.schema = schema;
        this.timeStamp = rowRecord.getTsMs();
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setIndexInfo(SecondaryIndex indexInfo) {
        this.indexInfo = indexInfo;
    }


    public void initIndexKey() {
        if (getOp() == SinkProto.OperationType.INSERT || getOp() == SinkProto.OperationType.SNAPSHOT) {
            // We do not need to generate an index key for insert request
            return;
        }

        this.tableMetadata = TableMetadataRegistry.Instance().getMetadata(
                this.rowRecord.getSource().getDb(),
                this.rowRecord.getSource().getTable());
        List<String> keyColumnNames = tableMetadata.getKeyColumnNames();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);


        // TODO ser index key
        for (String name : keyColumnNames) {
            //  byteBuffer.put(before.get(name).toString().getBytes());
        }

        this.indexKey = IndexProto.IndexKey.newBuilder()
                .setTimestamp(timeStamp)
                .setKey(ByteString.copyFrom(byteBuffer))
                // .setIndexId(indexInfo.getId())
                .build();
    }


    // TODO change
    public RetinaProto.Value getBeforePk() {
        return rowRecord.getBefore().getValues(0).getValue();
    }

    public RetinaProto.Value getAfterPk() {
        return rowRecord.getBefore().getValues(0).getValue();
    }

    public String getSourceTable() {
        return rowRecord.getSource().getTable();
    }

    public SinkProto.TransactionInfo getTransaction() {
        return rowRecord.getTransaction();
    }

    public String getTable() {
        return rowRecord.getSource().getTable();
    }

    public String getFullTableName() {
        return getSchemaName() + "." + getTable();
    }
    // TODO(AntiO2): How to Map Schema Names Between Source DB and Pixels
    public String getSchemaName() {
        return rowRecord.getSource().getDb();
        // return rowRecord.getSource().getSchema();
    }

    public boolean hasError() {
        return false;
    }

    public SinkProto.ErrorInfo getErrorInfo() {
        return rowRecord.getError();
    }

    public String getDb() {
        return rowRecord.getSource().getDb();
    }

    public boolean isDelete() {
        return getOp() == SinkProto.OperationType.DELETE;
    }

    public boolean isInsert() {
        return getOp() == SinkProto.OperationType.INSERT;
    }

    public boolean isUpdate() {
        return getOp() == SinkProto.OperationType.UPDATE;
    }

    public Long getTimeStampUs() {
        return rowRecord.getTsUs();
    }

    public int getPkId() {
        return tableMetadata.getPkId();
    }

    public void startLatencyTimer() {
        this.latencyTimer = metricsFacade.startProcessLatencyTimer();
    }

    public void endLatencyTimer() {
        if (latencyTimer != null) {
            this.latencyTimer.close();
        }

    }

    public SinkProto.OperationType getOp() {
        return rowRecord.getOp();
    }
}
