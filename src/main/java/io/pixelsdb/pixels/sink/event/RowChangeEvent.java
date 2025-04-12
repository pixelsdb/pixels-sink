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
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.sink.RetinaWriter;
import io.prometheus.client.Summary;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
public class RowChangeEvent {

    @Getter
    private IndexProto.IndexKey indexKey;
    private SecondaryIndex indexInfo;
    /**
     * timestamp from pixels transaction server
     */
    @Getter
    private long timeStamp;
    @Getter
    private final OperationType op;
    private final RowRecordMessage.RowRecord rowRecord;

    @Getter
    private final TypeDescription schema;
    private Map<String, Object> before = null;
    private Map<String, Object> after = null;
    @Getter
    private String topic;

    @Getter
    private TableMetadata tableMetadata = null;

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private Summary.Timer latencyTimer;


    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord) {
        this.rowRecord = rowRecord;
        this.op = OperationType.fromString(rowRecord.getOp());
        this.schema = null;
    }

    @Deprecated
    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord, OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.op = op;
        this.before = before;
        this.after = after;
        this.schema = null;
    }

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord, TypeDescription schema, OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.op = op;
        this.before = before;
        this.after = after;
        this.schema = schema;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setIndexInfo(SecondaryIndex indexInfo) {
        this.indexInfo = indexInfo;
    }


    public void initIndexKey() {
        if (op == OperationType.INSERT || op == OperationType.SNAPSHOT) {
            // We do not need to generate an index key for insert request
            return;
        }

        this.tableMetadata = TableMetadataRegistry.Instance().getMetadata(
                this.rowRecord.getSource().getDb(),
                this.rowRecord.getSource().getTable());
        List<String> keyColumnNames = tableMetadata.getKeyColumnNames();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);


        for (String name : keyColumnNames) {
            byteBuffer.put(before.get(name).toString().getBytes()); // TODO (LiZinuo): 不确定这里如何生成index key
        }
        this.indexKey = IndexProto.IndexKey.newBuilder()
                .setTimestamp(timeStamp)
                .setKey(ByteString.copyFrom(byteBuffer))
                .setIndexId(indexInfo.getId())
                .build();
    }

    public RetinaProto.Value getBeforePk() {
        return RetinaWriter.convertValue(
                before.get(tableMetadata.getKeyColumnNames().get(0)));

    }

    public RetinaProto.Value getAfterPk() {
        return RetinaWriter.convertValue(
                after.get(tableMetadata.getKeyColumnNames().get(0)));
    }

    public String getSourceTable() {
        return rowRecord.getSource().getTable();
    }


    public Map<String, Object> getBeforeData() {
        return before;
    }

    public Map<String, Object> getAfterData() {
        return after;
    }

    public RowRecordMessage.TransactionInfo getTransaction() {
        return rowRecord.getTransaction();
    }

    public String getTable() {
        return rowRecord.getSource().getTable();
    }

    // TODO(AntiO2): How to Map Schema Names Between Source DB and Pixels
    public String getSchemaName() {
        return rowRecord.getSource().getDb();
        // return rowRecord.getSource().getSchema();
    }

    public boolean hasError() {
        return false;
    }

    public RowRecordMessage.ErrorInfo getErrorInfo() {
        return rowRecord.getError();
    }

    public String getDb() {
        return rowRecord.getSource().getDb();
    }

    public boolean isDelete() {
        return op == OperationType.DELETE;
    }

    public boolean isInsert() {
        return op == OperationType.INSERT;
    }

    public boolean isUpdate() {
        return op == OperationType.UPDATE;
    }

    public Long getTimeStampUs() {
        return rowRecord.getTsUs();
    }

    public int getPkId() {
        return tableMetadata.getPkId();
    }

    public void startLatencyTimer() {
        this.latencyTimer = metricsFacade.startLatencyTimer();
    }

    public void endLatencyTimer() {
        if (latencyTimer != null) {
            this.latencyTimer.close();
        }

    }
}
