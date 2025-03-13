package io.pixelsdb.pixels.sink.core.event;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import lombok.Getter;

import java.util.Map;
public class RowChangeEvent {
    @Getter
    private final SinkProto.OperationType op;
    private final RowRecordMessage.RowRecord rowRecord;

    @Getter
    private final TypeDescription schema;
    private Map<String, Object> before = null;
    private Map<String, Object> after = null;
    @Getter
    private String topic;

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord) {
        this.rowRecord = rowRecord;
        this.op = OperationType.fromString(rowRecord.getOp());
        this.schema = null;
    }

    @Deprecated
    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord, SinkProto.OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.op = op;
        this.before = before;
        this.after = after;
        this.schema = null;
    }

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord, TypeDescription schema, SinkProto.OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.op = op;
        this.before = before;
        this.after = after;
        this.schema = schema;
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
        return op == SinkProto.OperationType.DELETE;
    }

    public boolean isInsert() {
        return op == SinkProto.OperationType.INSERT;
    }

    public boolean isUpdate() {
        return op == SinkProto.OperationType.UPDATE;
    }

    public Long getTimeStampUs() {
        return rowRecord.getTsUs();
    }
}
