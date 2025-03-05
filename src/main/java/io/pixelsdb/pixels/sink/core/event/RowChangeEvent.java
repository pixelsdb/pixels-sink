package io.pixelsdb.pixels.sink.core.event;

import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;

import java.util.Map;
public class RowChangeEvent {
    private final OperationType op;
    private final RowRecordMessage.RowRecord rowRecord;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private String topic;

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord) {
        this.rowRecord = rowRecord;
        this.op = OperationType.fromString(rowRecord.getOp());
    }

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord, OperationType op, Map<String, Object> before, Map<String, Object> after) {
        this.rowRecord = rowRecord;
        this.op = op;
        this.before = before;
        this.after = after;
    }
    public String getSourceTable() {
        return rowRecord.getSource().getTable();
    }


    public OperationType getOp() {
        return op;
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

    public String getTopic() {
        return topic;
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

}
