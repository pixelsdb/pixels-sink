package io.pixelsdb.pixels.sink.core.event;

import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;

public class RowChangeEvent {
    private final OperationType op;
    private RowRecordMessage.RowRecord rowRecord;

    public RowChangeEvent(RowRecordMessage.RowRecord rowRecord) {
        this.rowRecord = rowRecord;
        this.op = OperationType.fromString(rowRecord.getOp());
    }

    public String getSourceTable() {
        return rowRecord.getSource().getTable();
    }


    public OperationType getOp() {
        return op;
    }

    public RowRecordMessage.RowData getBefore() {
        return rowRecord.getBefore();
    }

    public RowRecordMessage.RowData getAfter() {
        return rowRecord.getAfter();
    }

    public RowRecordMessage.TransactionInfo getTransaction() {
        return rowRecord.getTransaction();
    }

    public
}
