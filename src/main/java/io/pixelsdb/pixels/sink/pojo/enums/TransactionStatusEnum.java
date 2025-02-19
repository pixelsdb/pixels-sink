package io.pixelsdb.pixels.sink.pojo.enums;

public enum TransactionStatusEnum {
    BEGIN,
    END,
    ABORT;


    public static TransactionStatusEnum fromValue(String value) {
        for (TransactionStatusEnum status : values()) {
            if (status.name().equalsIgnoreCase(value)) {
                return status;
            }
        }
        return null;
    }
}
