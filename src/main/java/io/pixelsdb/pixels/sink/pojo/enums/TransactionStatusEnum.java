package io.pixelsdb.pixels.sink.pojo.enums;

public enum TransactionStatusEnum {
    BEGIN,
    END;

    public static TransactionStatusEnum fromValue(String value) {
        for (TransactionStatusEnum status : values()) {
            if (status.name().equalsIgnoreCase(value)) {
                return status;
            }
        }
        return null;
    }
}
