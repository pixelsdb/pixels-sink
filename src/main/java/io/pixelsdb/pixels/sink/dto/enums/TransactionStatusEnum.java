package io.pixelsdb.pixels.sink.dto.enums;

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
