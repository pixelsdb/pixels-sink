package io.pixelsdb.pixels.sink.metadata;

import lombok.Getter;

import java.util.Objects;

public final class TableMetadataKey {
    @Getter
    private final String schemaName;
    @Getter
    private final String tableName;
    private final int hash;

    public TableMetadataKey(String schemaName, String tableName) {
        this.schemaName = schemaName.toLowerCase();
        this.tableName = tableName.toLowerCase();
        this.hash = Objects.hash(this.schemaName, this.tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadataKey that = (TableMetadataKey) o;
        return schemaName.equals(that.schemaName) &&
                tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
