package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SecondaryIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

@Getter
public class TableMetadata {
    private final Table table;
    private final SecondaryIndex index;
    private final List<String> keyColumnNames;

    private final List<Column> columns;

    public TableMetadata(Table table, SecondaryIndex index, List<Column> columns) {
        this.table = table;
        this.index = index;
        this.columns = columns;
        keyColumnNames = new LinkedList<>();
        List<Integer> keyColumnIds = index.getKeyColumns().getKeyColumnIds();
        for (Integer keyColumnId : keyColumnIds) {
            keyColumnNames.add(columns.get(keyColumnId).getName());
        }
    }

    public int getPkId() {
        return index.getKeyColumns().getKeyColumnIds().get(0);
    }
}