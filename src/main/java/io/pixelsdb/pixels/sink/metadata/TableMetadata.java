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